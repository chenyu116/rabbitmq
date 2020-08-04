package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

const (
	TYPE_REPLY  = "REPLY"
	TYPE_SIMPLE = "SIMPLE"
)

type Client struct {
	config               *Config
	conn                 *amqp.Connection
	channel              *amqp.Channel
	confirmMap           map[string]context.CancelFunc
	confirmMapMu         sync.Mutex
	confirm              chan uint64
	confirmChan          chan amqp.Confirmation
	messageConsumer      []func(msg amqp.Delivery)
	replyConsumer        []func(msg amqp.Delivery)
	consumersLenMap      map[string]int32
	consumersLenMu       sync.RWMutex
	recovering           bool
	synchronizing        bool
	exchangeConsumersLen int32
	scheme               string
	queueName            string
}

func NewClient(cfg *Config) *Client {
	return &Client{
		config:          cfg,
		confirmMap:      make(map[string]context.CancelFunc, 10),
		consumersLenMap: make(map[string]int32),
	}
}
func (c *Client) GetQueueName() string {
	return c.queueName
}
func (c *Client) AddReplyConsumer(consumer func(msg amqp.Delivery)) {
	c.replyConsumer = append(c.replyConsumer, consumer)
}
func (c *Client) AddMessageConsumer(consumer func(msg amqp.Delivery)) {
	c.messageConsumer = append(c.messageConsumer, consumer)
}
func (c *Client) checkConfig() (err error) {
	c.scheme = "amqp"
	if c.config.UseTls {
		if c.config.Amqp.TLSClientConfig == nil {
			return errors.New("config.Amqp.TLSClientConfig is nil")
		}
		c.scheme = "amqps"
	}
	if c.config.ReplyConfirmTimeout == 0 {
		c.config.ReplyConfirmTimeout = time.Second * 10
	}
	c.queueName = c.config.QueuePrefix + c.config.QueueName
	return
}
func (c *Client) Start() (err error) {
	err = c.checkConfig()
	if err != nil {
		return
	}
	amqpUrl := fmt.Sprintf("%s://%s:%s@%s/", c.scheme, c.config.Username, c.config.Password, c.config.HostPort)
	c.conn, err = amqp.DialConfig(amqpUrl, c.config.Amqp)
	if err != nil {
		return
	}
	c.channel, err = c.conn.Channel()
	if err != nil {
		return
	}
	c.confirmChan = c.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	err = c.channel.Confirm(false)
	if err != nil {
		return
	}
	if c.config.QueueEnable {
		err = c.channel.Qos(c.config.Prefetch, 0, false)
		if err != nil {
			return
		}
		_, err = c.channel.QueueDeclare(
			c.queueName,              // name
			true,                     // durable
			c.config.QueueAutoDelete, // delete when unused
			false,                    // exclusive
			false,                    // no-wait
			nil,                      // arguments
		)
		if err != nil {
			return
		}

		for _, v := range c.config.Exchanges {
			err = c.channel.ExchangeDeclare(v.Name, v.Kind, true, false, false, false, nil)
			if err != nil {
				return
			}
			err = c.channel.QueueBind(
				c.queueName, // queue name
				c.queueName, // routing key
				v.Name,      // exchange
				false,
				nil,
			)
			if err != nil {
				return
			}
		}
		go c.consumerMessage()
	}
	if !c.recovering {
		go c.recovery()
	}
	return
}

func (c *Client) consumerMessage() {
	defer func() {
		log.Println("consumerMessage stopped")
	}()

	messages, err := c.channel.Consume(
		c.queueName, // queue
		c.queueName, // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)

	if err != nil {
		log.Println(err)
		return
	}

	for msg := range messages {
		go func(d amqp.Delivery) {
			if d.ReplyTo != "" {
				c.confirmMapMu.Lock()
				if cancel, ok := c.confirmMap[d.ReplyTo]; ok {
					cancel()
					delete(c.confirmMap, d.ReplyTo)
					c.confirmMapMu.Unlock()
				} else {
					c.confirmMapMu.Unlock()
					return
				}

				if len(c.replyConsumer) > 0 {
					for _, rc := range c.replyConsumer {
						rc(d)
					}
				}

			} else {
				for _, mc := range c.messageConsumer {
					mc(d)
				}
			}
			_ = d.Ack(false)
		}(msg)
	}
}

func (c *Client) Publish(exchange, routeKey string,
	msg amqp.Publishing, confirm bool) (err error) {
	if c.isClosed() {
		err = errors.New("source channel closed")
		return
	}
	if !c.config.QueueEnable {
		confirm = false
	}
	if confirm {
		msg.AppId = c.queueName
	}

	err = c.channel.Publish(
		exchange,
		routeKey,
		false,
		false,
		msg)
	if err != nil {
		return
	}
	select {
	case <-time.After(c.config.ReplyConfirmTimeout):
		err = errors.New("publish fail")
		return
	case m := <-c.confirmChan:
		if !m.Ack {
			err = errors.New("publish fail")
			return
		}
	}

	if confirm {
		ctx, can := context.WithTimeout(context.Background(), c.config.ReplyConfirmTimeout)
		c.confirmMapMu.Lock()
		c.confirmMap[msg.ReplyTo] = can
		c.confirmMapMu.Unlock()
		<-ctx.Done()
		if ctx.Err() == context.Canceled {
			return nil
		}
		err = errors.New("confirmed timeout")
		c.confirmMapMu.Lock()
		delete(c.confirmMap, msg.ReplyTo)
		c.confirmMapMu.Unlock()
	}
	return
}

func (c *Client) recovery() {
	c.recovering = true
	recoveryTicker := time.NewTicker(time.Second * 3)
	reconnecting := false
	for range recoveryTicker.C {
		if !c.isClosed() || reconnecting {
			continue
		}
		log.Println("start recovery")
		reconnecting = true
		err := c.Start()
		if err != nil {
			log.Println(err)
		}
		reconnecting = false
	}
}

func (c *Client) isClosed() bool {
	if c.conn == nil || c.conn.IsClosed() {
		return true
	}
	return false
}
