package rabbitmq

import (
  "errors"
  "fmt"
  "github.com/streadway/amqp"
  "log"
  "sync"
  "time"
)

type Config struct {
  HostPort  string
  Username  string
  Password  string
  Prefetch  int
  Exchanges []map[string]string
  QueueName string
  Scheme    string
  amqp      amqp.Config
}

var RabbitMQ *Client

type Client struct {
  config       *Config
  conn         *amqp.Connection
  channel      *amqp.Channel
  deliveryTag  uint64
  deliveryMu   sync.Mutex
  confirmMap   map[string]chan bool
  confirmMapMu sync.Mutex
  confirm      chan uint64
  confirmChan  chan amqp.Confirmation
  consume      func(msg amqp.Delivery) error
  recovering   bool
}

func NewClient(cfg *Config) *Client {
  if cfg.amqp.Vhost == "" {
    cfg.amqp.Vhost = "/"
  }

  if cfg.Scheme != "amqp" && cfg.Scheme != "amqps" {
    cfg.Scheme = "amqp"
  }
  return &Client{
    config:     cfg,
    confirmMap: make(map[string]chan bool, 10),
  }
}

func (c *Client) SetConsumer(consumer func(msg amqp.Delivery) error) {
  c.consume = consumer
}
func (c *Client) Start() (err error) {
  amqpUrl := fmt.Sprintf("%s://%s:%s@%s/", c.config.Scheme, c.config.Username, c.config.Password, c.config.HostPort)
  c.conn, err = amqp.DialConfig(amqpUrl, c.config.amqp)
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

  err = c.channel.Qos(c.config.Prefetch, 0, false)
  if err != nil {
    return
  }
  _, err = c.channel.QueueDeclare(
    c.config.QueueName, // name
    true,               // durable
    true,               // delete when unused
    false,              // exclusive
    false,              // no-wait
    nil,                // arguments
  )
  if err != nil {
    return
  }

  for _, v := range c.config.Exchanges {
    err = c.channel.ExchangeDeclare(v["name"], v["kind"], true, false, false, false, nil)
    if err != nil {
      return
    }
    err = c.channel.QueueBind(
      c.config.QueueName, // queue name
      c.config.QueueName, // routing key
      v["name"],          // exchange
      false,
      nil,
    )
    if err != nil {
      return
    }
  }
  go c.consumerMessage()
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
    c.config.QueueName, // queue
    c.config.QueueName, // consumer
    false,              // auto-ack
    false,              // exclusive
    false,              // no-local
    false,              // no-wait
    nil,                // args
  )

  if err != nil {
    log.Fatal(err)
    return
  }

  log.Println("consumerMessage started", c.config.QueueName)
  for d := range messages {
    go func(d amqp.Delivery) {
      var err error
      if d.ReplyTo != "" {
        c.confirmMapMu.Lock()
        if b, ok := c.confirmMap[d.ReplyTo]; ok {
          b <- true
          delete(c.confirmMap, d.ReplyTo)
        }
        c.confirmMapMu.Unlock()
      } else {
        if c.consume != nil {
          err = c.consume(d)
        }
      }
      if err == nil {
        _ = d.Ack(false)
      }
    }(d)
  }
}

func (c *Client) Publish(exchange, routeKey string,
  msg amqp.Publishing, confirmChan chan bool) (err error) {
  if c.isClosed() {
    err = errors.New("source channel closed")
    return
  }
  if confirmChan != nil && msg.ReplyTo == "" {
    err = errors.New("confirm 'ReplyTo' empty")
    return
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

  if m := <-c.confirmChan; !m.Ack {
    err = errors.New("publish fail")
    return
  }

  if confirmChan != nil {
    c.confirmMapMu.Lock()
    c.confirmMap[msg.ReplyTo] = confirmChan
    c.confirmMapMu.Unlock()

    select {
    case <-confirmChan:
    case <-time.After(time.Second * 10):
      err = errors.New("destination not confirmed")
      c.confirmMapMu.Lock()
      delete(c.confirmMap, msg.ReplyTo)
      c.confirmMapMu.Unlock()
    }
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
