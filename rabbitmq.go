package rabbitmq

import (
  "context"
  "encoding/json"
  "errors"
  "fmt"
  "github.com/chenyu116/rattle"
  "github.com/streadway/amqp"
  "log"
  "sync"
  "time"
)

const (
  TYPE_REPLY = "REPLY"
  TYPE_SEND  = "SEND"
)

func NewConfig() *Config {
  return new(Config)
}

type Client struct {
  config               *Config
  conn                 *amqp.Connection
  channel              *amqp.Channel
  confirmMap           map[string]*replyConfirm
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
}

func NewClient(cfg *Config) *Client {
  if cfg.amqp.Vhost == "" {
    cfg.amqp.Vhost = "/"
  }

  if cfg.Scheme != "amqp" && cfg.Scheme != "amqps" {
    cfg.Scheme = "amqp"
  }
  return &Client{
    config:          cfg,
    confirmMap:      make(map[string]*replyConfirm, 10),
    consumersLenMap: make(map[string]int32),
  }
}
func (c *Client) AddReplyConsumer(consumer func(msg amqp.Delivery)) {
  c.replyConsumer = append(c.replyConsumer, consumer)
}
func (c *Client) AddMessageConsumer(consumer func(msg amqp.Delivery)) {
  c.messageConsumer = append(c.messageConsumer, consumer)
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
  args := make(amqp.Table)
  for _, v := range c.config.Exchanges {
    args[v["name"]] = c.config.QueueName
  }
  _, err = c.channel.QueueDeclare(
    c.config.QueueName, // name
    true,               // durable
    true,               // delete when unused
    false,              // exclusive
    false,              // no-wait
    args,               // arguments
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
  if !c.synchronizing {
    go c.sync()
  }
  return
}

func (c *Client) sync() {
  time.Sleep(time.Second * 3)
  fmt.Println("sync")
  //exchangeMap := make(map[string])
  for {
    res, _, err := rattle.New(nil).Get("https://rbtmqman.astat.cn/api/queues").
      SetBasicAuth(c.
        config.
        Username,
        c.config.Password).Send()
    if err != nil {
      log.Println(err)
      continue
    }
    var queues []queue
    err = json.Unmarshal(res, &queues)
    if err != nil {
      log.Println(err)
      continue
    }
    c.consumersLenMu.Lock()
    c.consumersLenMap = make(map[string]int32)
    for _, v := range queues {
      for k := range v.Arguments {
        c.consumersLenMap[k]++
      }
    }
    c.consumersLenMu.Unlock()
    fmt.Println(c.consumersLenMap)
    time.Sleep(time.Second * 5)
  }
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
  }

  log.Println("consumerMessage started", c.config.QueueName)
  for msg := range messages {
    log.Println(c.config.QueueName, "got", msg.ReplyTo, string(msg.Body))
    go func(d amqp.Delivery) {
      if d.Type == TYPE_REPLY && d.ReplyTo != "" {
        c.confirmMapMu.Lock()
        if b, ok := c.confirmMap[d.ReplyTo]; ok {
          b.AddMessage(d)
          b.Incr()
          if b.Finish() {
            b.Done()
            delete(c.confirmMap, d.ReplyTo)
          }
          fmt.Println("consumerMessage Reply", c.confirmMap[d.ReplyTo])
          if len(c.replyConsumer) > 0 {
            go func(rMsg amqp.Delivery) {
              for _, rc := range c.replyConsumer {
                rc(rMsg)
              }
            }(d)
          }
        }
        c.confirmMapMu.Unlock()

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
  msg amqp.Publishing, confirms ...Confirm) (err error) {
  if c.isClosed() {
    err = errors.New("source channel closed")
    return
  }
  confirm := Confirm{}
  if len(confirms) > 0 {
    confirm = confirms[0]
  }
  if confirm.NeedConfirm && msg.ReplyTo == "" {
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

  if confirm.NeedConfirm {
    if confirm.Multi {
      go func() {
        var confirmLen int32 = 1
        c.consumersLenMu.RLock()
        if cs, ok := c.consumersLenMap[exchange]; ok {
          if cs == 0 {
            return
          }
          confirmLen = cs
        }
        c.consumersLenMu.RUnlock()
        rc := &replyConfirm{
          len:     confirmLen,
          done:    make(chan bool),
          replies: make([]amqp.Delivery, confirmLen),
        }
        c.confirmMapMu.Lock()
        c.confirmMap[msg.ReplyTo] = rc
        c.confirmMapMu.Unlock()

        fmt.Printf("Multi %+v\n", c.confirmMap[msg.ReplyTo])
        <-rc.done
      }()
    } else {
      var confirmLen int32 = 1
      rc := &replyConfirm{
        len:     confirmLen,
        done:    make(chan bool),
        replies: make([]amqp.Delivery, confirmLen),
      }
      c.confirmMapMu.Lock()
      c.confirmMap[msg.ReplyTo] = rc
      c.confirmMapMu.Unlock()
      if confirm.Timeout == 0 {
        confirm.Timeout = time.Second * 10
      }
      ctx, cancel := context.WithTimeout(context.Background(), confirm.Timeout)
      select {
      case <-rc.done:
        fmt.Println("rc.done")
        cancel()
        return
      case <-ctx.Done():
        err = errors.New("destination not confirmed")
        c.confirmMapMu.Lock()
        delete(c.confirmMap, msg.ReplyTo)
        c.confirmMapMu.Unlock()
      }
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
