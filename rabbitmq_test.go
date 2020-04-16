package rabbitmq

import (
  "fmt"
  "github.com/spf13/viper"
  "github.com/streadway/amqp"
  "log"
  "testing"
  "time"
)

var c cf
var rbtConfig *Config

type rabbitmqConfig struct {
  HostPort    string              `mapstructure:"hostPort"`
  Username    string              `mapstructure:"username"`
  Password    string              `mapstructure:"password"`
  Prefetch    int                 `mapstructure:"prefetch"`
  VHost       string              `mapstructure:"vHost"`
  Exchanges   []map[string]string `mapstructure:"exchanges"`
  QueuePrefix string              `mapstructure:"queuePrefix"`
}
type cf struct {
  Rabbitmq rabbitmqConfig `mapstructure:"rabbitmq"`
}

func init() {
  viper.SetConfigType("yaml")
  viper.SetConfigName("config")
  viper.AddConfigPath(".")
  err := viper.ReadInConfig()

  if err != nil {
    log.Fatal(err)
  }
  err = viper.Unmarshal(&c)

  if err != nil {
    log.Fatal(err)
  }
  rbtConfig = NewConfig()
  rbtConfig.HostPort = c.Rabbitmq.HostPort
  rbtConfig.amqp = amqp.Config{
    Vhost:     c.Rabbitmq.VHost,
    Heartbeat: time.Second * 3,
  }
  rbtConfig.QueueName = "websocketServer-" + time.Now().String()
  rbtConfig.Prefetch = c.Rabbitmq.Prefetch
  rbtConfig.Username = c.Rabbitmq.Username
  rbtConfig.Password = c.Rabbitmq.Password
  rbtConfig.Exchanges = c.Rabbitmq.Exchanges
}

//func TestClient_Start(t *testing.T) {
//  client := NewClient(rbtConfig)
//  err := client.Start()
//  if err != nil {
//    t.Fatal(err)
//  }
//}
//
//func TestClient_Publish(t *testing.T) {
//  client := NewClient(rbtConfig)
//  err := client.Start()
//  if err != nil {
//    t.Fatal(err)
//  }
//  err = client.Publish("websocketServer-fanout", "", amqp.Publishing{
//    AppId: client.config.QueueName,
//    Body:  []byte("test"),
//  }, nil)
//  if err != nil {
//    t.Fatal(err)
//  }
//}

func TestClient_PublishWithConfirm(t *testing.T) {
  //
  rbtConfig1 := NewConfig()
  rbtConfig1.HostPort = rbtConfig.HostPort
  rbtConfig1.amqp = amqp.Config{
    Vhost:     c.Rabbitmq.VHost,
    Heartbeat: time.Second * 3,
  }
  rbtConfig1.Prefetch = rbtConfig.Prefetch
  rbtConfig1.Username = rbtConfig.Username
  rbtConfig1.Password = rbtConfig.Password
  rbtConfig1.Exchanges = rbtConfig.Exchanges
  rbtConfig1.QueueName = "confirmServer-" + time.Now().String()
  r := NewClient(rbtConfig1)
  err := r.Start()
  if err != nil {
    t.Fatal(err)
  }
  r.AddMessageConsumer(func(msg amqp.Delivery) {
    //log.Printf("confirmServer message %+v", msg)
    r.Publish("websocketServer-direct", msg.ReplyTo, amqp.Publishing{
     Type:    TYPE_REPLY,
     ReplyTo: msg.ReplyTo,
     Body:    []byte("confirmed " + msg.AppId),
    })
  })
  //time.Sleep(time.Second * 10)
  client := NewClient(rbtConfig)
  err = client.Start()
  if err != nil {
    t.Fatal(err)
  }
  r.AddMessageConsumer(func(msg amqp.Delivery) {
    //log.Printf("websocketServer message %+v", msg)
    r.Publish("websocketServer-direct", msg.ReplyTo, amqp.Publishing{
      Type:    TYPE_REPLY,
      ReplyTo: msg.ReplyTo,
      Body:    []byte("confirmed " + msg.AppId),
    })
  })
  client.AddReplyConsumer(func(msg amqp.Delivery) {
    fmt.Println("ReplyConsumer", string(msg.Body))
  })
  time.Sleep(time.Second * 3)

  //for i := 0; i < 1000; i++ {
  err = client.Publish("websocketServer-fanout", "confirmServer", amqp.Publishing{
    Type:    TYPE_SEND,
    ReplyTo: client.config.QueueName,
    Body:    []byte("test"),
  }, Confirm{NeedConfirm: true, Multi: true, Timeout: time.Second * 30})
  if err != nil {
    t.Fatal(err)
  }

  fmt.Println("all confirmed")
  time.Sleep(time.Hour)
}

//func TestClient_Recovery(t *testing.T) {
// client := NewClient(rbtConfig)
// err := client.Start()
// if err != nil {
//   t.Fatal(err)
// }
//
// time.Sleep(time.Hour)
//}
