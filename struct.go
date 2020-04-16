package rabbitmq

import (
  "github.com/streadway/amqp"
  "time"
)

type Confirm struct {
  NeedConfirm bool
  Multi       bool
  Timeout     time.Duration
}

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
type queue struct {
  Consumers int32  `json:"consumers"`
  Name      string `json:"name"`
  Arguments amqp.Table `json:"arguments"`
}
