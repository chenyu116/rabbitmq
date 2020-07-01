package rabbitmq

import (
	"github.com/streadway/amqp"
	"time"
)

type Confirm struct {
	NeedConfirm bool
	ReplyTo     []string
	Timeout     time.Duration
}

type queue struct {
	Consumers int32      `json:"consumers"`
	Name      string     `json:"name"`
	Arguments amqp.Table `json:"arguments"`
}
