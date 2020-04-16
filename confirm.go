package rabbitmq

import (
  "github.com/streadway/amqp"
  "sync/atomic"
)

type replyConfirm struct {
  len       int32
  confirmed int32
  done      chan bool
  replies   []amqp.Delivery
}

func (r *replyConfirm) Done() {
  r.done <- true
}

func (r *replyConfirm) AddMessage(msg amqp.Delivery) {
  r.replies = append(r.replies, msg)
}
func (r *replyConfirm) Incr() {
  atomic.AddInt32(&r.confirmed, 1)
}

func (r *replyConfirm) Finish() bool {
  return r.len == r.confirmed
}