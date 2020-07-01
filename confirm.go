package rabbitmq

type replyConfirm struct {
	done chan bool
}

func (r *replyConfirm) Done() {
	r.done <- true
}
