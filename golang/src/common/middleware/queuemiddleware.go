package middleware

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type queueMiddleware struct {
	conn *amqp.Connection
	ch   *amqp.Channel

	consumerTag string
	cancelFunc  context.CancelFunc
	queueName   string
}

func (q *queueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if q.ch == nil {
		return ErrMessageMiddlewareDisconnected
	}

	msgs, err := consumeWithTag(q.ch, q.queueName, &q.consumerTag)
	if err != nil {
		return mapError(err)
	}

	return consumeLoop(msgs, &q.cancelFunc, callbackFunc)
}

func (q *queueMiddleware) StopConsuming() error {
	return stopConsuming(q.ch, &q.consumerTag, &q.cancelFunc)
}

func (q *queueMiddleware) Send(msg Message) error {
	if q.ch == nil {
		return ErrMessageMiddlewareDisconnected
	}
	return sendWithContext(q.ch, "", q.queueName, msg)
}

func (q *queueMiddleware) Close() error {
	_ = q.StopConsuming()
	var closeErr error = nil

	if q.ch != nil {
		if err := q.ch.Close(); err != nil {
			closeErr = ErrMessageMiddlewareClose
		}
		q.ch = nil
	}

	if q.conn != nil {
		if err := q.conn.Close(); err != nil {
			closeErr = ErrMessageMiddlewareClose
		}
		q.conn = nil
	}

	return closeErr
}
