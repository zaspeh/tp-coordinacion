package middleware

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type exchangeMiddleware struct {
	conn *amqp.Connection
	ch   *amqp.Channel

	consumerTag string
	cancelFunc  context.CancelFunc
	queueName   string

	exchange string
	keys     []string
}

func (e *exchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if e.ch == nil {
		return ErrMessageMiddlewareDisconnected
	}

	q, err := e.ch.QueueDeclare(
		"",    // name
		false, // durability
		true,  // delete when unused -> cuando se desconectan los consumidores, se borra la queue
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return mapError(err)
	}

	e.queueName = q.Name

	for _, key := range e.keys {
		err = e.ch.QueueBind(
			q.Name,
			key,
			e.exchange,
			false,
			nil,
		)
		if err != nil {
			return mapError(err)
		}
	}

	msgs, err := consumeWithTag(e.ch, e.queueName, &e.consumerTag)

	if err != nil {
		return mapError(err)
	}

	return consumeLoop(msgs, &e.cancelFunc, callbackFunc)
}

func (e *exchangeMiddleware) StopConsuming() error {
	return stopConsuming(e.ch, &e.consumerTag, &e.cancelFunc)
}

func (e *exchangeMiddleware) Send(msg Message) error {
	// debería haber una routing key
	if len(e.keys) == 0 {
		return ErrMessageMiddlewareMessage
	}

	if e.ch == nil {
		return ErrMessageMiddlewareDisconnected
	}

	for _, key := range e.keys { // uso todas las llaves que tengo
		err := sendWithContext(e.ch, e.exchange, key, msg)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *exchangeMiddleware) Close() error {
	_ = e.StopConsuming()
	var closeErr error = nil

	if e.ch != nil {
		if err := e.ch.Close(); err != nil { // seguro sea error interno no resoluble
			closeErr = ErrMessageMiddlewareClose
		}
		e.ch = nil
	}

	if e.conn != nil {
		if err := e.conn.Close(); err != nil { // seguro sea error interno no resoluble
			closeErr = ErrMessageMiddlewareClose
		}
		e.conn = nil
	}

	return closeErr
}
