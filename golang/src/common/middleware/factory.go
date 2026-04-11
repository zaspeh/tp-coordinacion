package middleware

import "fmt"

const TRIES_TO_CONNECT = 10

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := dialWithRetry(url, TRIES_TO_CONNECT)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	_, err = ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &queueMiddleware{
		conn:      conn,
		ch:        ch,
		queueName: queueName,
	}, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := dialWithRetry(url, TRIES_TO_CONNECT)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange, // name
		"direct", // type
		false,    // durability -> si se reinicia rabbit, el exchange sigue existiendo
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return nil, err
	}

	return &exchangeMiddleware{
		conn:      conn,
		ch:        ch,
		queueName: "",
		exchange:  exchange,
		keys:      keys,
	}, nil
}
