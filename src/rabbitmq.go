package src

import (
	"github.com/streadway/amqp"
	"log"
)

/**
@see https://github.com/streadway/amqp/blob/master/connection.go
	Dial() and Channel() methods.
*/

type rabbitMQClient struct {
	Channel *amqp.Channel
}

var instance *rabbitMQClient

func Connect(url string) *rabbitMQClient {
	conn, err := amqp.Dial(url)

	FailOnError(err, "Failed to connect to RabbitMQ")

	ch, errChannel := conn.Channel()

	FailOnError(errChannel, "Failed to open a channel")

	instance = &rabbitMQClient{ch}

	return instance
}

func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
