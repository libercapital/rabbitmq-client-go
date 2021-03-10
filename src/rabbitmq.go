package src

import (
	"github.com/streadway/amqp"
	"log"
)

/**
@see https://github.com/streadway/amqp/blob/master/connection.go
	Dial() and Channel() methods.
*/

type RabbitMQClient struct {
	Channel *amqp.Channel
}

var instance *RabbitMQClient

func Connect(url string) *RabbitMQClient {
	conn, err := amqp.Dial(url)

	FailOnError(err, "Failed to connect to RabbitMQ")

	ch, errChannel := conn.Channel()

	FailOnError(errChannel, "Failed to open a channel")

	instance = &RabbitMQClient{ch}

	return instance
}

func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
