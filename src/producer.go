package src

import "github.com/streadway/amqp"

/**
@See https://github.com/streadway/amqp/blob/master/channel.go
	 Publish method.
*/

type ProducerConfig struct {
	Exchange      string
	RoutingKey    string
	Mandatory     bool
	Immediate     bool
	PublishConfig amqp.Publishing
}

func Publish(config *ProducerConfig, channel *amqp.Channel) {
	err := channel.Publish(
		config.Exchange,
		config.RoutingKey,
		config.Mandatory,
		config.Immediate,
		config.PublishConfig,
	)

	FailOnError(err, "Failed to publish a message")
}
