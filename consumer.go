package rabbitmq

import "github.com/streadway/amqp"

/**
@See https://github.com/streadway/amqp/blob/master/channel.go
	 Consume method.
*/

type ConsumerConfig struct {
	QueueName string
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

func Consume(config *ConsumerConfig, client *rabbitMQClient) <-chan amqp.Delivery {
	messages, err := client.Channel.Consume(
		config.QueueName,
		config.Consumer,
		config.AutoAck,
		config.Exclusive,
		config.NoLocal,
		config.NoWait,
		config.Args,
	)

	FailOnError(err, "Failed to register a consumer")

	return messages
}
