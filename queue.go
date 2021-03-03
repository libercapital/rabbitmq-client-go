package rabbitmq

import "github.com/streadway/amqp"

/**
@See https://github.com/streadway/amqp/blob/master/channel.go
	 QueueDeclare method.
*/
type QueueDeclareConfig struct {
	QueueName  string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

func CreateQueue(config *QueueDeclareConfig, client *rabbitMQClient) amqp.Queue {
	queue, err := client.Channel.QueueDeclare(
		config.QueueName,
		config.Durable,
		config.AutoDelete,
		config.Exclusive,
		config.NoWait,
		config.Args,
	)

	FailOnError(err, "Failed to create Queue")

	return queue
}
