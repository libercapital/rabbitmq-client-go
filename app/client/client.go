package client

import (
	"fmt"

	"github.com/streadway/amqp"
	"gitlab.com/icredit/bava/architecture/software/libs/go-modules/rabbitmq-client.git/app/models"
)

type Client interface {
	NewPublisher(args *models.QueueArgs) (Publisher, error)
	NewConsumer(queueName string) (Consumer, error)
}

type clientImpl struct {
	channel *amqp.Channel
}

func New(credential models.Credential) (Client, error) {
	conn, err := amqp.Dial(credential.GetConnectionString())
	if err != nil {
		return nil, fmt.Errorf("connection error: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("channel connection error: %v", err)
	}

	return clientImpl{channel: ch}, nil
}

func (client clientImpl) NewPublisher(args *models.QueueArgs) (Publisher, error) {
	if args == nil {
		args = &models.QueueArgs{}
	}

	queue, err := client.channel.QueueDeclare(args.Name, args.Durable, args.AutoDelete, args.Exclusive, args.NoWait, nil)
	if err != nil {
		return nil, fmt.Errorf("queue connection error: %v", err)
	}

	return publisherImpl{channel: client.channel, queue: &queue}, nil
}

func (client clientImpl) NewConsumer(queueName string) (Consumer, error) {
	if err := client.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	); err != nil {
		return nil, err
	}

	return consumerImpl{channel: client.channel, queueName: queueName}, nil
}
