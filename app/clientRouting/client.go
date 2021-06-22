package clientRouting

import (
	"fmt"

	"github.com/streadway/amqp"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/rabbitmq-client.git/app/models"
)

type Client interface {
	NewPublisher(args *models.ExchangeArgs) (Publisher, error)
	NewConsumer(args *models.ExchangeArgs, routingKey string) (Consumer, error)
}

type clientImpl struct {
	connection *amqp.Connection
}

func New(credential models.Credential) (Client, error) {
	conn, err := amqp.Dial(credential.GetConnectionString())
	if err != nil {
		return nil, fmt.Errorf("connection error: %v", err)
	}

	return clientImpl{connection: conn}, nil
}

func (client clientImpl) NewPublisher(args *models.ExchangeArgs) (Publisher, error) {
	if args == nil {
		return nil, fmt.Errorf("args should not be nil")
	}

	channel, err := client.connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("channel connection error: %v", err)
	}

	err = channel.ExchangeDeclare(args.Name, args.Type, args.Durable, args.AutoDelete, args.Internal, args.NoWait, nil)
	if err != nil {
		return nil, fmt.Errorf("exchange connection error: %v", err)
	}

	return publisherImpl{channel: channel}, nil
}

func (client clientImpl) NewConsumer(args *models.ExchangeArgs, routingKey string) (Consumer, error) {
	channel, err := client.connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("channel connection error: %v", err)
	}

	err = channel.ExchangeDeclare(args.Name, args.Type, args.Durable, args.AutoDelete, args.Internal, args.NoWait, nil)
	if err != nil {
		return nil, fmt.Errorf("exchange connection error: %v", err)
	}

	err = channel.QueueBind(
		"",
		routingKey,
		args.Name,
		false,
		nil)

	if err != nil {
		return nil, fmt.Errorf("queue connection error: %v", err)
	}

	return consumerImpl{channel: channel}, nil
}
