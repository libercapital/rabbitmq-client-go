package client

import (
	"fmt"

	"github.com/streadway/amqp"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/rabbitmq-client.git/app/models"
)

type Client interface {
	NewPublisher(queueArgs *models.QueueArgs, exchangeArgs *models.ExchangeArgs) (Publisher, error)
	NewConsumer(queueName string) (Consumer, error)
	NewConsumerExchange(args *models.ExchangeArgs, routingKey string, queueName string) (Consumer, error)
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

func (client clientImpl) NewPublisher(queueArgs *models.QueueArgs, exchangeArgs *models.ExchangeArgs) (Publisher, error) {
	if queueArgs == nil && exchangeArgs == nil {
		return nil, fmt.Errorf("queueArgs and exchangeArgs should not both be nil")
	}

	channel, err := client.connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("channel connection error: %v", err)
	}

	if queueArgs != nil {
		queue, err := channel.QueueDeclare(queueArgs.Name, queueArgs.Durable, queueArgs.AutoDelete, queueArgs.Exclusive, queueArgs.NoWait, nil)
		if err != nil {
			return nil, fmt.Errorf("queue connection error: %v", err)
		}
		return publisherImpl{channel: channel, queue: &queue}, nil
	} else {
		err = channel.ExchangeDeclare(exchangeArgs.Name, exchangeArgs.Type, exchangeArgs.Durable, exchangeArgs.AutoDelete, exchangeArgs.Internal, exchangeArgs.NoWait, nil)
		if err != nil {
			return nil, fmt.Errorf("exchange connection error: %v", err)
		}

		exchangeName := exchangeArgs.Name

		return publisherImpl{channel: channel, queue: nil, exchangeName: &exchangeName}, nil
	}

}

func (client clientImpl) NewConsumer(queueName string) (Consumer, error) {
	channel, err := client.connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("channel connection error: %v", err)
	}

	return consumerImpl{channel: channel, queueName: queueName}, nil
}

func (client clientImpl) NewConsumerExchange(args *models.ExchangeArgs, routingKey string, queueName string) (Consumer, error) {
	channel, err := client.connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("channel connection error: %v", err)
	}

	err = channel.ExchangeDeclare(args.Name, args.Type, args.Durable, args.AutoDelete, args.Internal, args.NoWait, nil)
	if err != nil {
		return nil, fmt.Errorf("exchange connection error: %v", err)
	}

	_, err = channel.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	if err != nil {
		return nil, fmt.Errorf("queue connection error: %v", err)
	}

	err = channel.QueueBind(
		queueName,
		routingKey,
		args.Name,
		false,
		nil)

	if err != nil {
		return nil, fmt.Errorf("queue connection error: %v", err)
	}

	return consumerImpl{channel: channel, queueName: queueName}, nil
}
