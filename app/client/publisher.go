package client

import (
	"fmt"

	"github.com/streadway/amqp"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/rabbitmq-client.git/app/models"
)

type Publisher interface {
	GetQueueName() (*string, error)
	GetExchangeName() (*string, error)
	SendMessage(exchange string, routingKey string, mandatory bool, immediate bool, message models.PublishingMessage) error
}

type publisherImpl struct {
	channel      *amqp.Channel
	queue        *amqp.Queue
	exchangeName *string
}

func (publish publisherImpl) SendMessage(exchange string, routingKey string, mandatory bool, immediate bool, message models.PublishingMessage) error {
	if message.ContentType == "" {
		message.ContentType = "application/json"
	}

	return publish.channel.Publish(
		exchange,
		routingKey,
		mandatory,
		immediate,
		amqp.Publishing(message),
	)
}

func (publish publisherImpl) GetQueueName() (*string, error) {
	if publish.queue == nil {
		return nil, fmt.Errorf("not connect to a queue")
	}

	return &publish.queue.Name, nil
}

func (publish publisherImpl) GetExchangeName() (*string, error) {
	if publish.exchangeName == nil {
		return nil, fmt.Errorf("exchange name not defined")
	}

	return publish.exchangeName, nil
}
