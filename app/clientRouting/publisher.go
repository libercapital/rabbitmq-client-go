package clientRouting

import (
	"github.com/streadway/amqp"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/rabbitmq-client.git/app/models"
)

type Publisher interface {
	SendMessage(exchange string, routingKey string, mandatory bool, immediate bool, message models.PublishingMessage) error
}

type publisherImpl struct {
	channel *amqp.Channel
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
