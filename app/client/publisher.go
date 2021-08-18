package client

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/rabbitmq-client.git/app/models"
)

type Publisher interface {
	GetQueueName() (*string, error)
	GetExchangeName() (*string, error)
	SendMessage(exchange string, routingKey string, mandatory bool, immediate bool, message models.PublishingMessage) error
}

type publisherImpl struct {
	queueArgs    *models.QueueArgs
	exchangeArgs *models.ExchangeArgs
	client       *clientImpl

	channel      *amqp.Channel
	queue        *amqp.Queue
	exchangeName *string
	closed       int32
}

// IsClosed indicate closed by developer
func (publisher *publisherImpl) IsClosed() bool {
	return (atomic.LoadInt32(&publisher.closed) == 1)
}

// Close ensure closed flag set
func (publisher *publisherImpl) Close() error {
	if publisher.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&publisher.closed, 1)

	return publisher.channel.Close()
}

func (publish *publisherImpl) connect() error {
	channel, err := publish.client.connection.Channel()
	if err != nil {
		return err
	}
	publish.channel = channel

	if publish.queueArgs != nil {
		queueArgs := publish.queueArgs
		queue, err := channel.QueueDeclare(queueArgs.Name, queueArgs.Durable, queueArgs.AutoDelete, queueArgs.Exclusive, queueArgs.NoWait, nil)
		if err != nil {
			return fmt.Errorf("queue connection error: %v", err)
		}
		publish.queue = &queue
	}
	if publish.exchangeArgs != nil {
		exchangeArgs := publish.exchangeArgs
		err = channel.ExchangeDeclare(exchangeArgs.Name, exchangeArgs.Type, exchangeArgs.Durable, exchangeArgs.AutoDelete, exchangeArgs.Internal, exchangeArgs.NoWait, nil)
		if err != nil {
			return fmt.Errorf("exchange connection error: %v", err)
		}

		publish.exchangeName = &exchangeArgs.Name
	}

	go func() {
		for {
			_, ok := <-publish.channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || publish.IsClosed() {
				channel.Close() // close again, ensure closed flag set when connection closed
				break
			}

			// reconnect if not closed by developer
			for {
				// wait time for connection reconnect
				time.Sleep(time.Duration(publish.client.reconnectionDelay) * time.Second)

				ch, err := publish.client.connection.Channel()
				if err == nil {
					publish.channel = ch
					break
				}
			}
		}

	}()

	return nil
}

func (publish *publisherImpl) SendMessage(exchange string, routingKey string, mandatory bool, immediate bool, message models.PublishingMessage) error {
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

func (publish *publisherImpl) GetQueueName() (*string, error) {
	if publish.queue == nil {
		return nil, fmt.Errorf("not connect to a queue")
	}

	return &publish.queue.Name, nil
}

func (publish *publisherImpl) GetExchangeName() (*string, error) {
	if publish.exchangeName == nil {
		return nil, fmt.Errorf("exchange name not defined")
	}

	return publish.exchangeName, nil
}
