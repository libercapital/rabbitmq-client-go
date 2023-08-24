package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"gitlab.com/bavatech/architecture/software/libs/go-modules/bavalogs.git"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher interface {
	GetQueueName() (*string, error)
	GetExchangeName() (*string, error)
	SendMessage(exchange string, routingKey string, mandatory bool, immediate bool, message PublishingMessage) error
	Publish(ctx context.Context, routerKey string, corrID string, exchange string, payload IncomingEventMessage) error
}

type publisherImpl struct {
	queueArgs    *QueueArgs
	exchangeArgs *ExchangeArgs
	client       *clientImpl

	channel      *amqp.Channel
	queue        *amqp.Queue
	exchangeName *string
	closed       int32
	declare      bool
}

// IsClosed indicate closed by developer
func (publisher *publisherImpl) IsClosed() bool {
	return (atomic.LoadInt32(&publisher.closed) == 1)
}

// Close ensure closed flag set
func (publisher *publisherImpl) Close() error {
	bavalogs.Debug(context.Background()).Stack().Msg("closing channel")

	if publisher.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&publisher.closed, 1)

	return publisher.channel.Close()
}

func (publish *publisherImpl) connect() error {
	publish.client.OnReconnect(func() {
		bavalogs.Debug(context.Background()).Bool("connection_isclosed", publish.client.connection.IsClosed()).Msg("reconnecting... creating new channel")

		err := publish.createChannel()

		if err != nil {
			bavalogs.Fatal(context.Background(), err).Msg("cannot recreate publisher channel in rabbitmq")
		}
	})

	return publish.createChannel()
}

func (publish *publisherImpl) createChannel() error {
	channel, err := publish.client.connection.Channel()
	if err != nil {
		return err
	}
	publish.channel = channel

	if publish.declare == false {
		return nil
	}

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

	return nil
}

func (publish *publisherImpl) SendMessage(exchange string, routingKey string, mandatory bool, immediate bool, message PublishingMessage) error {
	if message.ContentType == "" {
		message.ContentType = "application/json"
	}

	if publish.client.reconnecting != nil {
		bavalogs.Debug(context.Background()).Interface("publish.channel", publish.channel).Msg("waiting for reconnection")
		r := <-publish.client.reconnecting
		bavalogs.Debug(context.Background()).Bool("publish.client.reconnecting", r).Interface("publish.channel", publish.channel).Msg("waiting for reconnection")
	}

	if publish.client.connection.IsClosed() {
		if err := publish.client.connect(); err != nil {
			bavalogs.Debug(context.Background()).Err(err).Interface("publish.channel", publish.channel).Msg("failed to reconnect")
			return err
		}
	}

	return publish.channel.Publish(
		exchange,
		routingKey,
		mandatory,
		immediate,
		amqp.Publishing(message),
	)
}

func (publish *publisherImpl) Publish(ctx context.Context, routerKey string, corrID string, exchange string, payload IncomingEventMessage) error {
	content, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	message := PublishingMessage{
		Body:    content,
		ReplyTo: payload.Content.ReplyTo,
	}

	if corrID != "" {
		message.CorrelationId = corrID
	}

	err = publish.SendMessage(exchange, routerKey, false, false, message)
	if err != nil {
		return err
	}

	return nil
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
