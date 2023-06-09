package rabbitmq

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"

	"gitlab.com/bavatech/architecture/software/libs/go-modules/bavalogs.git"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/bavalogs.git/tracing"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher interface {
	GetQueueName() (*string, error)
	GetExchangeName() (*string, error)
	SendMessage(ctx context.Context, exchange string, routingKey string, mandatory bool, immediate bool, message PublishingMessage, trace tracing.StartContextAndSpanConfig) error
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

func (publish *publisherImpl) SendMessage(ctx context.Context, exchange string, routingKey string, mandatory bool, immediate bool, message PublishingMessage, trace tracing.StartContextAndSpanConfig) (err error) {
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

	if trace.OperationName != "" {
		var span ddtrace.Span

		span, ctx = tracing.StartSpanFromContext(ctx, trace)

		defer func(e *error) {
			span.Finish(tracer.WithError(*e))
		}(&err)
	}

	if span, ok := tracing.SpanFromContext(ctx); ok {
		message.Headers = make(amqp.Table)

		message.Headers["x-datadog-trace-id"] = strconv.FormatUint(span.Context().TraceID(), 10)
	}

	return publish.channel.PublishWithContext(
		ctx,
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
