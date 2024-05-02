package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	liberlogger "github.com/libercapital/liber-logger-go"
	"github.com/libercapital/liber-logger-go/tracing"
	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type Client interface {
	NewPublisher(queueArgs *QueueArgs, exchangeArgs *ExchangeArgs) (Publisher, error)
	NewConsumer(args ConsumerArgs) (Consumer, error)
	GetConnection() *amqp.Connection
	Close() error
	OnReconnect(func())
	DirectReplyTo(ctx context.Context, exchange, key string, timeout int, messge IncomingEventMessage, trace tracing.SpanConfig) (IncomingEventMessage, error)
	HealthCheck(publisher Publisher) bool
}

type clientImpl struct {
	credential        Credential
	reconnectionDelay int

	connection        *amqp.Connection
	closed            int32
	callbackReconnect []func()
	declare           bool

	reconnecting     chan bool
	heartbeatTimeout *int
	channelMax       int

	consumers []Consumer
}

func (client *clientImpl) HealthCheck(publisher Publisher) bool {
	ctx := context.Background()
	for _, consumer := range client.consumers {
		_, err := client.DirectReplyTo(ctx, consumer.GetArgs().ExchangeArgs.Name, *consumer.GetArgs().RoutingKey, 10, IncomingEventMessage{
			Content: Event{
				Object: EventHealthCheck,
			},
			CorrelationID: uuid.New().String(),
		}, tracing.SpanConfig{})
		if err != nil {
			liberlogger.Error(ctx, err).Interface("vhost", *client.credential.Vhost).Interface("exchange", consumer.GetArgs().ExchangeArgs.Name).Interface("router_key", *consumer.GetArgs().RoutingKey).Msg("error RPC message: " + err.Error())
			return false
		}
	}
	return true
}

// IsClosed indicate closed by developer
func (client *clientImpl) IsClosed() bool {
	return (atomic.LoadInt32(&client.closed) == 1)
}

func (client *clientImpl) OnReconnect(callback func()) {
	client.callbackReconnect = append(client.callbackReconnect, callback)
}

// Close ensure closed flag set
func (client *clientImpl) Close() error {
	liberlogger.Debug(context.Background()).Stack().Msg("closing connection")

	if client.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&client.closed, 1)

	return client.connection.Close()
}

func (client *clientImpl) connect() error {
	if client.heartbeatTimeout == nil {
		defaultValue := 10
		client.heartbeatTimeout = &defaultValue
	}

	conn, err := amqp.DialConfig(client.credential.GetConnectionString(), amqp.Config{
		Heartbeat:  time.Duration(*client.heartbeatTimeout) * time.Second,
		ChannelMax: client.channelMax,
	})

	if err != nil {
		return err
	}

	client.connection = conn

	go func(client *clientImpl) {
		for {
			shouldReconnect, err := client.reconnect()

			if err != nil {
				liberlogger.Fatal(context.Background(), err).Send()
			}

			if shouldReconnect {
				for _, callback := range client.callbackReconnect {
					callback()
				}
			}
		}
	}(client)

	return nil
}

func (client *clientImpl) reconnect() (shouldReconnect bool, err error) {
	retries := 0

	chanErr := <-client.connection.NotifyClose(make(chan *amqp.Error))

	if chanErr == nil {
		liberlogger.Debug(context.Background()).Msg("rabbitmq connection closed, no reconnection needed")
		return false, nil
	}

	client.reconnecting = make(chan bool)
	defer func() {
		close(client.reconnecting)
		client.reconnecting = nil
	}()

	for {
		if retries >= 60 {
			err = fmt.Errorf("could not reconnect to rabbitmq after %d retries", retries)
			break
		}

		liberlogger.Warn(context.Background()).Interface("chan_err", chanErr).Msg("rabbitmq connection lost, trying reconnect")

		time.Sleep(time.Second)

		client.connection, err = amqp.DialConfig(client.credential.GetConnectionString(), amqp.Config{
			Heartbeat:  time.Duration(*client.heartbeatTimeout) * time.Second,
			ChannelMax: client.channelMax,
		})

		if err != nil {
			liberlogger.Warn(context.Background()).Err(err).Msg("error rabbitmq trying reconnect")
			retries++
			continue
		}

		liberlogger.Info(context.Background()).Msg("rabbitmq reconnected")

		return true, nil
	}

	return false, err
}

func New(credential Credential, options ClientOptions) (Client, error) {
	client := &clientImpl{
		credential:        credential,
		reconnectionDelay: options.ReconnectionDelay,
		declare:           options.Declare,
		heartbeatTimeout:  options.HeartbeatTimeout,
		channelMax:        options.ChannelMax,
	}

	err := client.connect()
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (client *clientImpl) NewPublisher(queueArgs *QueueArgs, exchangeArgs *ExchangeArgs) (Publisher, error) {
	publish := publisherImpl{
		queueArgs:    queueArgs,
		exchangeArgs: exchangeArgs,
		client:       client,
		declare:      client.declare,
	}
	err := publish.connect()
	return &publish, err
}

func (client *clientImpl) NewConsumer(args ConsumerArgs) (Consumer, error) {
	consumer := consumerImpl{
		Args:    args,
		client:  client,
		declare: client.declare,
	}

	client.consumers = append(client.consumers, &consumer)

	err := consumer.connect()
	return &consumer, err
}

func (client *clientImpl) GetConnection() *amqp.Connection {
	return client.connection
}

// DirectReplyTo publish an message into queue and expect an response RPC formart
// Error can be typeof models.TIMEOUT_ERROR
func (client *clientImpl) DirectReplyTo(ctx context.Context, exchange, key string, timeout int, message IncomingEventMessage, trace tracing.SpanConfig) (event IncomingEventMessage, err error) {
	clientId := uuid.NewString()

	expiration := ""

	var timer time.Timer
	if timeout > 0 {
		timer = *time.NewTimer(time.Duration(timeout) * time.Second)
		expiration = strconv.Itoa(timeout * 1000)
	}

	channel, err := client.connection.Channel()
	if err != nil {
		return
	}

	messages, err := channel.Consume("amq.rabbitmq.reply-to", clientId, true, false, false, false, nil)
	if err != nil {
		return
	}

	var headers amqp.Table

	if trace.OperationName != "" {
		var span ddtrace.Span

		span, ctx = tracing.StartSpanFromContext(ctx, trace)

		defer func(e *error) {
			defer span.Finish(tracer.WithError(*e))
		}(&err)
	}

	if span, hasTrace := tracing.SpanFromContext(ctx); hasTrace {
		headers = make(amqp.Table)

		headers["x-datadog-trace-id"] = strconv.FormatUint(span.Context().TraceID(), 10)
	}

	b, _ := json.Marshal(message)
	if err = channel.PublishWithContext(ctx, exchange, key, false, false, amqp.Publishing{
		ReplyTo:       "amq.rabbitmq.reply-to",
		CorrelationId: message.CorrelationID,
		Expiration:    expiration,
		Body:          b,
		Headers:       headers,
	}); err != nil {
		return
	}

	defer func() {
		channel.Cancel(clientId, false)
		channel.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			return event, TIMEOUT_ERROR
		case msg := <-messages:
			if msg.CorrelationId == message.CorrelationID {
				var body []byte
				if msg.Body != nil {
					body = msg.Body
				}

				err = json.Unmarshal(body, &event)

				return
			}
		}
	}
}
