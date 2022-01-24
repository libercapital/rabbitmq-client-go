package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/bavalogs.git"
)

type Client interface {
	NewPublisher(queueArgs *QueueArgs, exchangeArgs *ExchangeArgs) (Publisher, error)
	NewConsumer(args ConsumerArgs) (Consumer, error)
	GetConnection() *amqp.Connection
	Close() error
	OnReconnect(func())
	DirectReplyTo(ctx context.Context, exchange, key string, timeout int, messge IncomingEventMessage) (IncomingEventMessage, error)
}

type clientImpl struct {
	credential        Credential
	reconnectionDelay int

	connection        *amqp.Connection
	closed            int32
	callbackReconnect []func()
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
	if client.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&client.closed, 1)

	return client.connection.Close()
}

func (client *clientImpl) connect() error {
	conn, err := amqp.Dial(client.credential.GetConnectionString())
	if err != nil {
		return err
	}
	client.connection = conn

	go func(connParam *amqp.Connection) {
		for {
			newConn, err := client.reconnect(connParam, client.credential.GetConnectionString())

			if newConn == nil {
				errMsg := errors.New("could not reconnect to rabbitmq")

				if err != nil {
					errMsg = err
				}

				bavalogs.Fatal(context.Background(), errMsg).Send()
			}

			if err != nil {
				bavalogs.Fatal(context.Background(), err).Send()
			}

			client.connection = newConn

			for _, callback := range client.callbackReconnect {
				callback()
			}
		}
	}(conn)

	return nil
}

func (client *clientImpl) reconnect(connParam *amqp.Connection, credentials string) (*amqp.Connection, error) {
	var err error
	retries := 0

	<-client.connection.NotifyClose(make(chan *amqp.Error))

	for {
		if retries >= 60 {
			err = fmt.Errorf("could not reconnect to rabbitmq after %d retries", retries)
			break
		}

		time.Sleep(time.Second)

		connParam, err := amqp.Dial(client.credential.GetConnectionString())

		if err != nil {
			bavalogs.Warn(context.Background()).Err(err).Msg("rabbitmq trying reconnect")
			retries++
			continue
		}

		return connParam, nil
	}

	return nil, err
}

func New(credential Credential, reconnectionDelay int) (Client, error) {
	client := &clientImpl{credential: credential, reconnectionDelay: reconnectionDelay}
	err := client.connect()
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (client *clientImpl) NewPublisher(queueArgs *QueueArgs, exchangeArgs *ExchangeArgs) (Publisher, error) {
	publish := publisherImpl{queueArgs: queueArgs, exchangeArgs: exchangeArgs, client: client}
	err := publish.connect()
	return &publish, err
}

func (client *clientImpl) NewConsumer(args ConsumerArgs) (Consumer, error) {
	consumer := consumerImpl{Args: args, client: client}
	err := consumer.connect()
	return &consumer, err
}

func (client *clientImpl) GetConnection() *amqp.Connection {
	return client.connection
}

// DirectReplyTo publish an message into queue and expect an response RPC formart
// Error can be typeof models.TIMEOUT_ERROR
func (client *clientImpl) DirectReplyTo(ctx context.Context, exchange, key string, timeout int, message IncomingEventMessage) (event IncomingEventMessage, err error) {
	clientId := uuid.NewString()

	var timer time.Timer
	if timeout > 0 {
		timer = *time.NewTimer(time.Duration(timeout) * time.Second)
	}

	channel, err := client.connection.Channel()
	if err != nil {
		return
	}

	messages, err := channel.Consume("amq.rabbitmq.reply-to", clientId, true, false, false, false, nil)
	if err != nil {
		return
	}

	b, _ := json.Marshal(message)
	if err = channel.Publish(exchange, key, false, false, amqp.Publishing{
		ReplyTo:       "amq.rabbitmq.reply-to",
		CorrelationId: message.CorrelationID,
		Body:          b,
	}); err != nil {
		return
	}

	defer channel.Cancel(clientId, false)

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
