package client

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	liberlogger "github.com/libercapital/liber-logger-go"
	"github.com/libercapital/rabbitmq-client-go/app/models"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Client interface {
	NewPublisher(queueArgs *models.QueueArgs, exchangeArgs *models.ExchangeArgs) (Publisher, error)
	NewConsumer(args models.ConsumerArgs) (Consumer, error)
	GetConnection() *amqp.Connection
	Close() error
	OnReconnect(func())
}

type clientImpl struct {
	credential        models.Credential
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

				liberlogger.Fatal(context.Background(), errMsg).Send()
			}

			if err != nil {
				liberlogger.Fatal(context.Background(), err).Send()
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
			liberlogger.Warn(context.Background()).Err(err).Msg("rabbitmq trying reconnect")
			retries++
			continue
		}

		return connParam, nil
	}

	return nil, err
}

func New(credential models.Credential, reconnectionDelay int) (Client, error) {
	client := &clientImpl{credential: credential, reconnectionDelay: reconnectionDelay}
	err := client.connect()
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (client *clientImpl) NewPublisher(queueArgs *models.QueueArgs, exchangeArgs *models.ExchangeArgs) (Publisher, error) {
	publish := publisherImpl{queueArgs: queueArgs, exchangeArgs: exchangeArgs, client: client}
	err := publish.connect()
	return &publish, err
}

func (client *clientImpl) NewConsumer(args models.ConsumerArgs) (Consumer, error) {
	consumer := consumerImpl{Args: args, client: client}
	err := consumer.connect()
	return &consumer, err
}

func (client *clientImpl) GetConnection() *amqp.Connection {
	return client.connection
}
