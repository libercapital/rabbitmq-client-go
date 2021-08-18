package client

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/rabbitmq-client.git/app/models"
)

type Client interface {
	NewPublisher(queueArgs *models.QueueArgs, exchangeArgs *models.ExchangeArgs) (Publisher, error)
	NewConsumer(args models.ConsumerArgs) (Consumer, error)
	GetConnection() *amqp.Connection
}

type clientImpl struct {
	credential        models.Credential
	reconnectionDelay int

	connection *amqp.Connection
	closed     int32
}

// IsClosed indicate closed by developer
func (client *clientImpl) IsClosed() bool {
	return (atomic.LoadInt32(&client.closed) == 1)
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

	go func() {
		for {
			_, ok := <-client.connection.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok {
				break
			}

			// reconnect if not closed by developer
			for {
				// wait time for connection reconnect
				time.Sleep(time.Duration(client.reconnectionDelay) * time.Second)

				conn, err := amqp.Dial(client.credential.GetConnectionString())
				if err == nil {
					client.connection = conn
					break
				}
			}
		}
	}()

	return nil
}

func New(credential models.Credential, reconnectionDelay int) Client {
	client := &clientImpl{credential: credential, reconnectionDelay: reconnectionDelay}
	err := client.connect()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	return client
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
