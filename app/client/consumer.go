package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/rabbitmq-client.git/app/models"
	"golang.org/x/sync/errgroup"
)

type Consumer interface {
	SubscribeEvents(ctx context.Context, consumerEvent models.ConsumerEvent) error
	ReadMessage(ctx context.Context, correlationID string, consumerEvent models.ConsumerEvent) error
}

type consumerImpl struct {
	Args    models.ConsumerArgs
	client  *clientImpl
	channel *amqp.Channel
	closed  int32
}

// IsClosed indicate closed by developer
func (consumer *consumerImpl) IsClosed() bool {
	return (atomic.LoadInt32(&consumer.closed) == 1)
}

// Close ensure closed flag set
func (consumer *consumerImpl) Close() error {
	if consumer.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&consumer.closed, 1)

	return consumer.channel.Close()
}

func (consumer *consumerImpl) connect() error {
	channel, err := consumer.client.connection.Channel()
	if err != nil {
		return fmt.Errorf("channel connection error: %v", err)
	}
	consumer.channel = channel

	channel.Qos(50, 0, false)

	if consumer.Args.ExchangeArgs != nil {
		args := consumer.Args.ExchangeArgs
		if err := channel.ExchangeDeclare(args.Name, args.Type, args.Durable, args.AutoDelete, args.Internal, args.NoWait, nil); err != nil {
			return fmt.Errorf("exchange declare connection error: %v", err)
		}

		if _, err := channel.QueueDeclare(*consumer.Args.QueueName, false, false, false, false, nil); err != nil {
			return fmt.Errorf("exchange queue declare connection error: %v", err)
		}

		if err := channel.QueueBind(*consumer.Args.QueueName, *consumer.Args.RoutingKey, consumer.Args.ExchangeArgs.Name, false, nil); err != nil {
			return fmt.Errorf("exchange queue bind connection error: %v", err)
		}
	}

	go func() {
		for {
			_, ok := <-channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || consumer.IsClosed() {
				channel.Close() // close again, ensure closed flag set when connection closed
				break
			}

			// reconnect if not closed by developer
			for {
				// wait time for connection reconnect
				time.Sleep(time.Duration(consumer.client.reconnectionDelay) * time.Second)

				ch, err := consumer.client.connection.Channel()
				if err == nil {
					consumer.channel = ch
					break
				}
			}
		}

	}()

	return nil
}

func (consumer consumerImpl) SubscribeEvents(ctx context.Context, consumerEvent models.ConsumerEvent) error {
	errs, errCtx := errgroup.WithContext(ctx)
	errs.Go(func() error {
		return consumer.getEvents(errCtx, consumerEvent)
	})

	return errs.Wait()
}

func (consumer consumerImpl) getEvents(ctx context.Context, consumerEvent models.ConsumerEvent) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			messages, err := consumer.channel.Consume(*consumer.Args.QueueName, "", false, false, false, false, nil)
			if err != nil {
				return err
			}

			for message := range messages {
				var body []byte
				if message.Body != nil {
					body = message.Body
				}
				var event models.IncomingEventMessage
				if err := json.Unmarshal(body, &event); err != nil {
					message.Nack(false, false) //To move to dlq we need to send a Nack with requeue = false
					continue
				}

				go func(messageParam amqp.Delivery) {
					success := consumerEvent.Handler(event)
					if success {
						messageParam.Ack(true)
					} else {
						messageParam.Nack(false, false) //To move to dlq we need to send a Nack with requeue = false
					}
				}(message)
			}
		}
	}
}

func (consumer consumerImpl) ReadMessage(ctx context.Context, correlationID string, consumerEvent models.ConsumerEvent) error {
	tag := uuid.NewString()
	var timer time.Timer
	if consumerEvent.Timeout > 0 {
		timer = *time.NewTimer(time.Duration(consumerEvent.Timeout) * time.Second)
	}

	messages, err := consumer.channel.Consume(*consumer.Args.QueueName, tag, false, false, false, false, nil)
	if err != nil {
		return err
	}

	defer consumer.channel.Cancel(tag, false)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			return errors.New("consumer timeout reached")
		case message := <-messages:
			var body []byte
			if message.Body != nil {
				body = message.Body
			}
			var event models.IncomingEventMessage
			if err := json.Unmarshal(body, &event); err != nil {
				message.Nack(false, false) //To move to dlq we need to send a Nack with requeue = false
				continue
			}

			if message.CorrelationId == correlationID {
				success := consumerEvent.Handler(event)

				if success {
					message.Ack(true)
					return nil
				} else {
					message.Nack(false, true) //Requeue true because we are searching by specif message
				}
			} else {
				message.Nack(false, true) //Requeue true because we are searching by specif message
			}
		}
	}
}
