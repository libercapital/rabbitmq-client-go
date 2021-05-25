package client

import (
	"context"
	"encoding/json"
	"errors"
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
	queueName string
	channel   *amqp.Channel
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
			messages, err := consumer.channel.Consume(consumer.queueName, "", false, false, false, false, nil)
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

				success := consumerEvent.Handler(event)
				if success {
					message.Ack(true)
				} else {
					message.Nack(false, false) //To move to dlq we need to send a Nack with requeue = false
				}
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

	messages, err := consumer.channel.Consume(consumer.queueName, tag, false, false, false, false, nil)
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
