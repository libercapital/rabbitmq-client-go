package clientRouting

import (
	"context"
	"encoding/json"

	"github.com/streadway/amqp"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/rabbitmq-client.git/app/models"
	"golang.org/x/sync/errgroup"
)

type Consumer interface {
	SubscribeEvents(ctx context.Context, consumerEvent models.ConsumerEvent) error
}

type consumerImpl struct {
	channel *amqp.Channel
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
			messages, err := consumer.channel.Consume("", "", false, false, false, false, nil)
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
