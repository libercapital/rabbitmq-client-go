package client

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
	"gitlab.com/bavatech/architecture/software/libs/go-modules/rabbitmq-client.git/app/models"
)

type Consumer interface {
	SubscribeEvents(ctx context.Context, consumerEvent models.ConsumerEvent, concurrency int) error
	ReadMessage(ctx context.Context, correlationID string, consumerEvent models.ConsumerEvent) error
	GetQueue() amqp.Queue
}

type consumerImpl struct {
	Args    models.ConsumerArgs
	client  *clientImpl
	channel *amqp.Channel
	closed  int32
	queue   amqp.Queue
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

	if consumer.Args.PrefetchCount != nil {
		if err := consumer.channel.Qos(*consumer.Args.PrefetchCount, 0, false); err != nil {
			return fmt.Errorf("prefetch count setting error: %v", err)
		}
	}

	if err := consumer.buildDeadLetterQueue(); err != nil {
		return fmt.Errorf("build dead letter queue error: %v", err)
	}

	if consumer.Args.ExchangeArgs != nil {
		args := consumer.Args.ExchangeArgs
		if err := channel.ExchangeDeclare(args.Name, args.Type, args.Durable, args.AutoDelete, args.Internal, args.NoWait, nil); err != nil {
			return fmt.Errorf("exchange declare connection error: %v", err)
		}
	}

	parameters := consumer.buildQueueParameters()
	if queue, err := channel.QueueDeclare(*consumer.Args.QueueName, consumer.Args.Durable, false, false, false, parameters); err != nil {
		return fmt.Errorf("exchange queue declare connection error: %v", err)
	} else {
		consumer.queue = queue
	}

	if consumer.Args.ExchangeArgs != nil {
		if err := channel.QueueBind(*consumer.Args.QueueName, *consumer.Args.RoutingKey, consumer.Args.ExchangeArgs.Name, false, nil); err != nil {
			return fmt.Errorf("exchange queue bind connection error: %v", err)
		}
	}

	return nil
}

func (consumer *consumerImpl) SubscribeEvents(ctx context.Context, consumerEvent models.ConsumerEvent, concurrency int) error {
	consumer.client.OnReconnect(func() {
		err := consumer.createSubscribe(ctx, consumerEvent, concurrency)

		if err != nil {
			bavalogs.Fatal(context.Background(), err).Msg("cannot recreate subscriber events in rabbitmq")
		}
	})

	return consumer.createSubscribe(ctx, consumerEvent, concurrency)
}

func (consumer *consumerImpl) createSubscribe(ctx context.Context, consumerEvent models.ConsumerEvent, concurrency int) error {
	var messages <-chan amqp.Delivery

	channel, err := consumer.client.connection.Channel()
	if err != nil {
		return err
	}

	if *consumer.Args.QueueName != "" {
		messages, err = channel.Consume(*consumer.Args.QueueName, "", false, false, false, false, nil)
	} else {
		messages, err = channel.Consume(consumer.queue.Name, "", false, false, false, false, nil)
	}

	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		channel.Close()
		bavalogs.Info(ctx).Interface("queue", consumer.Args.QueueName).Msg("Consumer channel has been closed")
	}()

	consumer.client.OnReconnect(func() {

	})

	for i := 0; i < concurrency; i++ {
		bavalogs.Info(ctx).Interface("queue", consumer.Args.QueueName).Msgf("Processing messages on thread %v", i)
		go func() {
			for message := range messages {
				var body []byte
				if message.Body != nil {
					body = message.Body
				}

				var event models.IncomingEventMessage
				if err := json.Unmarshal(body, &event); err != nil {
					bavalogs.
						Error(ctx, err).
						Interface("corr_id", message.CorrelationId).
						Interface("queue", consumer.Args.QueueName).
						Msg("Failed to unmarshal incoming event message, sending message do dlq")

					message.Nack(false, false) //To move to dlq we need to send a Nack with requeue = false
					continue
				}

				event.Content.ReplyTo = message.ReplyTo
				event.CorrelationID = message.CorrelationId

				bavalogs.Info(ctx).
					Interface("id", event.Content.ID).
					Interface("corr_id", message.CorrelationId).
					Interface("queue", consumer.Args.QueueName).
					Msg("Received AMQP message")

				// if tha handler returns true then ACK, else NACK
				// the message back into the rabbit queue for another round of processing
				if consumerEvent.Handler(event) {
					message.Ack(false)
				} else if consumer.Args.Redelivery && !message.Redelivered {
					message.Nack(false, true)
				} else {
					message.Nack(false, false)
				}
			}
		}()
	}

	return nil
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
			if message.CorrelationId == correlationID {
				var body []byte
				if message.Body != nil {
					body = message.Body
				}
				var event models.IncomingEventMessage
				if err := json.Unmarshal(body, &event); err != nil {
					message.Nack(false, false) //To move to dlq we need to send a Nack with requeue = false
					return err
				}

				success := consumerEvent.Handler(event)

				if success {
					message.Ack(false)
				} else {
					message.Nack(false, false) //Move to dlq
				}

				return nil
			} else {
				message.Nack(false, true) //Requeue true because we are searching by specif message
			}
		}
	}
}

func (consumer consumerImpl) buildQueueParameters() amqp.Table {
	if consumer.Args.DeadLetterName == nil && consumer.Args.TimeToLive == nil {
		return nil
	}

	args := make(amqp.Table)
	if consumer.Args.DeadLetterName != nil {
		args["x-dead-letter-exchange"] = models.DeadLetterExchangeName
		args["x-dead-letter-routing-key"] = *consumer.Args.QueueName
	}
	if consumer.Args.TimeToLive != nil {
		args["x-message-ttl"] = *consumer.Args.TimeToLive
	}

	return args
}

func (consumer consumerImpl) buildDeadLetterQueue() error {
	if consumer.Args.DeadLetterName != nil {
		if err := consumer.channel.ExchangeDeclare(models.DeadLetterExchangeName, "direct", true, false, false, false, nil); err != nil {
			return fmt.Errorf("dead letter exchange declare connection error: %v", err)
		}

		if _, err := consumer.channel.QueueDeclare(*consumer.Args.DeadLetterName, true, false, false, false, nil); err != nil {
			return fmt.Errorf("dead letter queue declare connection error: %v", err)
		}

		if err := consumer.channel.QueueBind(*consumer.Args.DeadLetterName, *consumer.Args.QueueName, models.DeadLetterExchangeName, false, nil); err != nil {
			return fmt.Errorf("dead letter exchange queue bind connection error: %v", err)
		}
	}
	return nil
}

func (consumer consumerImpl) GetQueue() amqp.Queue {
	return consumer.queue
}
