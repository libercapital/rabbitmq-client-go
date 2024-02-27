package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"

	liberlogger "github.com/libercapital/liber-logger-go"
	"github.com/libercapital/liber-logger-go/tracing"
	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
)

type Consumer interface {
	SubscribeEvents(ctx context.Context, consumerEvent ConsumerEvent) error
	SubscribeEventsWithHealthCheck(ctx context.Context, consumerEvent ConsumerEvent, concurrency int, publisher Publisher) error
	GetQueue() amqp.Queue
	GetArgs() ConsumerArgs
}

type consumerImpl struct {
	Args    ConsumerArgs
	client  *clientImpl
	channel *amqp.Channel
	closed  int32
	queue   amqp.Queue
	declare bool
}

func (consumer *consumerImpl) GetArgs() ConsumerArgs {
	return consumer.Args
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
		if err := consumer.channel.Qos(*consumer.Args.PrefetchCount, 0, consumer.Args.QosGlobal); err != nil {
			return fmt.Errorf("prefetch count setting error: %v", err)
		}
	}

	if !consumer.declare {
		return nil
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

func (consumer *consumerImpl) SubscribeEvents(ctx context.Context, consumerEvent ConsumerEvent) error {
	consumer.client.OnReconnect(func() {
		go func() {
			err := consumer.createSubscribe(ctx, consumerEvent)

			if err != nil {
				liberlogger.Fatal(ctx, err).Msg("cannot recreate subscriber events in rabbitmq")
			}
		}()
	})

	return consumer.createSubscribe(ctx, consumerEvent)
}

func (consumer *consumerImpl) SubscribeEventsWithHealthCheck(ctx context.Context, consumerEvent ConsumerEvent, concurrency int, publisher Publisher) error {
	event := ConsumerEvent{
		Handler: func(ctx context.Context, data IncomingEventMessage) bool {
			if data.Content.Object == EventHealthCheck {
				publisher.SendMessage(ctx, "", data.Content.ReplyTo, false, false, PublishingMessage{
					CorrelationId: data.CorrelationID,
					Body:          []byte(`{"status": "ok"}`),
				}, tracing.SpanConfig{})
				return true
			}
			return consumerEvent.Handler(ctx, data)
		},
	}

	consumer.client.OnReconnect(func() {
		go func() {
			err := consumer.createSubscribe(ctx, event)

			if err != nil {
				liberlogger.Fatal(ctx, err).Msg("cannot recreate subscriber events in rabbitmq")
			}
		}()
	})

	return consumer.createSubscribe(ctx, event)
}

func (consumer *consumerImpl) createSubscribe(ctx context.Context, consumerEvent ConsumerEvent) error {
	var messages <-chan amqp.Delivery

	channel, err := consumer.client.connection.Channel()
	if err != nil {
		return err
	}

	if consumer.Args.PrefetchCount != nil {
		if err := channel.Qos(*consumer.Args.PrefetchCount, 0, consumer.Args.QosGlobal); err != nil {
			return fmt.Errorf("prefetch count setting error: %v", err)
		}
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
		liberlogger.Info(ctx).Interface("queue", consumer.Args.QueueName).Msg("Consumer channel has been closed")
	}()

	for message := range messages {
		processMessage(consumer.Args.Tracer, consumerEvent.Handler, consumer.Args, message)
	}

	return nil
}

func (consumer consumerImpl) buildQueueParameters() amqp.Table {
	if consumer.Args.DeadLetterName == nil && consumer.Args.TimeToLive == nil {
		return nil
	}

	args := make(amqp.Table)
	if consumer.Args.DeadLetterName != nil {
		args["x-dead-letter-exchange"] = DeadLetterExchangeName
		args["x-dead-letter-routing-key"] = *consumer.Args.QueueName
	}
	if consumer.Args.TimeToLive != nil {
		args["x-message-ttl"] = *consumer.Args.TimeToLive
	}

	return args
}

func (consumer consumerImpl) buildDeadLetterQueue() error {
	if consumer.Args.DeadLetterName != nil {
		if err := consumer.channel.ExchangeDeclare(DeadLetterExchangeName, "direct", true, false, false, false, nil); err != nil {
			return fmt.Errorf("dead letter exchange declare connection error: %v", err)
		}

		if _, err := consumer.channel.QueueDeclare(*consumer.Args.DeadLetterName, true, false, false, false, nil); err != nil {
			return fmt.Errorf("dead letter queue declare connection error: %v", err)
		}

		if err := consumer.channel.QueueBind(*consumer.Args.DeadLetterName, *consumer.Args.QueueName, DeadLetterExchangeName, false, nil); err != nil {
			return fmt.Errorf("dead letter exchange queue bind connection error: %v", err)
		}
	}
	return nil
}

func (consumer consumerImpl) GetQueue() amqp.Queue {
	return consumer.queue
}

func processMessage(tracer tracing.SpanConfig, handler ConsumerEventHandler, args ConsumerArgs, message amqp.Delivery) {
	var tracerID uint64
	var span ddtrace.Span
	ctxTrace := context.Background()

	if traceID, hasTrace := message.Headers["x-datadog-trace-id"]; hasTrace {
		traceIDUint, err := strconv.ParseUint(traceID.(string), 10, 64)

		if err != nil {
			liberlogger.Error(ctxTrace, err).Msg("error converting traceID")
		}

		tracerID = traceIDUint
	}

	if tracer.OperationName != "" {
		ctxTrace, span = tracing.StartContextAndSpan(ctxTrace, tracing.SpanConfig{
			OperationName: tracer.OperationName,
			SpanType:      tracer.SpanType,
			ResourceName:  tracer.ResourceName,
			TraceID:       tracerID,
			Tags:          tracer.Tags,
		})

		defer span.Finish()
	}

	var body []byte
	if message.Body != nil {
		body = message.Body
	}

	var event IncomingEventMessage

	if err := json.Unmarshal(body, &event); err != nil {
		liberlogger.
			Error(ctxTrace, err).
			Interface("corr_id", message.CorrelationId).
			Interface("queue", args.QueueName).
			Msg("Failed to unmarshal incoming event message, sending message do dlq")

		message.Nack(false, false) //To move to dlq we need to send a Nack with requeue = false
		return
	}

	event.Content.ReplyTo = message.ReplyTo
	event.CorrelationID = message.CorrelationId

	if event.Content.Object != EventHealthCheck {
		liberlogger.Debug(ctxTrace).
			Interface("id", event.Content.ID).
			Interface("corr_id", message.CorrelationId).
			Interface("queue", args.QueueName).
			Msg("Received AMQP message")
	}

	// if tha handler returns true then ACK, else NACK
	// the message back into the rabbit queue for another round of processing
	if handler(ctxTrace, event) {
		message.Ack(false)
	} else if args.Redelivery && !message.Redelivered {
		message.Nack(false, true)
	} else {
		message.Nack(false, false)
	}
}
