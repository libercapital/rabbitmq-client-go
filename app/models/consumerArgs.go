package models

type ConsumerArgs struct {
	ExchangeArgs   *ExchangeArgs
	QueueName      *string
	RoutingKey     *string
	PrefetchCount  *int
	DeadLetterName *string
	TimeToLive     *int //in milliseconds
}

const DeadLetterExchangeName = "default-dlq-exchange"
