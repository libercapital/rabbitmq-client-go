package models

type ConsumerArgs struct {
	ExchangeArgs *ExchangeArgs
	QueueName    *string
	RoutingKey   *string
}
