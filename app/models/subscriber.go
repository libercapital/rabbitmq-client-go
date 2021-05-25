package models

type ConsumerEventHandler func(data IncomingEventMessage) bool

type ConsumerEvent struct {
	QueueName          string
	Handler            ConsumerEventHandler
	RetryMessagePeriod int
	//Seconds to timeout consume event
	Timeout int
}

type IncomingEventMessage struct {
	// The name of the service that published the message
	Source string `json:"source"`
	// The structure/values of the message
	Content Event `json:"content"`
}
