package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/bavalogs.git/tracing"
)

var TIMEOUT_ERROR = errors.New("consumer timeout reached")

type AmqpProtocol = string
type PublishingMessage amqp.Publishing

const (
	AMQP  AmqpProtocol = "amqp"
	AMQPS AmqpProtocol = "amqps"
)

type Credential struct {
	Host     string
	User     string
	Password string
	Vhost    *string
	Protocol AmqpProtocol
}

type ClientOptions struct {
	ReconnectionDelay int
	Declare           bool
	HeartbeatTimeout  *int
	ChannelMax        int
}

func (credential Credential) GetConnectionString() string {
	if credential.Protocol == "" {
		credential.Protocol = AMQPS
	}

	var vhost string
	if credential.Vhost == nil {
		vhost = credential.User
	} else {
		vhost = *credential.Vhost
	}

	return fmt.Sprintf("%s://%s:%s@%s/%s", credential.Protocol, credential.User, credential.Password, credential.Host, vhost)
}

type ConsumerArgs struct {
	ExchangeArgs   *ExchangeArgs
	QueueName      *string
	RoutingKey     *string
	PrefetchCount  *int
	QosGlobal      bool
	DeadLetterName *string
	TimeToLive     *int //in milliseconds
	Redelivery     bool
	Durable        bool
	Tracer         tracing.SpanConfig
}

const DeadLetterExchangeName = "default-dlq-exchange"

type Event struct {
	// unique id of the call
	ID string `json:"id,omitempty"`
	// name of the object to suffer the event
	Object string `json:"object,omitempty"`
	//  Request ID received from API Gateway and used for tracing
	RequestID string `json:"request_id,omitempty"`
	// flag error response
	HasError bool `json:"has_error,omitempty"`
	// readed message time
	ReadedTime time.Time `json:"-"`
	// created message time
	CreatedTime string `json:"created_time,omitempty"`
	// used to address message
	ReplyTo string `json:"reply_to,omitempty"`
	// JSON body of the event
	Properties interface{} `json:"properties,omitempty"`
}

func (e Event) Json() string {
	b, _ := json.Marshal(e)
	return string(b)
}

type ExchangeArgs struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
}

type QueueArgs struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
}

type ConsumerEventHandler func(ctx context.Context, data IncomingEventMessage) bool

type ConsumerEvent struct {
	Handler ConsumerEventHandler
	//Seconds to timeout consume event
	Timeout int
}

type IncomingEventMessage struct {
	// The name of the service that published the message
	Source        string `json:"source"`
	CorrelationID string `json:"correlation_id,omitempty"`
	// The structure/values of the message
	Content Event `json:"content"`
}
