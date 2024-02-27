# Welcome to RabbitMQ client module 👋

![Version](https://img.shields.io/badge/version-3.0.0-blue.svg?cacheSeconds=2592000)

> Module to connect Bava's apps to RabbitMq Instance

## Install

```go
go get github.com/libercapital/rabbitmq-client-go/v3
```

## Usage

> To use this module needs to instantiate a publisher or a consumer...
> This module is based on the examples presented at www.rabbitmq.com

> The following standards are implemented:
>
> - [Tutorial Rabbit MQ Go - Work Queues](https://www.rabbitmq.com/tutorials/tutorial-two-go.html)
> - [Tutorial Rabbit MQ Go - Routing](https://www.rabbitmq.com/tutorials/tutorial-four-go.html)
> - [Tutorial Rabbit MQ Go - Remote procedure call (RPC)](https://www.rabbitmq.com/tutorials/tutorial-six-go.html)
> - [RPC - Direct Reply To](https://www.rabbitmq.com/direct-reply-to.html)

### Create a RabbitMQ connection

```go
var vhost = "vhost"

credential := rabbitmq.Credential{
  Host:     "host",
  User:     "user",
  Password: "password",
  Vhost:    &vhost //optional default User as vhost,
  Protocol: rabbitmq.AMQPS //optional default AMQPS
}

options := rabbitmq.ClienOptions{
  ReconnectionDelay: 1, //time in seconds to try to reconnect when the connection is broken
  Declare: false, // Declare consumers and producers configurations, default is false
}

client, err := rabbitmq.New(credential, options)

// if needed, the connection for the ampq its shared by the function GetConnection
amqpConnection := client.GetConnection()
```

### Simple queue publisher code

Create new publisher

```go
publisher, err := client.NewPublisher(
  &rabbitmq.QueueArgs{
    Name: "queue-name",
  },
  nil,
)
```

Sending message

```go
messageId := uuid.NewString()

type BodyMessage struct {
  Value string `json:"value"`
}

jsonBodyMessage := BodyMessage{
  Value: "value",
}

bodyMessage := rabbitmq.IncomingEventMessage{
  Source: constants.ApplicationName,
  Content: rabbitmq.Event{
    ID:         messageId,
    Object:     event,
    Properties: jsonBodyMessage,
  },
}

content, _ := json.Marshal(bodyMessage)
message := rabbitmq.PublishingMessage{
	Body: content,
}

queueName, _ := publisher.GetQueueName()

err := queue.SendMessage(
  "",        //exchange
  queueName, //routing key -> Queue name
  false,     //mandatory
  false,     //imediate
  message    //message interface
)
```

### Simple routing key publisher code

Create new publisher

```go
publisher, err := client.NewPublisher(
  nil,
  &rabbitmq.ExchangeArgs{
    Name : "exchange-name",
    Type : "direct",
    Durable : false,
    AutoDelete : false,
    Internal : false,
    NoWait : false,
  },
)
```

Sending message

```go
messageId := uuid.NewString()

bodyMessage := rabbitmq.IncomingEventMessage{
  Source: constants.ApplicationName,
  Content: rabbitmq.Event{
    ID:         messageId,
    Object:     event,
    Properties: jsonBodyMessage,
  },
}

content, _ := json.Marshal(bodyMessage)
message := rabbitmq.PublishingMessage{
	Body: content,
}

exchangeName, _ := publisher.GetExchangeName()

err := queue.SendMessage(
  exchangeName,
  routingKey,
  false,          //mandatory
  false,          //imediate
  message         //message interface
)
```

### Simple RPC implementation code

RPC consists into Send message and receives an reply. Its required and correlation ID

```go
exchange := "exchange if publish into one"
routeKey := "routing key when publish into exchange or queue name"
messageToPublish := rabbitmq.IncomingEventMessage{}
timout := 25 //consumer timeout

message, err := client.DirectReplyTo(ctx, exchange, routeKey, timeout, messageToPublish)
```

### Simple queue consumer code

```go
consumer, err := client.NewConsumer(&rabbitmq.ConsumerArgs{QueueName:"queue-name"})

event := rabbitmq.ConsumerEvent{
  Handler:   func(message model.IncomingEventMessage) bool,
}
ctx := context.Background()
err = consumer.SubscribeEvents(ctx, event, 10) // 10 threads
```

### Simple queue consumer code - using DLQ and TTL of 30 seconds and Redelivery true

```go
consumer, err := client.NewConsumer(
  &rabbitmq.ConsumerArgs{
    QueueName: "queue-name",
    TimeToLive: 30000,
    DeadLetterName: "dlq-queue-name",
    Redelivery: true, // try again once
  }
)

event := rabbitmq.ConsumerEvent{
  Handler:   func(message model.IncomingEventMessage) bool,
}
ctx := context.Background()
err = consumer.SubscribeEvents(ctx, event, 10) // 10 threads
```

### Simple routing key consumer code

```go
consumer, err := client.NewConsumer(
  &rabbitmq.ConsumerArgs{
    RoutingKey: "routing-key", //chave para roteamento no exchange
    QueueName: "queue-name"
    ExchangeArgs: &rabbitmq.ExchangeArgs{
      Name : "exchange-name",
      Type : "direct",
      Durable : false,
      AutoDelete : false,
      Internal : false,
      NoWait : false,
    }
  }
)

event := rabbitmq.ConsumerEvent{
  Handler:   func(message model.IncomingEventMessage) bool,
}
ctx := context.Background()
err = consumer.SubscribeEvents(ctx, event, 5) //5 threads
```