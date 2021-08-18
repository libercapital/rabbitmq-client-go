# Welcome to RabbitMQ client module 👋

![Version](https://img.shields.io/badge/version-0.0.19-blue.svg?cacheSeconds=2592000)

> Module to connect Bava's apps to RabbitMq Instance

## Install

```go
go get gitlab.com/bavatech/architecture/software/libs/go-modules/rabbitmq-client.git
```

## Usage

> To use this module needs to instantiate a publisher or a consumer...
> This module is based on the examples presented at www.rabbitmq.com

> The following standards are implemented:
>
> - [Tutorial Rabbit MQ Go - Work Queues](https://www.rabbitmq.com/tutorials/tutorial-two-go.html)
> - [Tutorial Rabbit MQ Go - Routing](https://www.rabbitmq.com/tutorials/tutorial-four-go.html)
> - [Tutorial Rabbit MQ Go - Remote procedure call (RPC)](https://www.rabbitmq.com/tutorials/tutorial-six-go.html)

### Create a RabbitMQ connection

```go
var vhost = "vhost"

credential := rabbit_models.Credential{
  Host:     "host",
  User:     "user",
  Password: "password",
  Vhost:    &vhost //optional default User as vhost,
  Protocol: rabbit_models.AMQPS //optional default AMQPS
}

delay := 1 //time in seconds to try to reconnect when the connection is broken
client, err := rabbit_client.New(credential, delay)

// if needed, the connection for the ampq its shared by the function GetConnection
amqpConnection := client.GetConnection()
```

### Simple queue publisher code

Create new publisher

```go
publisher, err := client.NewPublisher(
  &rabbit_models.QueueArgs{
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

bodyMessage := rabbit_models.IncomingEventMessage{
  Source: constants.ApplicationName,
  Content: rabbit_models.Event{
    ID:         messageId,
    Object:     event,
    Properties: jsonBodyMessage,
  },
}

content, _ := json.Marshal(bodyMessage)
message := rabbit_models.PublishingMessage{
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
  &rabbit_models.ExchangeArgs{
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

bodyMessage := rabbit_models.IncomingEventMessage{
  Source: constants.ApplicationName,
  Content: rabbit_models.Event{
    ID:         messageId,
    Object:     event,
    Properties: jsonBodyMessage,
  },
}

content, _ := json.Marshal(bodyMessage)
message := rabbit_models.PublishingMessage{
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

### Simple RPC queue consumer code

```go
consumer, err := client.NewConsumer(&rabbit_models.ConsumerArgs{QueueName: "queue-name"})

event := rabbit_models.ConsumerEvent{
  Handler: func(message model.IncomingEventMessage) bool,
  Timeout: 25, //25 seconds
}
ctx := context.Background()
err = consumer.ReadMessage(ctx, correlationID, event)
```

### Simple queue consumer code

```go
consumer, err := client.NewConsumer(&rabbit_models.ConsumerArgs{QueueName:"queue-name"})

event := rabbit_models.ConsumerEvent{
  Handler:   func(message model.IncomingEventMessage) bool,
}
ctx := context.Background()
err = consumer.SubscribeEvents(ctx, event, 10) // 10 threads
```

### Simple queue consumer code - using DLQ and TTL of 30 seconds and Redelivery true

```go
consumer, err := client.NewConsumer(
  &rabbit_models.ConsumerArgs{
    QueueName: "queue-name",
    TimeToLive: 30000,
    DeadLetterName: "dlq-queue-name",
    Redelivery: true, // try again once
  }
)

event := rabbit_models.ConsumerEvent{
  Handler:   func(message model.IncomingEventMessage) bool,
}
ctx := context.Background()
err = consumer.SubscribeEvents(ctx, event, 10) // 10 threads
```

### Simple routing key consumer code

```go
consumer, err := client.NewConsumer(
  &rabbit_models.ConsumerArgs{
    RoutingKey: "routing-key", //chave para roteamento no exchange
    QueueName: "queue-name"
    ExchangeArgs: &rabbit_models.ExchangeArgs{
      Name : "exchange-name",
      Type : "direct",
      Durable : false,
      AutoDelete : false,
      Internal : false,
      NoWait : false,
    }
  }
)

event := rabbit_models.ConsumerEvent{
  Handler:   func(message model.IncomingEventMessage) bool,
}
ctx := context.Background()
err = consumer.SubscribeEvents(ctx, event, 5) //5 threads
```

## Author

👤 **Eduardo Mello**

- Gitlab: [@eduardo.mello@bavabank.com](https://gitlab.com/eduardo.mello)

## Contributors

👤 **Vinícius Deuner**

- Gitlab: [@vinicius.deuner@bavabank.com](https://gitlab.com/vinicius.deuner)

## Show your support

Give a ⭐️ if this project helped you!

---

_This README was generated with ❤️ by [readme-md-generator](https://github.com/kefranabg/readme-md-generator)_
