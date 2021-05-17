# Welcome to RabbitMQ client module 👋

![Version](https://img.shields.io/badge/version-1.0.0-blue.svg?cacheSeconds=2592000)

> Module to connect Bava's apps to RabbitMq Instance

## Install

```sh
go get gitlab.com/icredit/bava/architecture/software/libs/go-modules/rabbit-client.git
```

## Usage

> To use this module needs to instantiate a publisher or a consumer...

### Create a RaabitMQ connection

```sh
credential := rabbit_models.Credential{
  Host:     "host",
  User:     "user",
  Password: "password",
  Vhost:    "vhost" //optional
}

client, err := rabbit_client.New(credential)
```

### Simple Publisher code

```sh
queue, err := client.NewPublisher(&rabbit_models.QueueArgs{
  Name: "queue-name",
})

err := queue.SendMessage(
  "",     //exchange
  "",     //routing key
  false,  //mandatory
  false,  //imediate
  message //message interfave
)
```

### Simple RPC Consumer code

```sh
event := rabbit_models.ConsumerEvent{
  QueueName: "queue-name",
  Handler:   func(message model.IncomingEventMessage) bool,
}
ctx := context.Background()
err = consumer.ReadMessage(ctx, correlationID, event)
```

### Simple Consumer code

```sh
event := rabbit_models.ConsumerEvent{
  QueueName: "queue-name",
  Handler:   func(message model.IncomingEventMessage) bool,
}
ctx := context.Background()
err = consumer.SubscribeEvents(ctx, event)
```

## Author

👤 **Eduardo Mello**

- Gitlab: [@eduardo.mello@bavabank.com](https://gitlab.com/eduardo.mello)

## Show your support

Give a ⭐️ if this project helped you!

---

_This README was generated with ❤️ by [readme-md-generator](https://github.com/kefranabg/readme-md-generator)_
