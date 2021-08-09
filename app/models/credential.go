package models

import "fmt"

type AmqpProtocol = string

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
