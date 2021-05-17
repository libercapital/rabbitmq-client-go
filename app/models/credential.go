package models

import "fmt"

type Credential struct {
	Host     string
	User     string
	Password string
	Vhost    *string
}

func (credential Credential) GetConnectionString() string {
	var vhost string
	if credential.Vhost == nil {
		vhost = credential.User
	} else {
		vhost = *credential.Vhost
	}

	return fmt.Sprintf("amqps://%s:%s@%s/%s", credential.User, credential.Password, credential.Host, vhost)
}
