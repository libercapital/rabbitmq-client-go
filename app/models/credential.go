package models

import "fmt"

type Credential struct {
	Host     string
	User     string
	Password string
	Vhost    *string
	Protocol string
}

func (credential Credential) GetConnectionString() string {
	if credential.Protocol == "" {
		credential.Protocol = "amqps"
	}

	var vhost string
	if credential.Vhost == nil {
		vhost = credential.User
	} else if *credential.Vhost == "all" {
		vhost = ""
	} else {
		vhost = *credential.Vhost
	}

	return fmt.Sprintf("%s://%s:%s@%s/%s", credential.Protocol, credential.User, credential.Password, credential.Host, vhost)
}
