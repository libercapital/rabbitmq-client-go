package models

import (
	"encoding/json"
	"time"
)

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
