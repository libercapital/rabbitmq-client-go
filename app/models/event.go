package models

import (
	"encoding/json"
	"time"
)

type Event struct {
	// unique id of the call
	ID string `json:"id"`
	// name of the object to suffer the event
	Object string `json:"object"`
	//  Request ID received from API Gateway and used for tracing
	RequestID string `json:"request_id"`
	// flag error response
	HasError bool `json:"has_error"`
	// readed message time
	ReadedTime time.Time `json:"-"`
	// created message time
	CreatedTime string `json:"created_time"`
	// JSON body of the event
	Properties interface{} `json:"properties"`
}

func (e Event) Json() string {
	b, _ := json.Marshal(e)
	return string(b)
}
