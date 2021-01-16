package types

import "time"

type Command interface {
}

type CommandWithMetadata struct {
	ID            string    `json:"ID"`
	Type          string    `json:"Type"`
	Time          time.Time `json:"Time"`
	Author        string    `json:"Author"`
	Payload       Command   `json:"Payload"`
	CorrelationID string    `json:"CorrelationID"`
}
