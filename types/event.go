package types

import "time"

type Event interface {
}

type EventWithMetadata struct {
	ID            uint64    `json:"ID"`
	Type          string    `json:"Type"`
	Time          time.Time `json:"Time"`
	Author        string    `json:"Author"`
	Payload       Event     `json:"Payload"`
	RequestID     string    `json:"RequestID"`
	CorrelationID string    `json:"CorrelationID"`
}
