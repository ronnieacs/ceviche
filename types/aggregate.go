package types

import "time"

type Aggregate interface {
}

type AggregateWithMetadata struct {
	ID        string    `json:"ID"`
	Type      string    `json:"Type"`
	Version   uint64    `json:"Version"`
	Payload   Aggregate `json:"Payload"`
	UpdatedAt time.Time `json:"UpdatedAt"`
	UpdatedBy string    `json:"UpdatedBy"`
	CreatedAt time.Time `json:"CreatedAt"`
	CreatedBy string    `json:"CreatedBy"`
}
