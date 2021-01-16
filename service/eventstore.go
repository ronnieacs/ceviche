package service

import (
	"context"
	"github.com/rxac/ceviche/types"
)

type EventStore interface {
	Save(ctx context.Context, aggregate types.AggregateWithMetadata, event types.EventWithMetadata) error
}
