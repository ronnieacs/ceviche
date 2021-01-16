package service

import (
	"context"
	"github.com/rxac/ceviche/types"
	"time"
)

type SnapshotStore interface {
	List(ctx context.Context, continuationToken string) (types.AggregatePage, error)
	Versions(ctx context.Context, aggregateID string) (map[uint64]time.Time, error)
	Version(ctx context.Context, aggregateID string, aggregateVersion uint64) (types.AggregateWithMetadata, error)
	Latest(ctx context.Context, aggregateID string) (types.AggregateWithMetadata, error)
}
