package service

import "context"

type ObjectStore interface {
	Load(ctx context.Context, objectID string, object interface{}) error
	Save(ctx context.Context, objectID string, object interface{}) error
}
