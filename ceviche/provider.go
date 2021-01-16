package ceviche

import (
	"context"
	cevichecontext "github.com/rxac/ceviche/context"
	"github.com/rxac/ceviche/service"
	"reflect"
)

func AggregateType(ctx context.Context) reflect.Type {
	return ctx.Value(cevichecontext.CevicheContextKey).(cevichecontext.CevicheContext).AggregateType()
}

func EventStore(ctx context.Context) service.EventStore {
	return ctx.Value(cevichecontext.CevicheContextKey).(cevichecontext.CevicheContext).EventStore()
}

func ObjectStore(ctx context.Context) service.ObjectStore {
	return ctx.Value(cevichecontext.CevicheContextKey).(cevichecontext.CevicheContext).ObjectStore()
}

func SnapshotStore(ctx context.Context) service.SnapshotStore {
	return ctx.Value(cevichecontext.CevicheContextKey).(cevichecontext.CevicheContext).SnapshotStore()
}
