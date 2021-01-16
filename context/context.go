package context

import (
	"github.com/rxac/ceviche/service"
	"reflect"
)

const CevicheContextKey string = "CevicheContext"

type CevicheContext interface {
	AggregateType() reflect.Type
	EventStore() service.EventStore
	ObjectStore() service.ObjectStore
	SnapshotStore() service.SnapshotStore
}
