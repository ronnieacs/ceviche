package service

import (
	"locationgroup/domain/aggregate"
	"locationgroup/domain/command"
	"locationgroup/domain/event"
)

type Service interface {
	ProcessCreate(agg *aggregate.LocationGroup, cmd *command.Create) (*event.Created, error)
	ApplyCreated(agg *aggregate.LocationGroup, event *event.Created) (*aggregate.LocationGroup, error)
}

type serviceImpl struct {
}

func New() Service {
	return serviceImpl{}
}
