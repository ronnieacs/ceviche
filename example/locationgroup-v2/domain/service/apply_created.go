package service

import (
	"locationgroup/domain/aggregate"
	"locationgroup/domain/event"
)

func (svc serviceImpl) ApplyCreated(_ *aggregate.LocationGroup, event *event.Created) (*aggregate.LocationGroup, error) {

	initialStatus := aggregate.Active
	return &aggregate.LocationGroup{
		Status:      &initialStatus,
		Description: event.Description,
		Locations:   event.Locations,
	}, nil
}
