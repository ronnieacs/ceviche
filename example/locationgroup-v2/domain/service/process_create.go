package service

import (
	"fmt"
	"locationgroup/domain/aggregate"
	"locationgroup/domain/command"
	"locationgroup/domain/event"
)

func (svc serviceImpl) ProcessCreate(_ *aggregate.LocationGroup, cmd *command.Create) (*event.Created, error) {
	if len(cmd.Locations) == 0 {
		return nil, fmt.Errorf("empty locations array")
	}

	return &event.Created{
		Description: cmd.Description,
		Locations:   cmd.Locations,
	}, nil
}
