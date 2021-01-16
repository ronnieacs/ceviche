package command

import "locationgroup/domain/aggregate"

type Create struct {
	Description *string               `json:"Description,omitempty"`
	Locations   []*aggregate.Location `json:"Locations,omitempty"`
}
