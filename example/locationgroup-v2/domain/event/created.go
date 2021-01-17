package event

import (
	"locationgroup/domain/aggregate"
)

type Created struct {
	Description *string               `json:"Description,omitempty"`
	Locations   []*aggregate.Location `json:"Locations,omitempty"`
}
