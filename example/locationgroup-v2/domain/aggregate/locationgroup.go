package aggregate

type LocationGroup struct {
	Status      *Status     `json:"Status,omitempty"`
	Description *string     `json:"Description,omitempty"`
	Locations   []*Location `json:"Locations,omitempty"`
}

type Status string

const (
	Active   Status = "ACTIVE"
	Inactive Status = "INACTIVE"
)

type Location struct {
	ID          *string       `json:"ID,omitempty"`
	Type        *LocationType `json:"Type,omitempty"`
	Description *string       `json:"Description,omitempty"`
}

type LocationType string

const (
	Department LocationType = "DEPARTMENT"
	Province   LocationType = "PROVINCE"
)
