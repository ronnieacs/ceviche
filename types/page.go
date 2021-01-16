package types

type AggregatePage struct {
	ContinuationToken string                  `json:"ContinuationToken"`
	Items             []AggregateWithMetadata `json:"Items"`
}
