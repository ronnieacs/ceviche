package ceviche

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/google/uuid"
	"github.com/rxac/ceviche/types"
	"reflect"
	"time"
)

func AddCommandMetadataFromAPIGatewayProxyRequest(
	ctx context.Context,
	request events.APIGatewayProxyRequest,
	command types.Command) types.CommandWithMetadata {

	requestTimeUnix := request.RequestContext.RequestTimeEpoch * int64(time.Millisecond)

	correlationID := xray.TraceID(ctx)
	if correlationID == "" {
		correlationID = uuid.New().String()
	}

	var author string
	if request.RequestContext.Identity.CognitoIdentityID != "" {
		author = request.RequestContext.Identity.CognitoIdentityID
	} else {
		author = "unknown"
	}

	return types.CommandWithMetadata{
		ID:            request.RequestContext.RequestID,
		Type:          reflect.TypeOf(command).Name(),
		Time:          time.Unix(0, requestTimeUnix),
		Author:        author,
		Payload:       command,
		CorrelationID: correlationID,
	}
}

func AddEventMetadataFromCommandAndAggregate(
	aggregateWithMetadata types.AggregateWithMetadata,
	commandWithMetadata types.CommandWithMetadata,
	event types.Event) types.EventWithMetadata {

	return types.EventWithMetadata{
		ID:            aggregateWithMetadata.Version + 1,
		Type:          reflect.TypeOf(event).Name(),
		Time:          time.Now(),
		Author:        commandWithMetadata.Author,
		Payload:       event,
		RequestID:     commandWithMetadata.ID,
		CorrelationID: commandWithMetadata.CorrelationID,
	}
}

func NewAggregateWithMetadata(ctx context.Context) types.AggregateWithMetadata {

	aggType := AggregateType(ctx)
	aggregate := reflect.New(aggType).Elem().Interface().(types.Aggregate)
	return types.AggregateWithMetadata{
		ID:        uuid.New().String(),
		Type:      aggType.Name(),
		Version:   0,
		Payload:   aggregate,
		UpdatedAt: time.Now(),
		UpdatedBy: "unknown",
		CreatedAt: time.Now(),
		CreatedBy: "unknown",
	}
}

func UpdateAggregateWithMetadata(
	aggregateWithMetadata types.AggregateWithMetadata,
	eventWithMetadata types.EventWithMetadata,
	aggregate interface{}) types.AggregateWithMetadata {

	return types.AggregateWithMetadata{
		ID:        aggregateWithMetadata.ID,
		Type:      aggregateWithMetadata.Type,
		Version:   eventWithMetadata.ID,
		Payload:   aggregate,
		UpdatedAt: eventWithMetadata.Time,
		UpdatedBy: eventWithMetadata.Author,
		CreatedAt: aggregateWithMetadata.CreatedAt,
		CreatedBy: aggregateWithMetadata.CreatedBy,
	}
}
