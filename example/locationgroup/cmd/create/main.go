package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/rxac/ceviche/ceviche"
	"github.com/rxac/ceviche/config"
	"locationgroup/domain/aggregate"
	"locationgroup/domain/command"
	"locationgroup/domain/service"
	"reflect"
)

func handler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

	domainService := service.New()
	newAggregate := &aggregate.LocationGroup{}
	domainCommand := &command.Create{}
	err := json.Unmarshal([]byte(request.Body), domainCommand)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	event, err := domainService.ProcessCreate(newAggregate, domainCommand)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	newAggregate, err = domainService.ApplyCreated(newAggregate, event)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	aggregateWithMetadata := ceviche.NewAggregateWithMetadata(ctx)
	commandWithMetadata := ceviche.AddCommandMetadataFromAPIGatewayProxyRequest(ctx, request, domainCommand)
	eventWithMetadata := ceviche.AddEventMetadataFromCommandAndAggregate(aggregateWithMetadata, commandWithMetadata, event)
	aggregateWithMetadata = ceviche.UpdateAggregateWithMetadata(aggregateWithMetadata, eventWithMetadata, newAggregate)

	eventStore := ceviche.EventStore(ctx)
	err = eventStore.Save(ctx, aggregateWithMetadata, eventWithMetadata)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	jsonResponse, _ := json.Marshal(aggregateWithMetadata)
	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       string(jsonResponse),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, nil
}

func main() {
	cfg := config.Config{
		AggregateType:  reflect.TypeOf(&aggregate.LocationGroup{}),
		ObjectStoreUse: config.ObjectStoreUseNone,
		EnableSnpList:  false,
	}
	ceviche.Start(cfg, handler)
}
