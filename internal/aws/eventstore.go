package aws

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/rxac/ceviche/service"
	"github.com/rxac/ceviche/types"
	"strconv"
)

type EventStore struct {
	service.EventStore
	MaintainLatestRecord bool
	EnableObjectStore    bool
	ObjectStoreClient    service.ObjectStore
	ObjectHeaderReader   func(interface{}) interface{}
	DynamoDBTable        string
	DynamoDBClient       dynamodbiface.DynamoDBAPI
}

func (es EventStore) Save(ctx context.Context, aggregate types.AggregateWithMetadata, event types.EventWithMetadata) error {

	if es.EnableObjectStore {
		err := es.saveEventIntoObjectStore(ctx, aggregate, event)
		if err != nil {
			return err
		}

		lightAggregate := types.AggregateWithMetadata{
			ID:        aggregate.ID,
			Type:      aggregate.Type,
			Version:   aggregate.Version,
			Payload:   es.ObjectHeaderReader(aggregate.Payload),
			UpdatedAt: aggregate.UpdatedAt,
			UpdatedBy: aggregate.UpdatedBy,
			CreatedAt: aggregate.CreatedAt,
			CreatedBy: aggregate.CreatedBy,
		}
		lightEvent := types.EventWithMetadata{
			ID:            event.ID,
			Type:          event.Type,
			Time:          event.Time,
			Author:        event.Author,
			Payload:       nil,
			RequestID:     event.RequestID,
			CorrelationID: event.CorrelationID,
		}
		return es.saveEventIntoEventStore(ctx, lightAggregate, lightEvent)
	} else {
		return es.saveEventIntoEventStore(ctx, aggregate, event)
	}
}

func (es EventStore) saveEventIntoEventStore(ctx context.Context, aggregate types.AggregateWithMetadata, event types.EventWithMetadata) error {

	if es.MaintainLatestRecord {
		return es.saveEventWithLatestRecord(ctx, aggregate, event)
	} else {
		return es.saveEventWithoutLatestRecord(ctx, aggregate, event)
	}
}

func (es EventStore) saveEventWithLatestRecord(ctx context.Context, aggregate types.AggregateWithMetadata, event types.EventWithMetadata) error {
	eventStatement, err := es.generateEventPutStatement(aggregate, event)
	if err != nil {
		return err
	}

	snapshotStatement, err := es.generateLatestSnapshotPutStatement(aggregate, event)
	if err != nil {
		return err
	}

	input := &dynamodb.TransactWriteItemsInput{
		TransactItems: []*dynamodb.TransactWriteItem{
			{Put: eventStatement},
			{Put: snapshotStatement},
		},
	}

	_, err = es.DynamoDBClient.TransactWriteItemsWithContext(ctx, input)
	if err != nil {
		return fmt.Errorf("cannot save event(%s:%s:%d) into event store\n%v", aggregate.Type, aggregate.ID, event.ID, err)
	}

	return nil
}

func (es EventStore) saveEventWithoutLatestRecord(ctx context.Context, aggregate types.AggregateWithMetadata, event types.EventWithMetadata) error {

	input, err := es.generateEventPutItemStatement(aggregate, event)
	if err != nil {
		return err
	}

	_, err = es.DynamoDBClient.PutItemWithContext(ctx, input)
	if err != nil {
		return fmt.Errorf("cannot save event(%s:%s:%d) into event store\n%v", aggregate.Type, aggregate.ID, event.ID, err)
	}

	return nil
}

func (es EventStore) generateEventPutStatement(aggregate types.AggregateWithMetadata, event types.EventWithMetadata) (*dynamodb.Put, error) {

	aggregatePayload, err := dynamodbattribute.MarshalMap(aggregate.Payload)
	if err != nil {
		return nil, fmt.Errorf("cannot generate statement to save event aggregate(%s:%s:%d)\n%v", aggregate.Type, aggregate.ID, event.ID, err)
	}

	eventPayload, err := dynamodbattribute.MarshalMap(event.Payload)
	if err != nil {
		return nil, fmt.Errorf("cannot generate statement to save event(%s:%s:%d)\n%v", aggregate.Type, aggregate.ID, event.ID, err)
	}

	return &dynamodb.Put{
		TableName:           aws.String(es.DynamoDBTable),
		ConditionExpression: aws.String("attribute_not_exists(#PK) AND attribute_not_exists(#SK)"),
		ExpressionAttributeNames: map[string]*string{
			"#PK": aws.String("PK"),
			"#SK": aws.String("SK"),
		},
		Item: map[string]*dynamodb.AttributeValue{
			"PK":                 {S: aws.String(aggregate.Type + "#" + aggregate.ID)},
			"SK":                 {S: aws.String("v" + strconv.FormatUint(event.ID, 10))},
			"EventID":            {N: aws.String(strconv.FormatUint(event.ID, 10))},
			"EventType":          {S: aws.String(event.Type)},
			"EventTime":          {N: aws.String(strconv.FormatInt(event.Time.UnixNano(), 10))},
			"EventAuthor":        {S: aws.String(event.Author)},
			"EventRequestID":     {S: aws.String(event.RequestID)},
			"EventPayload":       {M: eventPayload},
			"EventCorrelationID": {S: aws.String(event.CorrelationID)},
			"AggregateID":        {S: aws.String(aggregate.ID)},
			"AggregateType":      {S: aws.String(aggregate.Type)},
			"AggregateVersion":   {N: aws.String(strconv.FormatUint(aggregate.Version, 10))},
			"AggregatePayload":   {M: aggregatePayload},
			"AggregateUpdatedAt": {S: aws.String(strconv.FormatInt(aggregate.UpdatedAt.UnixNano(), 10))},
			"AggregateUpdatedBy": {S: aws.String(aggregate.UpdatedBy)},
			"AggregateCreatedAt": {S: aws.String(strconv.FormatInt(aggregate.CreatedAt.UnixNano(), 10))},
			"AggregateCreatedBy": {S: aws.String(aggregate.CreatedBy)},
		},
	}, nil
}

func (es EventStore) generateEventPutItemStatement(aggregate types.AggregateWithMetadata, event types.EventWithMetadata) (*dynamodb.PutItemInput, error) {

	aggregatePayload, err := dynamodbattribute.MarshalMap(aggregate.Payload)
	if err != nil {
		return nil, fmt.Errorf("cannot generate statement to save event aggregate(%s:%s:%d)\n%v", aggregate.Type, aggregate.ID, event.ID, err)
	}

	eventPayload, err := dynamodbattribute.MarshalMap(event.Payload)
	if err != nil {
		return nil, fmt.Errorf("cannot generate statement to save event(%s:%s:%d)\n%v", aggregate.Type, aggregate.ID, event.ID, err)
	}

	return &dynamodb.PutItemInput{
		TableName:           aws.String(es.DynamoDBTable),
		ConditionExpression: aws.String("attribute_not_exists(#PK) AND attribute_not_exists(#SK)"),
		ExpressionAttributeNames: map[string]*string{
			"#PK": aws.String("PK"),
			"#SK": aws.String("SK"),
		},
		Item: map[string]*dynamodb.AttributeValue{
			"PK":                 {S: aws.String(aggregate.Type + "#" + aggregate.ID)},
			"SK":                 {S: aws.String("v" + strconv.FormatUint(event.ID, 10))},
			"EventID":            {N: aws.String(strconv.FormatUint(event.ID, 10))},
			"EventType":          {S: aws.String(event.Type)},
			"EventTime":          {N: aws.String(strconv.FormatInt(event.Time.UnixNano(), 10))},
			"EventAuthor":        {S: aws.String(event.Author)},
			"EventRequestID":     {S: aws.String(event.RequestID)},
			"EventPayload":       {M: eventPayload},
			"EventCorrelationID": {S: aws.String(event.CorrelationID)},
			"AggregateID":        {S: aws.String(aggregate.ID)},
			"AggregateType":      {S: aws.String(aggregate.Type)},
			"AggregateVersion":   {N: aws.String(strconv.FormatUint(aggregate.Version, 10))},
			"AggregatePayload":   {M: aggregatePayload},
			"AggregateUpdatedAt": {S: aws.String(strconv.FormatInt(aggregate.UpdatedAt.UnixNano(), 10))},
			"AggregateUpdatedBy": {S: aws.String(aggregate.UpdatedBy)},
			"AggregateCreatedAt": {S: aws.String(strconv.FormatInt(aggregate.CreatedAt.UnixNano(), 10))},
			"AggregateCreatedBy": {S: aws.String(aggregate.CreatedBy)},
		},
	}, nil
}

func (es EventStore) generateLatestSnapshotPutStatement(aggregate types.AggregateWithMetadata, event types.EventWithMetadata) (*dynamodb.Put, error) {

	aggregatePayload, err := dynamodbattribute.MarshalMap(aggregate.Payload)
	if err != nil {
		return nil, fmt.Errorf("cannot generate statement to save aggregate(%s:%s:%d)\n%v", aggregate.Type, aggregate.ID, event.ID, err)
	}

	return &dynamodb.Put{
		TableName: aws.String(es.DynamoDBTable),
		Item: map[string]*dynamodb.AttributeValue{
			"PK":            {S: aws.String(aggregate.Type + "#" + aggregate.ID)},
			"SK":            {S: aws.String("latest")},
			"CorrelationID": {S: aws.String(event.CorrelationID)},
			"ID":            {S: aws.String(aggregate.ID)},
			"Type":          {S: aws.String(aggregate.Type)},
			"Version":       {N: aws.String(strconv.FormatUint(aggregate.Version, 10))},
			"Payload":       {M: aggregatePayload},
			"UpdatedAt":     {S: aws.String(strconv.FormatInt(aggregate.UpdatedAt.UnixNano(), 10))},
			"UpdatedBy":     {S: aws.String(aggregate.UpdatedBy)},
			"CreatedAt":     {S: aws.String(strconv.FormatInt(aggregate.CreatedAt.UnixNano(), 10))},
			"CreatedBy":     {S: aws.String(aggregate.CreatedBy)},
		},
	}, nil
}

func (es EventStore) saveEventIntoObjectStore(ctx context.Context, aggregate types.AggregateWithMetadata, event types.EventWithMetadata) error {

	objectID := aggregate.ID + "/" + strconv.FormatUint(event.ID, 10) + ".json"
	objectPayload := map[string]interface{}{"Event": event, "Aggregate": aggregate}

	err := es.ObjectStoreClient.Save(ctx, objectID, objectPayload)

	if err != nil {
		return fmt.Errorf("cannot save event object(%s:%s:%d)\n%v", aggregate.Type, aggregate.ID, event.ID, err)
	}

	return nil
}
