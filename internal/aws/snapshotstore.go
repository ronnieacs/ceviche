package aws

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/rxac/ceviche/config"
	"github.com/rxac/ceviche/service"
	"github.com/rxac/ceviche/types"
	"reflect"
	"strconv"
	"sync"
	"time"
)

type SnapshotStore struct {
	service.SnapshotStore
	AggregateType     reflect.Type
	ObjectStoreUse    config.ObjectStoreUse
	ObjectStoreClient ObjectStore
	DynamoDBTable     string
	DynamoDBIndex     string
	DynamoDBClient    dynamodbiface.DynamoDBAPI
}

func (ss SnapshotStore) List(ctx context.Context, continuationToken string) (types.AggregatePage, error) {
	if ss.DynamoDBIndex == "" {
		panic(fmt.Errorf("list operation not supported without index"))
	} else {
		if ss.ObjectStoreUse == config.ObjectStoreUseAlways {
			return ss.listWithIndexAndObjectStore(ctx, continuationToken)
		} else {
			return ss.listWithIndex(ctx, continuationToken)
		}
	}
}

func (ss SnapshotStore) listWithIndex(ctx context.Context, continuationToken string) (types.AggregatePage, error) {

	var startKey map[string]*dynamodb.AttributeValue
	if continuationToken == "" {
		startKey = map[string]*dynamodb.AttributeValue{
			"Type": {S: aws.String("Type")},
			"ID":   {S: aws.String(continuationToken)},
		}
	}

	input := &dynamodb.QueryInput{
		TableName:              aws.String(ss.DynamoDBTable),
		IndexName:              aws.String(ss.DynamoDBIndex),
		Limit:                  aws.Int64(50),
		ScanIndexForward:       aws.Bool(false),
		KeyConditionExpression: aws.String("#Type = :Type"),
		ExpressionAttributeNames: map[string]*string{
			"#Type": aws.String("Type"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":Type": {S: aws.String(ss.AggregateType.Name())},
		},
		ExclusiveStartKey: startKey,
	}

	output, err := ss.DynamoDBClient.QueryWithContext(ctx, input)
	if err != nil {
		return types.AggregatePage{}, fmt.Errorf("cannot list aggregates(%s:%s) from dynamodb table (%s)\n%v",
			ss.AggregateType.Name(), continuationToken, ss.DynamoDBTable, err)
	}

	result := make([]types.AggregateWithMetadata, *output.Count)
	for index, element := range output.Items {

		newPayload := reflect.New(ss.AggregateType).Interface()
		aggregate := types.AggregateWithMetadata{Payload: newPayload}

		err := dynamodbattribute.UnmarshalMap(element, aggregate)
		if err != nil {
			return types.AggregatePage{}, fmt.Errorf("cannot deserialize aggregate\n%v", err)
		}
		result[index] = aggregate
	}

	if output.LastEvaluatedKey != nil {
		continuationToken = aws.StringValue(output.LastEvaluatedKey["ID"].S)
	}

	return types.AggregatePage{
		ContinuationToken: continuationToken,
		Items:             result,
	}, nil
}

func (ss SnapshotStore) listWithIndexAndObjectStore(ctx context.Context, continuationToken string) (types.AggregatePage, error) {

	var startKey map[string]*dynamodb.AttributeValue
	if continuationToken == "" {
		startKey = map[string]*dynamodb.AttributeValue{
			"Type": {S: aws.String("Type")},
			"ID":   {S: aws.String(continuationToken)},
		}
	}
	input := &dynamodb.QueryInput{
		TableName:              aws.String(ss.DynamoDBTable),
		IndexName:              aws.String(ss.DynamoDBIndex),
		Limit:                  aws.Int64(50),
		ScanIndexForward:       aws.Bool(false),
		KeyConditionExpression: aws.String("#Type = :Type"),
		ExpressionAttributeNames: map[string]*string{
			"#Type": aws.String("Type"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":Type": {S: aws.String(ss.AggregateType.Name())},
		},
		ProjectionExpression: aws.String("ID,Version"),
		ExclusiveStartKey:    startKey,
	}

	output, err := ss.DynamoDBClient.QueryWithContext(ctx, input)
	if err != nil {
		return types.AggregatePage{}, fmt.Errorf("cannot list aggregates(%s:%s) from dynamodb table (%s)\n%v",
			ss.AggregateType.Name(), continuationToken, ss.DynamoDBTable, err)
	}

	var wg sync.WaitGroup
	inputChannel := make(chan ConcurrentObjectStoreResult)
	outputChannel := make(chan ConcurrentObjectStoreResults)

	go ss.collectAggregateFromObjectStore(inputChannel, outputChannel, &wg)

	for _, element := range output.Items {

		objectID := ss.AggregateType.Name() + "/" +
			aws.StringValue(element["ID"].S) + "/" +
			aws.StringValue(element["Version"].N) + ".json"

		wg.Add(1)
		go ss.concurrentGetAggregateFromObjectStore(ctx, objectID, inputChannel)
	}

	wg.Wait()
	close(inputChannel)
	results := <-outputChannel

	if len(results.Errs) > 0 {
		return types.AggregatePage{}, results.Errs[0]
	}

	if output.LastEvaluatedKey != nil {
		continuationToken = aws.StringValue(output.LastEvaluatedKey["ID"].S)
	}

	return types.AggregatePage{
		ContinuationToken: continuationToken,
		Items:             results.Aggregates,
	}, nil
}

func (ss SnapshotStore) Versions(ctx context.Context, id string) (map[uint64]time.Time, error) {

	aggregateTypeName := ss.AggregateType.Name()
	input := &dynamodb.QueryInput{
		TableName:              aws.String(ss.DynamoDBTable),
		AttributesToGet:        []*string{aws.String("EventID"), aws.String("EventTime")},
		ScanIndexForward:       aws.Bool(false),
		KeyConditionExpression: aws.String("#PK = :PK AND begins_with( #SK, :SKPrefix )"),
		ExpressionAttributeNames: map[string]*string{
			"#PK": aws.String("PK"),
			"#SK": aws.String("SK"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":PK":       {S: aws.String(aggregateTypeName + "#" + id)},
			":SKPrefix": {S: aws.String("v")},
		},
	}

	output, err := ss.DynamoDBClient.QueryWithContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("cannot get aggregate(%s:%s) versions\n%v", aggregateTypeName, id, err)
	}

	result := make(map[uint64]time.Time)
	for _, element := range output.Items {
		version, err := strconv.ParseUint(*element["EventID"].N, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot read versions for aggregate(%s:%s)\n%v", aggregateTypeName, id, err)
		}
		unixTime, err := strconv.ParseInt(*element["EventTime"].N, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot read versions for aggregate(%s:%s)\n%v", aggregateTypeName, id, err)
		}
		result[version] = time.Unix(0, unixTime)
	}

	return result, nil
}

func (ss SnapshotStore) Version(ctx context.Context, id string, version uint64) (types.AggregateWithMetadata, error) {
	if ss.ObjectStoreUse != config.ObjectStoreUseNone {
		return ss.versionWithObjectStorage(ctx, id, version)
	} else {
		return ss.versionWithoutObjectStorage(ctx, id, version)
	}
}

func (ss SnapshotStore) versionWithObjectStorage(ctx context.Context, id string, version uint64) (types.AggregateWithMetadata, error) {

	objectID := ss.AggregateType.Name() + "/" + id + "/" + strconv.FormatUint(version, 10) + ".json"

	aggregate, err := ss.getAggregateFromObjectStore(ctx, objectID)
	if err != nil {
		return types.AggregateWithMetadata{}, fmt.Errorf("Cannot get aggregate(%s:%s:%d) from Object Store\n%v", ss.AggregateType.Name(), id, version, err)
	}

	return aggregate, nil
}

func (ss SnapshotStore) versionWithoutObjectStorage(ctx context.Context, id string, version uint64) (types.AggregateWithMetadata, error) {

	input := &dynamodb.GetItemInput{
		TableName: aws.String(ss.DynamoDBTable),
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(ss.AggregateType.Name() + "#" + id)},
			"SK": {S: aws.String("v" + strconv.FormatUint(version, 10))},
		},
		ProjectionExpression: aws.String(
			"AggregateID," +
				"AggregateType," +
				"AggregateVersion," +
				"AggregatePayload," +
				"AggregateUpdatedAt," +
				"AggregateUpdatedBy," +
				"AggregateCreatedAt," +
				"AggregateCreatedBy"),
	}

	output, err := ss.DynamoDBClient.GetItemWithContext(ctx, input)
	if err != nil {
		return types.AggregateWithMetadata{}, fmt.Errorf("cannot get aggregate(%s:%s)\n%v", ss.AggregateType.Name(), id, err)
	}
	return ss.mapEventRecordToAggregate(output.Item), nil
}

func (ss SnapshotStore) Latest(ctx context.Context, id string) (types.AggregateWithMetadata, error) {
	if ss.ObjectStoreUse != config.ObjectStoreUseNone {
		return ss.latestWithObjectStore(ctx, id)
	} else {
		return ss.latestWithoutObjectStore(ctx, id)
	}
}

func (ss SnapshotStore) latestWithObjectStore(ctx context.Context, id string) (types.AggregateWithMetadata, error) {

	input := &dynamodb.QueryInput{
		TableName:              aws.String(ss.DynamoDBTable),
		ScanIndexForward:       aws.Bool(false),
		Limit:                  aws.Int64(1),
		KeyConditionExpression: aws.String("#PK = :PK AND begins_with( #SK, :SKPrefix )"),
		ExpressionAttributeNames: map[string]*string{
			"#PK": aws.String("PK"),
			"#SK": aws.String("SK"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":PK":       {S: aws.String(ss.AggregateType.Name() + "#" + id)},
			":SKPrefix": {S: aws.String("v")},
		},
		ProjectionExpression: aws.String(
			"AggregateID," +
				"AggregateType," +
				"AggregateVersion"),
	}

	output, err := ss.DynamoDBClient.QueryWithContext(ctx, input)
	if err != nil {
		return types.AggregateWithMetadata{}, fmt.Errorf("cannot get aggregate(%s:%s)\n%v", ss.AggregateType.Name(), id, err)
	}

	if *output.Count == 0 {
		return types.AggregateWithMetadata{}, fmt.Errorf("aggregate(%s:%s) not found", ss.AggregateType.Name(), id)
	}

	record := output.Items[0]
	objectID := aws.StringValue(record["AggregateType"].S) + "/" +
		aws.StringValue(record["AggregateID"].S) + "/" +
		aws.StringValue(record["AggregateVersion"].N) + ".json"

	return ss.getAggregateFromObjectStore(ctx, objectID)
}

func (ss SnapshotStore) latestWithoutObjectStore(ctx context.Context, id string) (types.AggregateWithMetadata, error) {

	input := &dynamodb.QueryInput{
		TableName:              aws.String(ss.DynamoDBTable),
		ScanIndexForward:       aws.Bool(false),
		Limit:                  aws.Int64(1),
		KeyConditionExpression: aws.String("#PK = :PK AND begins_with( #SK, :SKPrefix )"),
		ExpressionAttributeNames: map[string]*string{
			"#PK": aws.String("PK"),
			"#SK": aws.String("SK"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":PK":       {S: aws.String(ss.AggregateType.Name() + "#" + id)},
			":SKPrefix": {S: aws.String("v")},
		},
		ProjectionExpression: aws.String(
			"AggregateID," +
				"AggregateType," +
				"AggregateVersion," +
				"AggregatePayload," +
				"AggregateUpdatedAt," +
				"AggregateUpdatedBy," +
				"AggregateCreatedAt," +
				"AggregateCreatedBy"),
	}

	output, err := ss.DynamoDBClient.QueryWithContext(ctx, input)
	if err != nil {
		return types.AggregateWithMetadata{}, fmt.Errorf("cannot get aggregate(%s:%s)\n%v", ss.AggregateType.Name(), id, err)
	}

	if *output.Count == 0 {
		return types.AggregateWithMetadata{}, fmt.Errorf("aggregate(%s:%s) not found", ss.AggregateType.Name(), id)
	}

	return ss.mapEventRecordToAggregate(output.Items[0]), nil
}

func (ss SnapshotStore) mapEventRecordToAggregate(record map[string]*dynamodb.AttributeValue) types.AggregateWithMetadata {

	version, err := strconv.ParseUint(*record["AggregateVersion"].N, 10, 64)
	if err != nil {
		panic(fmt.Errorf("cannot deserialize aggregate version\n%v", err))
	}

	payload := reflect.New(ss.AggregateType).Interface()
	err = dynamodbattribute.UnmarshalMap(record["AggregatePayload"].M, payload)
	if err != nil {
		panic(fmt.Errorf("cannot deserialize aggregate payload\n%v", err))
	}

	updatedAtUnix, err := strconv.ParseInt(*record["AggregateUpdatedAt"].N, 10, 64)
	if err != nil {
		panic(fmt.Errorf("cannot deserialize aggregate UpdatedAt field\n%v", err))
	}

	createdAtUnix, err := strconv.ParseInt(*record["AggregateCreatedAt"].N, 10, 64)
	if err != nil {
		panic(fmt.Errorf("cannot deserialize aggregate CreatedAt field\n%v", err))
	}

	return types.AggregateWithMetadata{
		ID:        aws.StringValue(record["AggregateID"].S),
		Type:      aws.StringValue(record["AggregateType"].S),
		Version:   version,
		Payload:   payload,
		UpdatedAt: time.Unix(0, updatedAtUnix*int64(time.Millisecond)),
		UpdatedBy: aws.StringValue(record["AggregateUpdatedBy"].S),
		CreatedAt: time.Unix(0, createdAtUnix*int64(time.Millisecond)),
		CreatedBy: aws.StringValue(record["AggregateCreatedBy"].S),
	}
}

func (ss SnapshotStore) mapSnapshotRecordToAggregate(record map[string]*dynamodb.AttributeValue) types.AggregateWithMetadata {

	version, err := strconv.ParseUint(*record["Version"].N, 10, 64)
	if err != nil {
		panic(fmt.Errorf("cannot deserialize aggregate version\n%v", err))
	}

	payload := reflect.New(ss.AggregateType).Interface()
	err = dynamodbattribute.UnmarshalMap(record["Payload"].M, payload)
	if err != nil {
		panic(fmt.Errorf("cannot deserialize aggregate payload\n%v", err))
	}

	updatedAtUnix, err := strconv.ParseInt(*record["UpdatedAt"].N, 10, 64)
	if err != nil {
		panic(fmt.Errorf("cannot deserialize aggregate UpdatedAt field\n%v", err))
	}

	createdAtUnix, err := strconv.ParseInt(*record["CreatedAt"].N, 10, 64)
	if err != nil {
		panic(fmt.Errorf("cannot deserialize aggregate CreatedAt field\n%v", err))
	}

	return types.AggregateWithMetadata{
		ID:        aws.StringValue(record["ID"].S),
		Type:      aws.StringValue(record["Type"].S),
		Version:   version,
		Payload:   payload,
		UpdatedAt: time.Unix(0, updatedAtUnix*int64(time.Millisecond)),
		UpdatedBy: aws.StringValue(record["UpdatedBy"].S),
		CreatedAt: time.Unix(0, createdAtUnix*int64(time.Millisecond)),
		CreatedBy: aws.StringValue(record["CreatedBy"].S),
	}
}

func (ss SnapshotStore) getAggregateFromObjectStore(ctx context.Context, objectID string) (types.AggregateWithMetadata, error) {

	newPayload := reflect.New(ss.AggregateType).Interface()
	object := map[string]interface{}{"Aggregate": types.AggregateWithMetadata{Payload: newPayload}}

	err := ss.ObjectStoreClient.Load(ctx, objectID, object)
	if err != nil {
		return types.AggregateWithMetadata{}, err
	}

	return object["Aggregate"].(types.AggregateWithMetadata), nil
}

func (ss SnapshotStore) concurrentGetAggregateFromObjectStore(ctx context.Context, objectID string, input chan ConcurrentObjectStoreResult) {
	result, err := ss.getAggregateFromObjectStore(ctx, objectID)
	input <- ConcurrentObjectStoreResult{result, err}
}

func (ss SnapshotStore) collectAggregateFromObjectStore(input chan ConcurrentObjectStoreResult, output chan ConcurrentObjectStoreResults, wg *sync.WaitGroup) {
	var results ConcurrentObjectStoreResults
	for result := range input {
		if result.Err != nil {
			results.Errs = append(results.Errs, result.Err)
		} else {
			results.Aggregates = append(results.Aggregates, result.Aggregate)
		}
		wg.Done()
	}
	output <- results
}

type ConcurrentObjectStoreResult struct {
	Aggregate types.AggregateWithMetadata
	Err       error
}

type ConcurrentObjectStoreResults struct {
	Aggregates []types.AggregateWithMetadata
	Errs       []error
}
