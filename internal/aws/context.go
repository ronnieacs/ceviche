package aws

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/aws/aws-xray-sdk-go/xraylog"
	"github.com/rxac/ceviche/config"
	cevichecontext "github.com/rxac/ceviche/context"
	"github.com/rxac/ceviche/service"
	"os"
	"reflect"
)

type CevicheAWSContext struct {
	aggregateType reflect.Type
	eventStore    service.EventStore
	objectStore   service.ObjectStore
	snapshotStore service.SnapshotStore
}

func ConfigureContext(ctx context.Context, cfg config.Config) context.Context {

	awssession := newSession()
	s3client := newS3Client(awssession)
	dynamoclient := newDynamoDBClient(awssession)

	ctx = configureXRayIntoContext(ctx)

	objectStore := ObjectStore{
		S3Bucket: cfg.ObjectStoreID,
		S3Client: s3client,
	}

	eventStore := EventStore{
		MaintainLatestRecord: cfg.EnableSnpList,
		EnableObjectStore:    cfg.ObjectStoreUse != config.ObjectStoreUseNone,
		ObjectStoreClient:    objectStore,
		ObjectHeaderReader:   cfg.ObjectHeaderR,
		DynamoDBTable:        cfg.EventStoreID,
		DynamoDBClient:       dynamoclient,
	}

	snapshotStore := SnapshotStore{
		AggregateType:     cfg.AggregateType,
		ObjectStoreUse:    cfg.ObjectStoreUse,
		ObjectStoreClient: objectStore,
		DynamoDBTable:     cfg.EventStoreID,
		DynamoDBIndex:     cfg.EventStoreIdx,
		DynamoDBClient:    dynamoclient,
	}

	awscontext := &CevicheAWSContext{
		aggregateType: cfg.AggregateType,
		eventStore:    &eventStore,
		objectStore:   &objectStore,
		snapshotStore: &snapshotStore,
	}

	return context.WithValue(ctx, cevichecontext.CevicheContextKey, awscontext)
}

func (ctx CevicheAWSContext) AggregateType() reflect.Type {
	return ctx.aggregateType
}

func (ctx CevicheAWSContext) EventStore() service.EventStore {
	return ctx.eventStore
}

func (ctx CevicheAWSContext) ObjectStore() service.ObjectStore {
	return ctx.objectStore
}

func (ctx CevicheAWSContext) SnapshotStore() service.SnapshotStore {
	return ctx.snapshotStore
}

func newSession() *session.Session {
	region := os.Getenv("AWS_REGION")

	sess := session.Must(
		session.NewSessionWithOptions(
			session.Options{
				Config: aws.Config{
					Region:                  aws.String(region),
					S3ForcePathStyle:        aws.Bool(true),
					DisableParamValidation:  aws.Bool(true),
					DisableComputeChecksums: aws.Bool(true),
				},
				SharedConfigState: session.SharedConfigEnable,
			}))
	xray.AWSSession(sess)
	return sess
}

func newS3Client(sess *session.Session) s3iface.S3API {
	s3Client := s3.New(sess)
	return s3Client
}

func newDynamoDBClient(sess *session.Session) dynamodbiface.DynamoDBAPI {
	dynamoClient := dynamodb.New(sess)
	return dynamoClient
}

func configureXRayIntoContext(ctx context.Context) context.Context {
	xray.SetLogger(xraylog.NewDefaultLogger(os.Stderr, xraylog.LogLevelError))
	ctx, err := xray.ContextWithConfig(ctx, xray.Config{})
	if err != nil {
		panic(err)
	}
	return ctx
}
