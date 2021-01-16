package aws

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/rxac/ceviche/service"
	"io/ioutil"
)

type ObjectStore struct {
	service.ObjectStore
	S3Bucket string
	S3Client s3iface.S3API
}

func (os ObjectStore) Load(ctx context.Context, objectID string, object interface{}) error {

	input := &s3.GetObjectInput{
		Bucket: aws.String(os.S3Bucket),
		Key:    aws.String(objectID),
	}

	output, err := os.S3Client.GetObjectWithContext(ctx, input)
	if err != nil {
		return fmt.Errorf("cannot get object(%s) from S3(%s)\n%v", os.S3Bucket, objectID, err)
	}

	buffer, err := ioutil.ReadAll(output.Body)
	err = json.Unmarshal(buffer, object)
	if err != nil {
		return fmt.Errorf("cannot deserialize object(%s) from S3(%s)\n%v", os.S3Bucket, objectID, err)
	}

	return nil
}

func (os ObjectStore) Save(ctx context.Context, objectID string, object interface{}) error {

	buffer, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("cannot serialize object(%s) for S3(%s)\n%v", os.S3Bucket, objectID, err)
	}

	reader := bytes.NewReader(buffer)
	input := s3.PutObjectInput{
		Bucket:      aws.String(os.S3Bucket),
		Key:         aws.String(objectID),
		ContentType: aws.String("application/json"),
		Body:        aws.ReadSeekCloser(reader),
	}

	_, err = os.S3Client.PutObjectWithContext(ctx, &input)
	if err != nil {
		return fmt.Errorf("cannot put object(%s) to S3(%s)\n%v", os.S3Bucket, objectID, err)
	}

	return nil
}
