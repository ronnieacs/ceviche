package ceviche

import (
	"context"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/rxac/ceviche/config"
	"github.com/rxac/ceviche/internal/aws"
)

func Start(cfg config.Config, handler interface{}) {
	ctx := context.Background()
	cfg = config.Complete(cfg)
	ctx = aws.ConfigureContext(ctx, cfg)
	lambda.StartWithContext(ctx, handler)
}
