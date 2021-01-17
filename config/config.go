package config

import (
	"os"
	"reflect"
)

type Config struct {
	AggregateType  reflect.Type
	EventStoreID   string
	EventStoreIdx  string
	ObjectStoreID  string
	ObjectStoreUse ObjectStoreUse
	ObjectHeaderR  func(interface{}) interface{}
	EnableSnpList  bool
	DisableSSL     bool
}

func Complete(cfg Config) Config {

	if cfg.AggregateType == nil {
		panic("ceviche aggregate type required")
	}

	if cfg.EventStoreID == "" {
		cfg.EventStoreID = os.Getenv("CEVICHE_EVENT_STORE")
	}

	if cfg.EventStoreIdx == "" {
		customIndexName := os.Getenv("CEVICHE_EVENT_STORE_IDX")
		if customIndexName == "" {
			cfg.EventStoreIdx = "AggregatesByType"
		} else {
			cfg.EventStoreIdx = customIndexName
		}
	}

	if cfg.ObjectStoreID == "" {
		cfg.ObjectStoreID = os.Getenv("CEVICHE_OBJECT_STORE")
	}

	if cfg.ObjectStoreUse == "" {
		cfg.ObjectStoreUse = ObjectStoreUseStandard
	}

	if cfg.ObjectHeaderR == nil {
		cfg.ObjectHeaderR = defaultObjectHeaderReader
	}

	return cfg
}

func defaultObjectHeaderReader(interface{}) interface{} {
	return nil
}
