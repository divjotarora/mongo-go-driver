package unified

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

type Iterator interface {
	Close(context.Context) error
	Decode(interface{}) error
	Err() error
	Next(context.Context) bool
	TryNext(context.Context) bool
}

type IteratorEntity struct {
	Iterator
	TimeoutMS int32
}

type AllResult struct {
	Documents []bson.Raw
	Err       error
}

func (ie *IteratorEntity) All(ctx context.Context) (*AllResult, error) {
	var docs []bson.Raw
	defer ie.Close(ctx)

	for ie.Next(ctx) {
		doc, err := ie.Current()
		if err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	return &AllResult{Documents: docs, Err: ie.Err()}, nil
}

func (ie *IteratorEntity) Current() (bson.Raw, error) {
	var doc bson.Raw
	if err := ie.Decode(&doc); err != nil {
		return nil, fmt.Errorf("error decoding iterator result: %v", err)
	}
	return doc, nil
}

func NewIteratorEntity(ctx context.Context, iter Iterator) *IteratorEntity {
	entity := &IteratorEntity{
		Iterator: iter,
	}
	if timeout, ok := TimeoutMS(ctx); ok {
		entity.TimeoutMS = timeout
	}
	return entity
}
