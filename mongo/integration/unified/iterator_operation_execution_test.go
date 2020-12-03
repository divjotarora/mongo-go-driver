// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package unified

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/internal"
)

func executeClose(ctx context.Context, operation *Operation) error {
	iter, err := Entities(ctx).Iterator(operation.Object)
	if err != nil {
		return err
	}

	ctx, cancel := getCursorOperationContext(ctx, iter)
	if cancel != nil {
		defer cancel()
	}

	// Per the spec, we ignore all errors from Close.
	_ = iter.Close(ctx)
	return nil
}

func executeIterateUntilDocumentOrError(ctx context.Context, operation *Operation) (*OperationResult, error) {
	iter, err := Entities(ctx).Iterator(operation.Object)
	if err != nil {
		return nil, err
	}

	for {
		ctx, cancel := getCursorOperationContext(ctx, iter)
		if cancel != nil {
			defer cancel()
		}

		if iter.TryNext(ctx) {
			// Because we don't have access to the actual ChangeStream or Cursor object, we can't use the "Current"
			// struct field. Instead, we use the Decode method with a bson.Raw, which should do a simple byte copy.
			// We don't expect the server to return malformed documents, so any errors from Decode are treated as fatal.
			var res bson.Raw
			if err := iter.Decode(&res); err != nil {
				return nil, fmt.Errorf("error decoding cursor result: %v", err)
			}

			return NewDocumentResult(res, nil), nil
		}
		if iter.Err() != nil {
			return NewErrorResult(iter.Err()), nil
		}
	}
}

func executeIterateOnce(ctx context.Context, operation *Operation) (*OperationResult, error) {
	iter, err := Entities(ctx).Iterator(operation.Object)
	if err != nil {
		return nil, err
	}
	ctx, cancel := getCursorOperationContext(ctx, iter)
	if cancel != nil {
		defer cancel()
	}

	if iter.TryNext(ctx) {
		var res bson.Raw
		if err := iter.Decode(&res); err != nil {
			return nil, fmt.Errorf("error decoding cursor result: %v", err)
		}

		return NewDocumentResult(res, nil), nil
	}
	if iter.Err() != nil {
		return NewErrorResult(iter.Err()), nil
	}
	return NewEmptyResult(), nil
}

func getCursorOperationContext(ctx context.Context, iter *IteratorEntity) (context.Context, context.CancelFunc) {
	if iter.TimeoutMS == 0 {
		return ctx, nil
	}
	return internal.NewContextWithTimeout(ctx, time.Duration(iter.TimeoutMS)*time.Millisecond)
}
