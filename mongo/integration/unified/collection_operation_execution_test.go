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
	"go.mongodb.org/mongo-driver/bson/bsontype"
	testhelpers "go.mongodb.org/mongo-driver/internal/testutil/helpers"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// This file contains helpers to execute collection operations.

func executeAggregate(ctx context.Context, operation *Operation) (*OperationResult, error) {
	var aggregator interface {
		Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (*mongo.Cursor, error)
	}
	var err error

	aggregator, err = Entities(ctx).Collection(operation.Object)
	if err != nil {
		aggregator, err = Entities(ctx).Database(operation.Object)
	}
	if err != nil {
		return nil, fmt.Errorf("no database or collection entity found with ID %q", operation.Object)
	}

	var pipeline []interface{}
	opts := options.Aggregate()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "allowDiskUse":
			opts.SetAllowDiskUse(val.Boolean())
		case "batchSize":
			opts.SetBatchSize(val.Int32())
		case "bypassDocumentValidation":
			opts.SetBypassDocumentValidation(val.Boolean())
		case "collation":
			collation, err := createCollation(val.Document())
			if err != nil {
				return nil, fmt.Errorf("error creating collation: %v", err)
			}
			opts.SetCollation(collation)
		case "comment":
			opts.SetComment(val.StringValue())
		case "hint":
			hint, err := createHint(val)
			if err != nil {
				return nil, fmt.Errorf("error creating hint: %v", err)
			}
			opts.SetHint(hint)
		case "maxTimeMS":
			opts.SetMaxTime(time.Duration(val.Int32()) * time.Millisecond)
		case "maxAwaitTimeMS":
			opts.SetMaxAwaitTime(time.Duration(val.Int32()) * time.Millisecond)
		case "pipeline":
			pipeline = testhelpers.RawToInterfaceSlice(val.Array())
		default:
			return nil, fmt.Errorf("unrecognized aggregate option %q", key)
		}
	}
	if pipeline == nil {
		return nil, newMissingArgumentError("pipeline")
	}

	cursor, err := aggregator.Aggregate(ctx, pipeline, opts)
	if err != nil {
		return NewErrorResult(err), nil
	}
	defer cursor.Close(ctx)

	var docs []bson.Raw
	if err := cursor.All(ctx, &docs); err != nil {
		return NewErrorResult(err), nil
	}
	return NewCursorResult(docs), nil
}

func executeBulkWrite(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	var models []mongo.WriteModel
	opts := options.BulkWrite()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "ordered":
			opts.SetOrdered(val.Boolean())
		case "requests":
			models, err = createBulkWriteModels(val.Array())
			if err != nil {
				return nil, fmt.Errorf("error creating write models: %v", err)
			}
		default:
			return nil, fmt.Errorf("unrecognized bulkWrite option %q", key)
		}
	}
	if models == nil {
		return nil, newMissingArgumentError("requests")
	}

	// Preserve the error from BulkWrite in a separate variable so "err" can be used elsewhere.
	res, bulkWriteErr := coll.BulkWrite(ctx, models, opts)
	raw := emptyCoreDocument
	if res != nil {
		rawUpsertedIDs := emptyDocument
		if res.UpsertedIDs != nil {
			rawUpsertedIDs, err = bson.Marshal(res.UpsertedIDs)
			if err != nil {
				return nil, fmt.Errorf("error marshalling UpsertedIDs map to BSON: %v", err)
			}
		}

		raw = bsoncore.NewDocumentBuilder().
			AppendInt64("insertedCount", res.InsertedCount).
			AppendInt64("deletedCount", res.DeletedCount).
			AppendInt64("matchedCount", res.MatchedCount).
			AppendInt64("modifiedCount", res.ModifiedCount).
			AppendInt64("upsertedCount", res.UpsertedCount).
			AppendDocument("upsertedIds", rawUpsertedIDs).
			Build()
	}
	return NewDocumentResult(raw, bulkWriteErr), nil
}

func executeCountDocuments(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	var filter bson.Raw
	opts := options.Count()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "collation":
			collation, err := createCollation(val.Document())
			if err != nil {
				return nil, fmt.Errorf("error creating collation: %v", err)
			}
			opts.SetCollation(collation)
		case "filter":
			filter = bson.Raw(val.Document())
		case "hint":
			hint, err := createHint(val)
			if err != nil {
				return nil, fmt.Errorf("error creating hint: %v", err)
			}
			opts.SetHint(hint)
		case "limit":
			opts.SetLimit(int64(val.Int64()))
		case "maxTimeMS":
			opts.SetMaxTime(time.Duration(val.Int32()) * time.Millisecond)
		case "skip":
			opts.SetSkip(int64(val.Int32()))
		default:
			return nil, fmt.Errorf("unrecognized countDocuments option %q", key)
		}
	}
	if filter == nil {
		return nil, newMissingArgumentError("filter")
	}

	count, err := coll.CountDocuments(ctx, filter, opts)
	if err != nil {
		return NewErrorResult(err), nil
	}
	return NewValueResult(bsontype.Int64, bsoncore.AppendInt64(nil, count), nil), nil
}

func executeCreateIndex(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	var keys bson.Raw
	indexOpts := options.Index()       // options to configure the created index
	cmdOpts := options.CreateIndexes() // options to configure the command

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		// Index options
		case "2dsphereIndexVersion":
			indexOpts.SetSphereVersion(val.Int32())
		case "background":
			indexOpts.SetBackground(val.Boolean())
		case "bits":
			indexOpts.SetBits(val.Int32())
		case "bucketSize":
			indexOpts.SetBucketSize(val.Int32())
		case "collation":
			collation, err := createCollation(val.Document())
			if err != nil {
				return nil, fmt.Errorf("error creating collation: %v", err)
			}
			indexOpts.SetCollation(collation)
		case "defaultLanguage":
			indexOpts.SetDefaultLanguage(val.StringValue())
		case "expireAfterSeconds":
			indexOpts.SetExpireAfterSeconds(val.Int32())
		case "hidden":
			indexOpts.SetHidden(val.Boolean())
		case "keys":
			keys = val.Document()
		case "languageOverride":
			indexOpts.SetLanguageOverride(val.StringValue())
		case "max":
			indexOpts.SetMax(val.Double())
		case "min":
			indexOpts.SetMin(val.Double())
		case "name":
			indexOpts.SetName(val.StringValue())
		case "partialFilterExpression":
			indexOpts.SetPartialFilterExpression(val.Document())
		case "sparse":
			indexOpts.SetSparse(val.Boolean())
		case "storageEngine":
			indexOpts.SetStorageEngine(val.Document())
		case "unique":
			indexOpts.SetUnique(val.Boolean())
		case "version":
			indexOpts.SetVersion(val.Int32())
		case "textIndexVersion":
			indexOpts.SetTextVersion(val.Int32())
		case "weights":
			indexOpts.SetWeights(val.Document())
		case "wildcardProjection":
			indexOpts.SetWildcardProjection(val.Document())

		// createIndexes command options
		case "maxTimeMS":
			cmdOpts.SetMaxTime(time.Duration(val.Int32()) * time.Millisecond)

		default:
			return nil, fmt.Errorf("unrecognized createIndex option %q", key)
		}
	}
	if keys == nil {
		return nil, newMissingArgumentError("keys")
	}

	model := mongo.IndexModel{
		Keys:    keys,
		Options: indexOpts,
	}
	name, err := coll.Indexes().CreateOne(ctx, model, cmdOpts)
	return NewValueResult(bsontype.String, bsoncore.AppendString(nil, name), err), nil
}

func executeDeleteOne(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	var filter bson.Raw
	opts := options.Delete()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "collation":
			collation, err := createCollation(val.Document())
			if err != nil {
				return nil, fmt.Errorf("error creating collation: %v", err)
			}
			opts.SetCollation(collation)
		case "filter":
			filter = val.Document()
		case "hint":
			hint, err := createHint(val)
			if err != nil {
				return nil, fmt.Errorf("error creating hint: %v", err)
			}
			opts.SetHint(hint)
		default:
			return nil, fmt.Errorf("unrecognized deleteOne option %q", key)
		}
	}
	if filter == nil {
		return nil, newMissingArgumentError("filter")
	}

	res, err := coll.DeleteOne(ctx, filter, opts)
	raw := emptyCoreDocument
	if res != nil {
		raw = bsoncore.NewDocumentBuilder().
			AppendInt64("deletedCount", res.DeletedCount).
			Build()
	}
	return NewDocumentResult(raw, err), nil
}

func executeDeleteMany(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	var filter bson.Raw
	opts := options.Delete()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "collation":
			collation, err := createCollation(val.Document())
			if err != nil {
				return nil, fmt.Errorf("error creating collation: %v", err)
			}
			opts.SetCollation(collation)
		case "filter":
			filter = val.Document()
		case "hint":
			hint, err := createHint(val)
			if err != nil {
				return nil, fmt.Errorf("error creating hint: %v", err)
			}
			opts.SetHint(hint)
		default:
			return nil, fmt.Errorf("unrecognized deleteMany option %q", key)
		}
	}
	if filter == nil {
		return nil, newMissingArgumentError("filter")
	}

	res, err := coll.DeleteMany(ctx, filter, opts)
	raw := emptyCoreDocument
	if res != nil {
		raw = bsoncore.NewDocumentBuilder().
			AppendInt64("deletedCount", res.DeletedCount).
			Build()
	}
	return NewDocumentResult(raw, err), nil
}

func executeDistinct(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	var fieldName string
	var filter bson.Raw
	opts := options.Distinct()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "collation":
			collation, err := createCollation(val.Document())
			if err != nil {
				return nil, fmt.Errorf("error creating collation: %v", err)
			}
			opts.SetCollation(collation)
		case "fieldName":
			fieldName = val.StringValue()
		case "filter":
			filter = val.Document()
		case "maxTimeMS":
			opts.SetMaxTime(time.Duration(val.Int32()) * time.Millisecond)
		default:
			return nil, fmt.Errorf("unrecognized distinct option %q", key)
		}
	}
	if fieldName == "" {
		return nil, newMissingArgumentError("fieldName")
	}
	if filter == nil {
		return nil, newMissingArgumentError("filter")
	}

	res, err := coll.Distinct(ctx, fieldName, filter, opts)
	if err != nil {
		return NewErrorResult(err), nil
	}
	_, rawRes, err := bson.MarshalValue(res)
	if err != nil {
		return nil, fmt.Errorf("error converting Distinct result to raw BSON: %v", err)
	}
	return NewValueResult(bsontype.Array, rawRes, nil), nil
}

func executeDropIndex(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	args, err := createDropIndexesArguments(operation.Arguments, true)
	if err != nil {
		return nil, err
	}
	res, err := coll.Indexes().DropOne(ctx, args.name, args.opts)
	return NewDocumentResult(res, err), nil
}

func executeDropIndexes(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	args, err := createDropIndexesArguments(operation.Arguments, false)
	if err != nil {
		return nil, err
	}
	res, err := coll.Indexes().DropAll(ctx, args.opts)
	return NewDocumentResult(res, err), nil
}

func executeEstimatedDocumentCount(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	opts := options.EstimatedDocumentCount()
	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "maxTimeMS":
			opts.SetMaxTime(time.Duration(val.Int32()) * time.Millisecond)
		default:
			return nil, fmt.Errorf("unrecognized estimatedDocumentCount option %q", key)
		}
	}

	count, err := coll.EstimatedDocumentCount(ctx, opts)
	if err != nil {
		return NewErrorResult(err), nil
	}
	return NewValueResult(bsontype.Int64, bsoncore.AppendInt64(nil, count), nil), nil
}

func executeCreateFindCursor(ctx context.Context, operation *Operation) (*OperationResult, error) {
	result, err := createFindCursor(ctx, operation)
	if err != nil {
		return nil, err
	}
	if result.err != nil {
		return NewErrorResult(result.err), nil
	}

	if operation.ResultEntityID == nil {
		return nil, fmt.Errorf("no entity name provided to store executeChangeStream result")
	}
	if err := Entities(ctx).AddIteratorEntity(*operation.ResultEntityID, result.cursor); err != nil {
		return nil, fmt.Errorf("error storing result as cursor entity: %v", err)
	}
	return NewEmptyResult(), nil
}

func executeCollectionFind(ctx context.Context, operation *Operation) (*OperationResult, error) {
	timeoutMode := cursorLifetime
	if mode, err := operation.Arguments.LookupErr("timeoutMode"); err == nil {
		if mode.StringValue() == "iteration" {
			timeoutMode = iteration
		}
		operation.Arguments = RemoveFieldsFromDocument(operation.Arguments, "timeoutMode")
	}

	// TODO: handle timeoutMode=cursorLifetime + timeoutMS inherited from collection

	result, err := createFindCursor(ctx, operation)
	if err != nil {
		return nil, err
	}
	if result.err != nil {
		return NewErrorResult(result.err), nil
	}

	defer result.cursor.Close(ctx)

	switch timeoutMode {
	case cursorLifetime:
		// Mimic timeoutMode=cursorLifetime by using All(). This will automatically handle both the inheritance and
		// override use cases.
		res, err := result.cursor.All(ctx)
		if err != nil {
			return nil, fmt.Errorf("cursor iteration error: %v", err)
		}
		if res.Err != nil {
			return NewErrorResult(res.Err), nil
		}
		return NewCursorResult(res.Documents), nil
	default:
		// If timeoutMode=iteration, the timeout should be refreshed for each Next() call.

		var docs []bson.Raw
		for {
			ctx, cancel := getCursorOperationContext(ctx, result.cursor)
			if cancel != nil {
				defer cancel()
			}

			if result.cursor.Next(ctx) {
				doc, err := result.cursor.Current()
				if err != nil {
					return nil, err
				}
				docs = append(docs, doc)
				continue
			}
			break
		}
		if err := result.cursor.Err(); err != nil {
			return NewErrorResult(err), nil
		}
		return NewCursorResult(docs), nil
	}
}

func executeFindOne(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	var filter bson.Raw
	opts := options.FindOne()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "filter":
			filter = val.Document()
		case "maxTimeMS":
			opts.SetMaxTime(time.Duration(val.Int32()) * time.Millisecond)
		default:
			return nil, fmt.Errorf("unrecognized findOne option %q", key)
		}
	}
	if filter == nil {
		return nil, newMissingArgumentError("filter")
	}

	res, err := coll.FindOne(ctx, filter, opts).DecodeBytes()
	if err == mongo.ErrNoDocuments {
		err = nil
	}
	return NewDocumentResult(res, err), nil
}

type cursorTimeoutMode int

const (
	cursorLifetime cursorTimeoutMode = iota
	iteration
)

type cursorResult struct {
	cursor *IteratorEntity
	err    error
}

// createFindCursor is a helper used to execute a "find" command and return the created cursor. Errors encountered
// during operation setup will result in a (nil, err) return. Errors encountered during operation execution will be
// returned as part of the cursorResult.
func createFindCursor(ctx context.Context, operation *Operation) (*cursorResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	var filter bson.Raw
	opts := options.Find()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "allowDiskUse":
			opts.SetAllowDiskUse(val.Boolean())
		case "allowPartialResults":
			opts.SetAllowPartialResults(val.Boolean())
		case "batchSize":
			opts.SetBatchSize(val.Int32())
		case "collation":
			collation, err := createCollation(val.Document())
			if err != nil {
				return nil, fmt.Errorf("error creating collation: %v", err)
			}
			opts.SetCollation(collation)
		case "comment":
			opts.SetComment(val.StringValue())
		case "cursorType":
			var cursorType options.CursorType
			switch typeStr := val.StringValue(); typeStr {
			case "tailable":
				cursorType = options.Tailable
			case "tailableAwait":
				cursorType = options.TailableAwait
			default:
				return nil, fmt.Errorf("unrecognized cursorType value %q", typeStr)
			}
			opts.SetCursorType(cursorType)
		case "filter":
			filter = val.Document()
		case "hint":
			hint, err := createHint(val)
			if err != nil {
				return nil, fmt.Errorf("error creating hint: %v", err)
			}
			opts.SetHint(hint)
		case "limit":
			opts.SetLimit(int64(val.Int32()))
		case "max":
			opts.SetMax(val.Document())
		case "maxAwaitTimeMS":
			opts.SetMaxAwaitTime(time.Duration(val.Int32()) * time.Millisecond)
		case "maxTimeMS":
			opts.SetMaxTime(time.Duration(val.Int32()) * time.Millisecond)
		case "min":
			opts.SetMin(val.Document())
		case "noCursorTimeout":
			opts.SetNoCursorTimeout(val.Boolean())
		case "oplogReplay":
			opts.SetOplogReplay(val.Boolean())
		case "projection":
			opts.SetProjection(val.Document())
		case "returnKey":
			opts.SetReturnKey(val.Boolean())
		case "showRecordId":
			opts.SetShowRecordID(val.Boolean())
		case "skip":
			opts.SetSkip(int64(val.Int32()))
		case "snapshot":
			opts.SetSnapshot(val.Boolean())
		case "sort":
			opts.SetSort(val.Document())
		default:
			return nil, fmt.Errorf("unrecognized find option %q", key)
		}
	}
	if filter == nil {
		return nil, newMissingArgumentError("filter")
	}

	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return &cursorResult{err: err}, nil
	}
	return &cursorResult{cursor: NewIteratorEntity(ctx, cursor)}, nil
}

func executeFindOneAndDelete(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	var filter bson.Raw
	opts := options.FindOneAndDelete()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "filter":
			filter = val.Document()
		case "maxTimeMS":
			opts.SetMaxTime(time.Duration(val.Int32()) * time.Millisecond)
		default:
			return nil, fmt.Errorf("unrecognized findOneAndDelete option %q", key)
		}
	}
	if filter == nil {
		return nil, newMissingArgumentError("filter")
	}

	res, err := coll.FindOneAndDelete(ctx, filter, opts).DecodeBytes()
	if err == mongo.ErrNoDocuments {
		err = nil
	}
	return NewDocumentResult(res, err), nil
}

func executeFindOneAndReplace(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	var filter, replacement bson.Raw
	opts := options.FindOneAndReplace()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "filter":
			filter = val.Document()
		case "maxTimeMS":
			opts.SetMaxTime(time.Duration(val.Int32()) * time.Millisecond)
		case "replacement":
			replacement = val.Document()
		default:
			return nil, fmt.Errorf("unrecognized findOneAndReplace option %q", key)
		}
	}
	if filter == nil {
		return nil, newMissingArgumentError("filter")
	}
	if replacement == nil {
		return nil, newMissingArgumentError("replacement")
	}

	res, err := coll.FindOneAndReplace(ctx, filter, replacement, opts).DecodeBytes()
	if err == mongo.ErrNoDocuments {
		err = nil
	}
	return NewDocumentResult(res, err), nil
}

func executeFindOneAndUpdate(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	var filter bson.Raw
	var update interface{}
	opts := options.FindOneAndUpdate()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "arrayFilters":
			opts.SetArrayFilters(options.ArrayFilters{
				Filters: testhelpers.RawToInterfaceSlice(val.Array()),
			})
		case "bypassDocumentValidation":
			opts.SetBypassDocumentValidation(val.Boolean())
		case "collation":
			collation, err := createCollation(val.Document())
			if err != nil {
				return nil, fmt.Errorf("error creating collation: %v", err)
			}
			opts.SetCollation(collation)
		case "filter":
			filter = val.Document()
		case "hint":
			hint, err := createHint(val)
			if err != nil {
				return nil, fmt.Errorf("error creating hint: %v", err)
			}
			opts.SetHint(hint)
		case "maxTimeMS":
			opts.SetMaxTime(time.Duration(val.Int32()) * time.Millisecond)
		case "projection":
			opts.SetProjection(val.Document())
		case "returnDocument":
			switch rd := val.StringValue(); rd {
			case "After":
				opts.SetReturnDocument(options.After)
			case "Before":
				opts.SetReturnDocument(options.Before)
			default:
				return nil, fmt.Errorf("unrecognized returnDocument value %q", rd)
			}
		case "sort":
			opts.SetSort(val.Document())
		case "update":
			update, err = createUpdateValue(val)
			if err != nil {
				return nil, fmt.Errorf("error processing update value: %q", err)
			}
		case "upsert":
			opts.SetUpsert(val.Boolean())
		default:
			return nil, fmt.Errorf("unrecognized findOneAndUpdate option %q", key)
		}
	}
	if filter == nil {
		return nil, newMissingArgumentError("filter")
	}
	if update == nil {
		return nil, newMissingArgumentError("update")
	}

	res, err := coll.FindOneAndUpdate(ctx, filter, update, opts).DecodeBytes()
	if err == mongo.ErrNoDocuments {
		err = nil
	}
	return NewDocumentResult(res, err), nil
}

func executeInsertMany(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	var documents []interface{}
	opts := options.InsertMany()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "documents":
			documents = testhelpers.RawToInterfaceSlice(val.Array())
		case "ordered":
			opts.SetOrdered(val.Boolean())
		default:
			return nil, fmt.Errorf("unrecognized insertMany option %q", key)
		}
	}
	if documents == nil {
		return nil, newMissingArgumentError("documents")
	}

	res, err := coll.InsertMany(ctx, documents, opts)
	raw := emptyCoreDocument
	if res != nil {
		// We return InsertedIDs as []interface{} but the CRUD spec documents it as a map[int64]interface{}, so
		// comparisons will fail if we include it in the result document. This is marked as an optional field and is
		// always surrounded in an $$unsetOrMatches assertion, so we leave it out of the document.
		raw = bsoncore.NewDocumentBuilder().
			AppendInt32("insertedCount", int32(len(res.InsertedIDs))).
			AppendInt32("deletedCount", 0).
			AppendInt32("matchedCount", 0).
			AppendInt32("modifiedCount", 0).
			AppendInt32("upsertedCount", 0).
			AppendDocument("upsertedIds", bsoncore.NewDocumentBuilder().Build()).
			Build()
	}
	return NewDocumentResult(raw, err), nil
}

func executeInsertOne(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	var document bson.Raw
	opts := options.InsertOne()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "document":
			document = val.Document()
		case "bypassDocumentValidation":
			opts.SetBypassDocumentValidation(val.Boolean())
		default:
			return nil, fmt.Errorf("unrecognized insertOne option %q", key)
		}
	}
	if document == nil {
		return nil, newMissingArgumentError("documents")
	}

	res, err := coll.InsertOne(ctx, document, opts)
	raw := emptyCoreDocument
	if res != nil {
		t, data, err := bson.MarshalValue(res.InsertedID)
		if err != nil {
			return nil, fmt.Errorf("error convertined InsertedID field to BSON: %v", err)
		}
		raw = bsoncore.NewDocumentBuilder().
			AppendValue("insertedId", bsoncore.Value{Type: t, Data: data}).
			Build()
	}
	return NewDocumentResult(raw, err), nil
}

func executeListIndexes(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	opts := options.ListIndexes()
	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "maxTimeMS":
			opts.SetMaxTime(time.Duration(val.Int32()) * time.Millisecond)
		default:
			return nil, fmt.Errorf("unrecognized listIndexes option %q", key)
		}
	}

	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return NewErrorResult(err), nil
	}

	defer cursor.Close(ctx)
	var docs []bson.Raw
	if err := cursor.All(ctx, &docs); err != nil {
		return NewErrorResult(err), nil
	}

	return NewCursorResult(docs), nil
}

func executeReplaceOne(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	filter := emptyDocument
	replacement := emptyDocument
	opts := options.Replace()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "bypassDocumentValidation":
			opts.SetBypassDocumentValidation(val.Boolean())
		case "collation":
			collation, err := createCollation(val.Document())
			if err != nil {
				return nil, fmt.Errorf("error creating collation: %v", err)
			}
			opts.SetCollation(collation)
		case "filter":
			filter = val.Document()
		case "hint":
			hint, err := createHint(val)
			if err != nil {
				return nil, fmt.Errorf("error creating hint: %v", err)
			}
			opts.SetHint(hint)
		case "replacement":
			replacement = val.Document()
		case "upsert":
			opts.SetUpsert(val.Boolean())
		default:
			return nil, fmt.Errorf("unrecognized replaceOne option %q", key)
		}
	}

	res, err := coll.ReplaceOne(ctx, filter, replacement, opts)
	raw, buildErr := buildUpdateResultDocument(res)
	if buildErr != nil {
		return nil, buildErr
	}
	return NewDocumentResult(raw, err), nil
}

func executeUpdateOne(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	updateArgs, err := createUpdateArguments(operation.Arguments)
	if err != nil {
		return nil, err
	}

	res, err := coll.UpdateOne(ctx, updateArgs.filter, updateArgs.update, updateArgs.opts)
	raw, buildErr := buildUpdateResultDocument(res)
	if buildErr != nil {
		return nil, buildErr
	}
	return NewDocumentResult(raw, err), nil
}

func executeUpdateMany(ctx context.Context, operation *Operation) (*OperationResult, error) {
	coll, err := Entities(ctx).Collection(operation.Object)
	if err != nil {
		return nil, err
	}

	updateArgs, err := createUpdateArguments(operation.Arguments)
	if err != nil {
		return nil, err
	}

	res, err := coll.UpdateMany(ctx, updateArgs.filter, updateArgs.update, updateArgs.opts)
	raw, buildErr := buildUpdateResultDocument(res)
	if buildErr != nil {
		return nil, buildErr
	}
	return NewDocumentResult(raw, err), nil
}

func buildUpdateResultDocument(res *mongo.UpdateResult) (bsoncore.Document, error) {
	if res == nil {
		return emptyCoreDocument, nil
	}

	builder := bsoncore.NewDocumentBuilder().
		AppendInt64("matchedCount", res.MatchedCount).
		AppendInt64("modifiedCount", res.ModifiedCount).
		AppendInt64("upsertedCount", res.UpsertedCount)

	if res.UpsertedID != nil {
		t, data, err := bson.MarshalValue(res.UpsertedID)
		if err != nil {
			return nil, fmt.Errorf("error converting UpsertedID to BSON: %v", err)
		}
		builder.AppendValue("upsertedId", bsoncore.Value{Type: t, Data: data})
	}
	return builder.Build(), nil
}
