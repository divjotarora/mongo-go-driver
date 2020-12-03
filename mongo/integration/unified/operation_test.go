// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package unified

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Operation struct {
	Name           string         `bson:"name"`
	Object         string         `bson:"object"`
	Arguments      bson.Raw       `bson:"arguments"`
	ExpectedError  *ExpectedError `bson:"expectError"`
	ExpectedResult *bson.RawValue `bson:"expectResult"`
	ResultEntityID *string        `bson:"saveResultAsEntity"`
}

// Execute runs the operation and verifies the returned result and/or error. If the result needs to be saved as
// an entity, it also updates the EntityMap associated with ctx to do so. If the test needs to be skipped (e.g it
// attempts to run an unsupported operation), a SkipTestError will be returned. All other errors should be considered
// fatal.
func (op *Operation) Execute(ctx context.Context) error {
	res, err := op.run(ctx)
	if err != nil {
		return fmt.Errorf("execution failed: %v", err)
	}

	if err := VerifyOperationError(ctx, op.ExpectedError, res); err != nil {
		return fmt.Errorf("error verification failed: %v", err)
	}

	if op.ExpectedResult != nil {
		if err := VerifyOperationResult(ctx, *op.ExpectedResult, res); err != nil {
			return fmt.Errorf("result verification failed: %v", err)
		}
	}
	return nil
}

func (op *Operation) run(ctx context.Context) (*OperationResult, error) {
	if op.Object == "testRunner" {
		// testRunner operations don't have results or expected errors, so we use NewEmptyResult to fake a result.
		return NewEmptyResult(), executeTestRunnerOperation(ctx, op)
	}

	// Special handling for options that apply to multiple operations.
	if id, ok := op.Arguments.Lookup("session").StringValueOK(); ok {
		sess, err := Entities(ctx).Session(id)
		if err != nil {
			return nil, err
		}
		ctx = mongo.NewSessionContext(ctx, sess)
	}
	if timeout, ok := op.Arguments.Lookup("timeoutMS").Int32OK(); ok {
		switch {
		case timeout == 0:
			cleanup, err := cloneEntityWithZeroTimeout(ctx, op.Object)
			if err != nil {
				return nil, fmt.Errorf("error cloning entity with timeout=0: %v", err)
			}
			defer cleanup(ctx)
		default:
			var cancel context.CancelFunc
			ctx, cancel = WithTimeoutMS(ctx, timeout)
			defer cancel() // TODO we might not always want to cancel
		}
	}

	// Remove fields from the arguments document so individual operations do not have to account for them.
	op.Arguments = RemoveFieldsFromDocument(op.Arguments, "session", "timeoutMS")

	switch op.Name {
	// Session operations
	case "abortTransaction":
		return executeAbortTransaction(ctx, op)
	case "commitTransaction":
		return executeCommitTransaction(ctx, op)
	case "endSession":
		// The EndSession() method doesn't return a result, so we return a non-nil empty result.
		return NewEmptyResult(), executeEndSession(ctx, op)
	case "startTransaction":
		return executeStartTransaction(ctx, op)
	case "withTransaction":
		return executeWithTransaction(ctx, op)

	// Client operations
	case "createChangeStream":
		return executeCreateChangeStream(ctx, op)
	case "listDatabases":
		return executeListDatabases(ctx, op)
	case "listDatabaseNames":
		return executeListDatabaseNames(ctx, op)

	// Database operations
	case "createCollection":
		return executeCreateCollection(ctx, op)
	case "dropCollection":
		return executeDropCollection(ctx, op)
	case "listCollections":
		return executeListCollections(ctx, op)
	case "listCollectionNames":
		return executeListCollectionNames(ctx, op)
	case "runCommand":
		return executeRunCommand(ctx, op)

	// Collection operations
	case "aggregate":
		return executeAggregate(ctx, op)
	case "bulkWrite":
		return executeBulkWrite(ctx, op)
	case "count":
		return nil, NewSkipTestError("count is not supported")
	case "countDocuments":
		return executeCountDocuments(ctx, op)
	case "createIndex":
		return executeCreateIndex(ctx, op)
	case "createFindCursor":
		return executeCreateFindCursor(ctx, op)
	case "deleteOne":
		return executeDeleteOne(ctx, op)
	case "deleteMany":
		return executeDeleteMany(ctx, op)
	case "distinct":
		return executeDistinct(ctx, op)
	case "dropIndex":
		return executeDropIndex(ctx, op)
	case "dropIndexes":
		return executeDropIndexes(ctx, op)
	case "estimatedDocumentCount":
		return executeEstimatedDocumentCount(ctx, op)
	case "findOne":
		return executeFindOne(ctx, op)
	case "findOneAndDelete":
		return executeFindOneAndDelete(ctx, op)
	case "findOneAndReplace":
		return executeFindOneAndReplace(ctx, op)
	case "findOneAndUpdate":
		return executeFindOneAndUpdate(ctx, op)
	case "insertMany":
		return executeInsertMany(ctx, op)
	case "insertOne":
		return executeInsertOne(ctx, op)
	case "listIndexes":
		return executeListIndexes(ctx, op)
	case "listIndexNames":
		return nil, NewSkipTestError("listIndexNames is unsupported")
	case "replaceOne":
		return executeReplaceOne(ctx, op)
	case "updateOne":
		return executeUpdateOne(ctx, op)
	case "updateMany":
		return executeUpdateMany(ctx, op)

	// GridFS operations
	case "delete":
		return executeBucketDelete(ctx, op)
	case "download":
		return executeBucketDownload(ctx, op)
	case "drop":
		return executeBucketDrop(ctx, op)
	case "rename":
		return executeBucketRename(ctx, op)
	case "upload":
		return executeBucketUpload(ctx, op)

	// Cursor operations
	case "close":
		return NewEmptyResult(), executeClose(ctx, op)
	case "iterateOnce":
		return executeIterateOnce(ctx, op)
	case "iterateUntilDocumentOrError":
		return executeIterateUntilDocumentOrError(ctx, op)

	// Operations that are shared across multiple entity types.
	case "find":
		if _, err := Entities(ctx).Collection(op.Object); err == nil {
			return executeCollectionFind(ctx, op)
		}
		if _, err := Entities(ctx).GridFSBucket(op.Object); err == nil {
			return executeBucketFind(ctx, op)
		}
		return nil, fmt.Errorf("no collection or bucket entity found with ID %q", op.Object)
	default:
		return nil, fmt.Errorf("unrecognized entity operation %q", op.Name)
	}
}

func cloneEntityWithZeroTimeout(ctx context.Context, id string) (func(context.Context) error, error) {
	entities := Entities(ctx)

	if client, err := entities.Client(id); err == nil {
		if err := client.Clone(ctx, options.Client().SetTimeout(0)); err != nil {
			return nil, fmt.Errorf("error cloning client entity: %v", err)
		}
		cleanup := func(ctx context.Context) error {
			return client.Reset(ctx)
		}
		return cleanup, nil
	}
	if db, err := entities.Database(id); err == nil {
		cloned := db.Clone(options.Database().SetTimeout(0))
		if err := entities.OverwriteDatabaseEntity(id, cloned); err != nil {
			return nil, fmt.Errorf("error overwriting database entity: %v", err)
		}

		cleanup := func(context.Context) error {
			return entities.OverwriteDatabaseEntity(id, db)
		}
		return cleanup, nil
	}
	if coll, err := entities.Collection(id); err == nil {
		cloned := coll.Clone(options.Collection().SetTimeout(0))
		if err := entities.OverwriteCollectionEntity(id, cloned); err != nil {
			return nil, fmt.Errorf("error overwriting collection entity: %v", err)
		}

		cleanup := func(context.Context) error {
			return entities.OverwriteCollectionEntity(id, coll)
		}
		return cleanup, nil
	}

	return nil, fmt.Errorf("no client, database, or collection entity found with ID %q", id)
}
