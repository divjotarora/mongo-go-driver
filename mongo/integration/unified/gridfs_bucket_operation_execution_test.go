// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package unified

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

func executeBucketDelete(ctx context.Context, operation *Operation) (*OperationResult, error) {
	bucket, err := Entities(ctx).GridFSBucket(operation.Object)
	if err != nil {
		return nil, err
	}

	var id *bson.RawValue
	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "id":
			id = &val
		default:
			return nil, fmt.Errorf("unrecognized bucket delete option %q", key)
		}
	}
	if id == nil {
		return nil, newMissingArgumentError("id")
	}

	if deadline, ok := ctx.Deadline(); ok {
		if err := bucket.SetWriteDeadline(deadline); err != nil {
			return nil, fmt.Errorf("error setting bucket read dealine: %v", err)
		}
	}

	return NewErrorResult(bucket.Delete(*id)), nil
}

func executeBucketDownload(ctx context.Context, operation *Operation) (*OperationResult, error) {
	bucket, err := Entities(ctx).GridFSBucket(operation.Object)
	if err != nil {
		return nil, err
	}

	var id *bson.RawValue
	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "id":
			id = &val
		default:
			return nil, fmt.Errorf("unrecognized bucket download option %q", key)
		}
	}
	if id == nil {
		return nil, newMissingArgumentError("id")
	}

	if deadline, ok := ctx.Deadline(); ok {
		if err := bucket.SetReadDeadline(deadline); err != nil {
			return nil, fmt.Errorf("error setting bucket read deadline: %v", err)
		}
	}

	stream, err := bucket.OpenDownloadStream(*id)
	if err != nil {
		return NewErrorResult(err), nil
	}

	var buffer bytes.Buffer
	if _, err := io.Copy(&buffer, stream); err != nil {
		return NewErrorResult(err), nil
	}

	return NewValueResult(bsontype.Binary, bsoncore.AppendBinary(nil, 0, buffer.Bytes()), nil), nil
}

func executeBucketDrop(ctx context.Context, operation *Operation) (*OperationResult, error) {
	bucket, err := Entities(ctx).GridFSBucket(operation.Object)
	if err != nil {
		return nil, err
	}

	elems, _ := operation.Arguments.Elements()
	if len(elems) > 0 {
		return nil, fmt.Errorf("unrecongized drop options %v", operation.Arguments)
	}

	if deadline, ok := ctx.Deadline(); ok {
		if err := bucket.SetWriteDeadline(deadline); err != nil {
			return nil, fmt.Errorf("error setting bucket write deadline: %v", err)
		}
	}

	return NewErrorResult(bucket.Drop()), nil
}

func executeBucketFind(ctx context.Context, operation *Operation) (*OperationResult, error) {
	bucket, err := Entities(ctx).GridFSBucket(operation.Object)
	if err != nil {
		return nil, err
	}

	var filter bson.Raw
	opts := options.GridFSFind()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "filter":
			filter = val.Document()
		case "maxTimeMS":
			maxTime := time.Duration(val.Int32()) * time.Millisecond
			opts.SetMaxTime(maxTime)
		default:
			return nil, fmt.Errorf("unrecognized find option %q", key)
		}
	}
	if filter == nil {
		return nil, newMissingArgumentError("filter")
	}

	if deadline, ok := ctx.Deadline(); ok {
		if err := bucket.SetReadDeadline(deadline); err != nil {
			return nil, fmt.Errorf("error setting bucket read deadline: %v", err)
		}
	}

	cursor, err := bucket.Find(filter, opts)
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

func executeBucketRename(ctx context.Context, operation *Operation) (*OperationResult, error) {
	bucket, err := Entities(ctx).GridFSBucket(operation.Object)
	if err != nil {
		return nil, err
	}

	var fileID *bson.RawValue
	var newName string

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "id":
			fileID = &val
		case "newFilename":
			newName = val.StringValue()
		default:
			return nil, fmt.Errorf("unrecognized rename option %q", key)
		}
	}
	if fileID == nil {
		return nil, newMissingArgumentError("id")
	}
	if newName == "" {
		return nil, newMissingArgumentError("newFilename")
	}

	if deadline, ok := ctx.Deadline(); ok {
		if err := bucket.SetWriteDeadline(deadline); err != nil {
			return nil, fmt.Errorf("error setting bucket write deadline: %v", err)
		}
	}

	return NewErrorResult(bucket.Rename(fileID, newName)), nil
}

func executeBucketUpload(ctx context.Context, operation *Operation) (*OperationResult, error) {
	bucket, err := Entities(ctx).GridFSBucket(operation.Object)
	if err != nil {
		return nil, err
	}

	var filename string
	var fileBytes []byte
	opts := options.GridFSUpload()

	elems, _ := operation.Arguments.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "chunkSizeBytes":
			opts.SetChunkSizeBytes(val.Int32())
		case "filename":
			filename = val.StringValue()
		case "metadata":
			opts.SetMetadata(val.Document())
		case "source":
			fileBytes, err = hex.DecodeString(val.Document().Lookup("$$hexBytes").StringValue())
			if err != nil {
				return nil, fmt.Errorf("error converting source string to bytes: %v", err)
			}
		default:
			return nil, fmt.Errorf("unrecognized bucket upload option %q", key)
		}
	}
	if filename == "" {
		return nil, newMissingArgumentError("filename")
	}
	if fileBytes == nil {
		return nil, newMissingArgumentError("source")
	}

	if dl, ok := ctx.Deadline(); ok {
		if err := bucket.SetWriteDeadline(dl); err != nil {
			return nil, fmt.Errorf("error setting bucket write deadline: %v", err)
		}
	}

	fileID, err := bucket.UploadFromStream(filename, bytes.NewReader(fileBytes), opts)
	if err != nil {
		return NewErrorResult(err), nil
	}

	if operation.ResultEntityID != nil {
		fileIDValue := bson.RawValue{
			Type:  bsontype.ObjectID,
			Value: fileID[:],
		}
		if err := Entities(ctx).AddBSONEntity(*operation.ResultEntityID, fileIDValue); err != nil {
			return nil, fmt.Errorf("error storing result as BSON entity: %v", err)
		}
	}

	return NewValueResult(bsontype.ObjectID, fileID[:], nil), nil
}
