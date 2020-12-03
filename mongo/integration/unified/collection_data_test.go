// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package unified

import (
	"bytes"
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	testhelpers "go.mongodb.org/mongo-driver/internal/testutil/helpers"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

type CollectionData struct {
	DatabaseName      string     `bson:"databaseName"`
	CollectionName    string     `bson:"collectionName"`
	CollectionOptions bson.Raw   `bson:"collectionOptions"`
	Documents         []bson.Raw `bson:"documents"`
}

// CreateCollection configures the collection represented by the receiver using the internal client. This function
// first drops the collection and then creates it and inserts the seed data if needed.
func (c *CollectionData) CreateCollection(ctx context.Context) error {
	db := mtest.GlobalClient().Database(c.DatabaseName)
	coll := db.Collection(c.CollectionName, options.Collection().SetWriteConcern(mtest.MajorityWc))
	if err := coll.Drop(ctx); err != nil {
		return fmt.Errorf("error dropping collection: %v", err)
	}

	writeConcern := bsoncore.NewDocumentBuilder().
		AppendString("w", "majority").
		Build()

	// Create the collection with options if necessary.
	if c.CollectionOptions != nil {
		cmd := bsoncore.NewDocumentBuilder().
			AppendString("create", coll.Name()).
			AppendDocument("writeConcern", writeConcern)

		elems, _ := c.CollectionOptions.Elements()
		for _, elem := range elems {
			cmd.AppendValue(elem.Key(), RawValueToCoreValue(elem.Value()))
		}

		if err := db.RunCommand(ctx, cmd.Build()).Err(); err != nil {
			return fmt.Errorf("error creating collection with options: %v", err)
		}
	}

	// If no data is given, create the collection with write concern "majority".
	if len(c.Documents) == 0 {
		// If CollectionOptions is set, the collection has already been created.
		if c.CollectionOptions != nil {
			return nil
		}

		cmd := bsoncore.NewDocumentBuilder().
			AppendString("create", coll.Name()).
			AppendDocument("writeConcern", writeConcern).
			Build()
		if err := db.RunCommand(ctx, cmd).Err(); err != nil {
			return fmt.Errorf("error creating collection without options: %v", err)
		}
		return nil
	}

	// Insert the test data.
	docs := testhelpers.RawSliceToInterfaceSlice(c.Documents)
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		return fmt.Errorf("error inserting data: %v", err)
	}
	return nil
}

// VerifyContents asserts that the collection on the server represented by this CollectionData instance contains the
// expected documents.
func (c *CollectionData) VerifyContents(ctx context.Context) error {
	collOpts := options.Collection().
		SetReadPreference(readpref.Primary()).
		SetReadConcern(readconcern.Local())
	coll := mtest.GlobalClient().Database(c.DatabaseName).Collection(c.CollectionName, collOpts)

	cursor, err := coll.Find(ctx, bson.D{}, options.Find().SetSort(bson.M{"_id": 1}))
	if err != nil {
		return fmt.Errorf("Find error: %v", err)
	}
	defer cursor.Close(ctx)

	var docs []bson.Raw
	if err := cursor.All(ctx, &docs); err != nil {
		return fmt.Errorf("cursor iteration error: %v", err)
	}

	// Verify the slice lengths are equal. This also covers the case of asserting that the collection is empty if
	// c.Documents is an empty slice.
	if len(c.Documents) != len(docs) {
		return fmt.Errorf("expected %d documents but found %d: %v", len(c.Documents), len(docs), docs)
	}

	// We can't use VerifyValuesMatch here because the rules for evaluating matches (e.g. flexible numeric comparisons
	// and special $$ operators) do not apply when verifying collection outcomes. We have to permit variations in key
	// order, though, so we sort documents before doing a byte-wise comparison.
	for idx, expected := range c.Documents {
		expected = SortDocument(expected)
		actual := SortDocument(docs[idx])

		if !bytes.Equal(expected, actual) {
			return fmt.Errorf("document comparison error at index %d: expected %s, got %s", idx, expected, actual)
		}
	}
	return nil
}

func (c *CollectionData) Namespace() string {
	return fmt.Sprintf("%s.%s", c.DatabaseName, c.CollectionName)
}
