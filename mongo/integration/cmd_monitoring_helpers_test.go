// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package integration

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/internal/testutil/assert"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

var (
	cursorCreatingCommands = map[string]struct{}{
		"aggregate":       {},
		"find":            {},
		"listCollections": {},
		"listIndexes":     {},
	}
)

// Helper functions to compare BSON values and command monitoring expectations.

func numberFromValue(mt *mtest.T, val bson.RawValue) int64 {
	mt.Helper()

	switch val.Type {
	case bson.TypeInt32:
		return int64(val.Int32())
	case bson.TypeInt64:
		return val.Int64()
	case bson.TypeDouble:
		return int64(val.Double())
	default:
		mt.Fatalf("unexpected type for number: %v", val.Type)
	}

	return 0
}

func compareNumberValues(mt *mtest.T, key string, expected, actual bson.RawValue) error {
	eInt := numberFromValue(mt, expected)
	if eInt == 42 {
		if actual.Type == bson.TypeNull {
			return fmt.Errorf("expected non-null value for key %s, got null", key)
		}
		return nil
	}

	aInt := numberFromValue(mt, actual)
	if eInt != aInt {
		return fmt.Errorf("value mismatch for key %s; expected %s, got %s", key, expected, actual)
	}
	return nil
}

// compare BSON values and fail if they are not equal. the key parameter is used for error strings.
// if the expected value is a numeric type (int32, int64, or double) and the value is 42, the function only asserts that
// the actual value is non-null.
func compareValues(mt *mtest.T, key string, expected, actual bson.RawValue) error {
	mt.Helper()

	switch expected.Type {
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble:
		if err := compareNumberValues(mt, key, expected, actual); err != nil {
			return err
		}
		return nil
	case bson.TypeString:
		val := expected.StringValue()
		if val == "42" {
			if actual.Type == bson.TypeNull {
				return fmt.Errorf("expected non-null value for key %s, got null", key)
			}
			return nil
		}
		break // compare bytes for expected.Value and actual.Value outside of the switch
	case bson.TypeEmbeddedDocument:
		e := expected.Document()
		if typeVal, err := e.LookupErr("$$type"); err == nil {
			// $$type represents a type assertion
			// for example {field: {$$type: "binData"}} should assert that "field" is an element with a binary value
			return checkValueType(mt, key, actual.Type, typeVal)
		}

		a := actual.Document()
		return compareDocsHelper(mt, e, a, key)
	case bson.TypeArray:
		e := expected.Array()
		a := actual.Array()
		return compareDocsHelper(mt, e, a, key)
	}

	if expected.Type != actual.Type {
		return fmt.Errorf("type mismatch for key %s; expected %s, got %s", key, expected.Type, actual.Type)
	}
	if !bytes.Equal(expected.Value, actual.Value) {
		return fmt.Errorf("value mismatch for key %s; expected %s, got %s", key, expected.Value, actual.Value)
	}
	return nil
}

// helper for $$type assertions
func checkValueType(mt *mtest.T, key string, actual bsontype.Type, typeVal bson.RawValue) error {
	mt.Helper()

	possibleTypes, err := getTypesArray(typeVal)
	if err != nil {
		return fmt.Errorf("error getting array of types: %v", err)
	}

	for _, possibleType := range possibleTypes {
		if actual == possibleType {
			return nil
		}
	}
	return fmt.Errorf("expected type to be one of %v but was %s", possibleTypes, actual)
}

func getTypesArray(val bson.RawValue) ([]bsontype.Type, error) {
	switch val.Type {
	case bsontype.String:
		convertedType, err := convertStringToBSONType(val.StringValue())
		if err != nil {
			return nil, err
		}

		return []bsontype.Type{convertedType}, nil
	case bsontype.Array:
		var typeStrings []string
		if err := val.Unmarshal(&typeStrings); err != nil {
			return nil, fmt.Errorf("error unmarshalling to slice of strings: %v", err)
		}

		var types []bsontype.Type
		for _, typeStr := range typeStrings {
			convertedType, err := convertStringToBSONType(typeStr)
			if err != nil {
				return nil, err
			}

			types = append(types, convertedType)
		}
		return types, nil
	default:
		return nil, fmt.Errorf("invalid type to convert to bsontype.Type slice: %s", val.Type)
	}
}

func convertStringToBSONType(typeStr string) (bsontype.Type, error) {
	switch typeStr {
	case "double":
		return bsontype.Double, nil
	case "string":
		return bsontype.String, nil
	case "object":
		return bsontype.EmbeddedDocument, nil
	case "array":
		return bsontype.Array, nil
	case "binData":
		return bsontype.Binary, nil
	case "undefined":
		return bsontype.Undefined, nil
	case "objectId":
		return bsontype.ObjectID, nil
	case "boolean":
		return bsontype.Boolean, nil
	case "date":
		return bsontype.DateTime, nil
	case "null":
		return bsontype.Null, nil
	case "regex":
		return bsontype.Regex, nil
	case "dbPointer":
		return bsontype.DBPointer, nil
	case "javascript":
		return bsontype.JavaScript, nil
	case "symbol":
		return bsontype.Symbol, nil
	case "javascriptWithScope":
		return bsontype.CodeWithScope, nil
	case "int":
		return bsontype.Int32, nil
	case "timestamp":
		return bsontype.Timestamp, nil
	case "long":
		return bsontype.Int64, nil
	case "decimal":
		return bsontype.Decimal128, nil
	case "minKey":
		return bsontype.MinKey, nil
	case "maxKey":
		return bsontype.MaxKey, nil
	default:
		return bsontype.Type(0), fmt.Errorf("unrecognized BSON type string %q", typeStr)
	}
}

// compare expected and actual BSON documents. comparison succeeds if actual contains each element in expected.
func compareDocsHelper(mt *mtest.T, expected, actual bson.Raw, prefix string) error {
	mt.Helper()

	eElems, err := expected.Elements()
	assert.Nil(mt, err, "error getting expected elements: %v", err)

	for _, e := range eElems {
		eKey := e.Key()
		fullKeyName := eKey
		if prefix != "" {
			fullKeyName = prefix + "." + eKey
		}

		aVal, err := actual.LookupErr(eKey)
		if err != nil {
			return fmt.Errorf("key %s not found in result", fullKeyName)
		}

		if err := compareValues(mt, fullKeyName, e.Value(), aVal); err != nil {
			return err
		}
	}
	return nil
}

func compareDocs(mt *mtest.T, expected, actual bson.Raw) error {
	mt.Helper()
	return compareDocsHelper(mt, expected, actual, "")
}

func checkExpectations(mt *mtest.T, expectations *[]*expectation, id0, id1 bson.Raw) {
	mt.Helper()

	// If the expectations field in the test JSON is null, we want to skip all command monitoring assertions.
	if expectations == nil {
		return
	}

	// Filter out events that shouldn't show up in monitoring expectations.
	ignoredEvents := map[string]struct{}{
		"configureFailPoint": {},
	}
	mt.FilterStartedEvents(func(evt *event.CommandStartedEvent) bool {
		// ok is true if the command should be ignored, so return !ok
		_, ok := ignoredEvents[evt.CommandName]
		return !ok
	})
	mt.FilterSucceededEvents(func(evt *event.CommandSucceededEvent) bool {
		// ok is true if the command should be ignored, so return !ok
		_, ok := ignoredEvents[evt.CommandName]
		return !ok
	})
	mt.FilterFailedEvents(func(evt *event.CommandFailedEvent) bool {
		// ok is true if the command should be ignored, so return !ok
		_, ok := ignoredEvents[evt.CommandName]
		return !ok
	})

	// If the expectations field in the test JSON is non-null but is empty, we want to assert that no events were
	// emitted.
	if len(*expectations) == 0 {
		// One of the bulkWrite spec tests expects update and updateMany to be grouped together into a single batch,
		// but this isn't the case because of GODRIVER-1157. To work around this, we expect one event to be emitted for
		// that test rather than 0. This assertion should be changed when GODRIVER-1157 is done.
		numExpectedEvents := 0
		bulkWriteTestName := "BulkWrite_on_server_that_doesn't_support_arrayFilters_with_arrayFilters_on_second_op"
		if strings.HasSuffix(mt.Name(), bulkWriteTestName) {
			numExpectedEvents = 1
		}

		numActualEvents := len(mt.GetAllStartedEvents())
		assert.Equal(mt, numExpectedEvents, numActualEvents, "expected %d events to be sent, but got %d events",
			numExpectedEvents, numActualEvents)
		return
	}

	for idx, expectation := range *expectations {
		var err error

		if expectation.CommandStartedEvent != nil {
			err = compareStartedEvent(mt, expectation, id0, id1)
		}
		if expectation.CommandSucceededEvent != nil {
			err = compareSucceededEvent(mt, expectation)
		}
		if expectation.CommandFailedEvent != nil {
			err = compareFailedEvent(mt, expectation)
		}

		assert.Nil(mt, err, "expectation comparison error at index %v: %s", idx, err)
	}
}

func fixMaxTimeMS(commandName string, expectedCommand, actualCommand bson.Raw) bson.Raw {
	if _, ok := cursorCreatingCommands[commandName]; !ok {
		return actualCommand
	}

	expectedVal, err := expectedCommand.LookupErr("maxTimeMS")
	if err != nil || expectedVal.IsNumber() {
		return actualCommand
	}

	actualCommand = actualCommand[:len(actualCommand)-1]
	actualCommand = bsoncore.AppendInt64Element(actualCommand, "maxTimeMS", 1)
	actualCommand, _ = bsoncore.AppendDocumentEnd(actualCommand, 0)
	return actualCommand
}

func compareStartedEvent(mt *mtest.T, expectation *expectation, id0, id1 bson.Raw) error {
	mt.Helper()

	expected := expectation.CommandStartedEvent
	evt := mt.GetStartedEvent()
	if evt == nil {
		return errors.New("expected CommandStartedEvent, got nil")
	}
	actualCommand := fixMaxTimeMS(expected.CommandName, expected.Command, evt.Command)

	if expected.CommandName != "" && expected.CommandName != evt.CommandName {
		return fmt.Errorf("command name mismatch; expected %s, got %s", expected.CommandName, evt.CommandName)
	}
	if expected.DatabaseName != "" && expected.DatabaseName != evt.DatabaseName {
		return fmt.Errorf("database name mismatch; expected %s, got %s", expected.DatabaseName, evt.DatabaseName)
	}

	eElems, err := expected.Command.Elements()
	if err != nil {
		return fmt.Errorf("error getting expected command elements: %s", err)
	}

	for _, elem := range eElems {
		key := elem.Key()
		val := elem.Value()

		actualVal, err := actualCommand.LookupErr(key)

		// Keys that may be nil
		if val.Type == bson.TypeNull {
			assert.NotNil(mt, err, "expected key %q to be omitted but got %q", key, actualVal)
			continue
		}
		assert.Nil(mt, err, "expected command to contain key %q", key)

		if key == "batchSize" {
			// Some command monitoring tests expect that the driver will send a lower batch size if the required batch
			// size is lower than the operation limit. We only do this for legacy servers <= 3.0 because those server
			// versions do not support the limit option, but not for 3.2+. We've already validated that the command
			// contains a batchSize field above and we can skip the actual value comparison below.
			continue
		}

		switch key {
		case "lsid":
			sessName := val.StringValue()
			var expectedID bson.Raw
			actualID := actualVal.Document()

			switch sessName {
			case "session0":
				expectedID = id0
			case "session1":
				expectedID = id1
			default:
				return fmt.Errorf("unrecognized session identifier in command document: %s", sessName)
			}

			if !bytes.Equal(expectedID, actualID) {
				return fmt.Errorf("session ID mismatch for session %s; expected %s, got %s", sessName, expectedID,
					actualID)
			}
		default:
			if err := compareValues(mt, key, val, actualVal); err != nil {
				return err
			}
		}
	}
	return nil
}

func compareWriteErrors(mt *mtest.T, expected, actual bson.Raw) error {
	mt.Helper()

	expectedErrors, _ := expected.Values()
	actualErrors, _ := actual.Values()

	for i, expectedErrVal := range expectedErrors {
		expectedErr := expectedErrVal.Document()
		actualErr := actualErrors[i].Document()

		eIdx := expectedErr.Lookup("index").Int32()
		aIdx := actualErr.Lookup("index").Int32()
		if eIdx != aIdx {
			return fmt.Errorf("write error index mismatch at index %d; expected %d, got %d", i, eIdx, aIdx)
		}

		eCode := expectedErr.Lookup("code").Int32()
		aCode := actualErr.Lookup("code").Int32()
		if eCode != 42 && eCode != aCode {
			return fmt.Errorf("write error code mismatch at index %d; expected %d, got %d", i, eCode, aCode)
		}

		eMsg := expectedErr.Lookup("errmsg").StringValue()
		aMsg := actualErr.Lookup("errmsg").StringValue()
		if eMsg == "" {
			if aMsg == "" {
				return fmt.Errorf("write error message mismatch at index %d; expected non-empty message, got empty", i)
			}
			return nil
		}
		if eMsg != aMsg {
			return fmt.Errorf("write error message mismatch at index %d, expected %s, got %s", i, eMsg, aMsg)
		}
	}
	return nil
}

func compareSucceededEvent(mt *mtest.T, expectation *expectation) error {
	mt.Helper()

	expected := expectation.CommandSucceededEvent
	evt := mt.GetSucceededEvent()
	if evt == nil {
		return errors.New("expected CommandSucceededEvent, got nil")
	}

	if expected.CommandName != "" && expected.CommandName != evt.CommandName {
		return fmt.Errorf("command name mismatch; expected %s, got %s", expected.CommandName, evt.CommandName)
	}

	eElems, err := expected.Reply.Elements()
	if err != nil {
		return fmt.Errorf("error getting expected reply elements: %s", err)
	}

	for _, elem := range eElems {
		key := elem.Key()
		val := elem.Value()
		actualVal := evt.Reply.Lookup(key)

		switch key {
		case "writeErrors":
			if err = compareWriteErrors(mt, val.Array(), actualVal.Array()); err != nil {
				return err
			}
		default:
			if err := compareValues(mt, key, val, actualVal); err != nil {
				return err
			}
		}
	}
	return nil
}

func compareFailedEvent(mt *mtest.T, expectation *expectation) error {
	mt.Helper()

	expected := expectation.CommandFailedEvent
	evt := mt.GetFailedEvent()
	if evt == nil {
		return errors.New("expected CommandFailedEvent, got nil")
	}

	if expected.CommandName != "" && expected.CommandName != evt.CommandName {
		return fmt.Errorf("command name mismatch; expected %s, got %s", expected.CommandName, evt.CommandName)
	}
	return nil
}
