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
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/internal/testutil/assert"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// keyPathCtxKey is used as a key for a Context object. The value conveys the BSON key path that is currently being
// compared.
type keyPathCtxKey struct{}

// extraKeysAllowedCtxKey is used as a key for a Context object. The value conveys whether or not the document under
// test can contain extra keys. For example, if the expected document is {x: 1}, the document {x: 1, y: 1} would match
// if the value for this key is true.
type extraKeysAllowedCtxKey struct{}

func makeMatchContext(ctx context.Context, keyPath string, extraKeysAllowed bool) context.Context {
	ctx = context.WithValue(ctx, keyPathCtxKey{}, keyPath)
	return context.WithValue(ctx, extraKeysAllowedCtxKey{}, extraKeysAllowed)
}

// VerifyValuesMatch compares the provided BSON values and returns an error if they do not match. If the values are
// documents and extraKeysAllowed is true, the actual value will be allowed to have additional keys at the top-level.
// For example, an expected document {x: 1} would match the actual document {x: 1, y: 1}.
func VerifyValuesMatch(ctx context.Context, expected, actual bson.RawValue, extraKeysAllowed bool) error {
	return verifyValuesMatch(makeMatchContext(ctx, "", extraKeysAllowed), expected, actual)
}

func verifyValuesMatch(ctx context.Context, expected, actual bson.RawValue) error {
	keyPath := ctx.Value(keyPathCtxKey{}).(string)
	extraKeysAllowed := ctx.Value(extraKeysAllowedCtxKey{}).(bool)

	if expectedDoc, ok := expected.DocumentOK(); ok {
		// If the root document only has one element and the key is a special matching operator, the actual value might
		// not actually be a document. In this case, evaluate the special operator with the actual value rather than
		// doing an element-wise document comparison.
		if requiresSpecialMatching(expectedDoc) {
			if err := evaluateSpecialComparison(ctx, expectedDoc, actual, ""); err != nil {
				return newMatchingError(keyPath, "error doing special matching assertion: %v", err)
			}
			return nil
		}

		actualDoc, ok := actual.DocumentOK()
		if !ok {
			return newMatchingError(keyPath, "expected value to be a document but got a %s", actual.Type)
		}

		// Perform element-wise comparisons.
		expectedElems, _ := expectedDoc.Elements()
		for _, expectedElem := range expectedElems {
			expectedKey := expectedElem.Key()
			expectedValue := expectedElem.Value()

			fullKeyPath := expectedKey
			if keyPath != "" {
				fullKeyPath = keyPath + "." + expectedKey
			}

			// Get the value from actualDoc here but don't check the error until later because some of the special
			// matching operators can assert that the value isn't present in the document (e.g. $$exists).
			actualValue, err := actualDoc.LookupErr(expectedKey)
			if specialDoc, ok := expectedValue.DocumentOK(); ok && requiresSpecialMatching(specialDoc) {
				// Reset the key path so any errors returned from the function will only have the key path for the
				// target value. Also unconditionally set extraKeysAllowed to false because an assertion like
				// $$unsetOrMatches could recurse back into this function. In that case, the target document is nested
				// and should not have extra keys.
				ctx = makeMatchContext(ctx, "", false)
				if err := evaluateSpecialComparison(ctx, specialDoc, actualValue, expectedKey); err != nil {
					return newMatchingError(fullKeyPath, "error doing special matching assertion: %v", err)
				}
				continue
			}

			// This isn't a special comparison. Assert that the value exists in the actual document.
			if err != nil {
				return newMatchingError(fullKeyPath, "key not found in actual document")
			}

			// Nested documents cannot have extra keys, so we unconditionally pass false for extraKeysAllowed.
			comparisonCtx := makeMatchContext(ctx, fullKeyPath, false)
			if err := verifyValuesMatch(comparisonCtx, expectedValue, actualValue); err != nil {
				return err
			}
		}
		// If required, verify that the actual document does not have extra elements. We do this by iterating over the
		// actual and checking for each key in the expected rather than comparing element counts because the presence of
		// special operators can cause incorrect counts. For example, the document {y: {$$exists: false}} has one
		// element, but should match the document {}, which has none.
		if !extraKeysAllowed {
			actualElems, _ := actualDoc.Elements()
			for _, actualElem := range actualElems {
				if _, err := expectedDoc.LookupErr(actualElem.Key()); err != nil {
					return newMatchingError(keyPath, "extra key %q found in actual document %s", actualElem.Key(),
						actualDoc)
				}
			}
		}

		return nil
	}
	if expectedArr, ok := expected.ArrayOK(); ok {
		actualArr, ok := actual.ArrayOK()
		if !ok {
			return newMatchingError(keyPath, "expected value to be an array but got a %s", actual.Type)
		}

		expectedValues, _ := expectedArr.Values()
		actualValues, _ := actualArr.Values()

		// Arrays must always have the same number of elements.
		if len(expectedValues) != len(actualValues) {
			return newMatchingError(keyPath, "expected array length %d, got %d", len(expectedValues),
				len(actualValues))
		}

		for idx, expectedValue := range expectedValues {
			// Use the index as the key to augment the key path.
			fullKeyPath := fmt.Sprintf("%d", idx)
			if keyPath != "" {
				fullKeyPath = keyPath + "." + fullKeyPath
			}

			comparisonCtx := makeMatchContext(ctx, fullKeyPath, extraKeysAllowed)
			err := verifyValuesMatch(comparisonCtx, expectedValue, actualValues[idx])
			if err != nil {
				return err
			}
		}

		return nil
	}

	// Numeric values must be considered equal even if their types are different (e.g. if expected is an int32 and
	// actual is an int64).
	if expected.IsNumber() {
		if !actual.IsNumber() {
			return newMatchingError(keyPath, "expected value to be a number but got a %s", actual.Type)
		}

		expectedInt64 := expected.AsInt64()
		actualInt64 := actual.AsInt64()
		if expectedInt64 != actualInt64 {
			return newMatchingError(keyPath, "expected numeric value %d, got %d", expectedInt64, actualInt64)
		}
		return nil
	}

	// If expected is not a recursive or numeric type, we can directly call Equal to do the comparison.
	if !expected.Equal(actual) {
		return newMatchingError(keyPath, "expected value %s, got %s", expected, actual)
	}
	return nil
}

func evaluateSpecialComparison(ctx context.Context, assertionDoc bson.Raw, actual bson.RawValue, fieldName string) error {
	assertionElem := assertionDoc.Index(0)
	assertion := assertionElem.Key()
	assertionVal := assertionElem.Value()

	switch assertion {
	case "$$exists":
		shouldExist := assertionVal.Boolean()
		exists := actual.Validate() == nil
		if shouldExist != exists {
			return fmt.Errorf("expected value to exist: %v; value actually exists: %v", shouldExist, exists)
		}
	case "$$type":
		possibleTypes, err := getTypesArray(assertionVal)
		if err != nil {
			return fmt.Errorf("error getting possible types for a $$type assertion: %v", err)
		}

		for _, possibleType := range possibleTypes {
			if actual.Type == possibleType {
				return nil
			}
		}
		return fmt.Errorf("expected type to be one of %v but was %s", possibleTypes, actual.Type)
	case "$$matchesEntity":
		expected, err := Entities(ctx).BSONValue(assertionVal.StringValue())
		if err != nil {
			return err
		}

		// $$matchesEntity doesn't modify the nesting level of the key path so we can propagate ctx without changes.
		return verifyValuesMatch(ctx, expected, actual)
	case "$$matchesHexBytes":
		expectedBytes, err := hex.DecodeString(assertionVal.StringValue())
		if err != nil {
			return fmt.Errorf("error converting $$matcesHexBytes value to bytes: %v", err)
		}

		_, actualBytes, ok := actual.BinaryOK()
		if !ok {
			return fmt.Errorf("expected binary value for a $$matchesHexBytes assertion, but got a %s", actual.Type)
		}
		if !bytes.Equal(expectedBytes, actualBytes) {
			return fmt.Errorf("expected bytes %v, got %v", expectedBytes, actualBytes)
		}
	case "$$unsetOrMatches":
		if actual.Validate() != nil {
			return nil
		}

		// $$unsetOrMatches doesn't modify the nesting level or the key path so we can propagate the context to the
		// comparison function without changing anything.
		return verifyValuesMatch(ctx, assertionVal, actual)
	case "$$sessionLsid":
		sess, err := Entities(ctx).Session(assertionVal.StringValue())
		if err != nil {
			return err
		}

		expectedID := sess.ID()
		actualID, ok := actual.DocumentOK()
		if !ok {
			return fmt.Errorf("expected document value for a $$sessionLsid assertion, but got a %s", actual.Type)
		}
		if !bytes.Equal(expectedID, actualID) {
			return fmt.Errorf("expected lsid %v, got %v", expectedID, actualID)
		}
	case "$$lte":
		expectedNumber, ok := assertionVal.AsInt64OK()
		if !ok {
			return fmt.Errorf("expected value %s is not a number", assertionVal)
		}
		actualNumber, ok := actual.AsInt64OK()
		if !ok {
			return fmt.Errorf("actual value %s is not a number", actual)
		}

		if expectedNumber > actualNumber {
			return fmt.Errorf("expected actual value %d to be less than or equal to %d", actualNumber, expectedNumber)
		}
	default:
		return fmt.Errorf("unrecognized special matching assertion %q", assertion)
	}

	return nil
}

func requiresSpecialMatching(doc bson.Raw) bool {
	elems, _ := doc.Elements()
	return len(elems) == 1 && strings.HasPrefix(elems[0].Key(), "$$")
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
	case "bool":
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

// newMatchingError creates an error to convey that BSON value comparison failed at the provided key path. If the
// key path is empty (e.g. because the values being compared were not documents), the error message will contain the
// phrase "top-level" instead of the path.
func newMatchingError(keyPath, msg string, args ...interface{}) error {
	fullMsg := fmt.Sprintf(msg, args...)
	if keyPath == "" {
		return fmt.Errorf("comparison error at top-level: %s", fullMsg)
	}
	return fmt.Errorf("comparison error at key %q: %s", keyPath, fullMsg)
}

func TestMatches(t *testing.T) {
	ctx := context.Background()
	unmarshalExtJSONValue := func(t *testing.T, str string) bson.RawValue {
		t.Helper()

		if str == "" {
			return bson.RawValue{}
		}

		var val bson.RawValue
		err := bson.UnmarshalExtJSON([]byte(str), false, &val)
		assert.Nil(t, err, "UnmarshalExtJSON error: %v", err)
		return val
	}
	marshalValue := func(t *testing.T, val interface{}) bson.RawValue {
		t.Helper()

		valType, data, err := bson.MarshalValue(val)
		assert.Nil(t, err, "MarshalValue error: %v", err)
		return bson.RawValue{
			Type:  valType,
			Value: data,
		}
	}

	assertMatches := func(t *testing.T, expected, actual bson.RawValue, shouldMatch bool) {
		t.Helper()

		err := VerifyValuesMatch(ctx, expected, actual, true)
		if shouldMatch {
			assert.Nil(t, err, "expected values to match, but got comparison error %v", err)
			return
		}
		assert.NotNil(t, err, "expected values to not match, but got no error")
	}

	t.Run("documents with extra keys allowed", func(t *testing.T) {
		expectedDoc := `{"x": 1, "y": {"a": 1, "b": 2}}`
		expectedVal := unmarshalExtJSONValue(t, expectedDoc)

		extraKeysAtRoot := `{"x": 1, "y": {"a": 1, "b": 2}, "z": 3}`
		extraKeysInEmbedded := `{"x": 1, "y": {"a": 1, "b": 2, "c": 3}}`

		testCases := []struct {
			name    string
			actual  string
			matches bool
		}{
			{"exact match", expectedDoc, true},
			{"extra keys allowed at root-level", extraKeysAtRoot, true},
			{"incorrect type for y", `{"x": 1, "y": 2}`, false},
			{"extra keys prohibited in embedded documents", extraKeysInEmbedded, false},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				assertMatches(t, expectedVal, unmarshalExtJSONValue(t, tc.actual), tc.matches)
			})
		}
	})
	t.Run("documents with extra keys not allowed", func(t *testing.T) {
		expected := unmarshalExtJSONValue(t, `{"x": 1}`)
		actual := unmarshalExtJSONValue(t, `{"x": 1, "y": 1}`)
		err := VerifyValuesMatch(ctx, expected, actual, false)
		assert.NotNil(t, err, "expected values to not match, but got no error")
	})
	t.Run("exists operator", func(t *testing.T) {
		rootExists := unmarshalExtJSONValue(t, `{"x": {"$$exists": true}}`)
		rootNotExists := unmarshalExtJSONValue(t, `{"x": {"$$exists": false}}`)
		embeddedExists := unmarshalExtJSONValue(t, `{"x": {"y": {"$$exists": true}}}`)
		embeddedNotExists := unmarshalExtJSONValue(t, `{"x": {"y": {"$$exists": false}}}`)

		testCases := []struct {
			name     string
			expected bson.RawValue
			actual   string
			matches  bool
		}{
			{"root - should exist and does", rootExists, `{"x": 1}`, true},
			{"root - should exist and does not", rootExists, `{"y": 1}`, false},
			{"root - should not exist and does", rootNotExists, `{"x": 1}`, false},
			{"root - should not exist and does not", rootNotExists, `{"y": 1}`, true},
			{"embedded - should exist and does", embeddedExists, `{"x": {"y": 1}}`, true},
			{"embedded - should exist and does not", embeddedExists, `{"x": {}}`, false},
			{"embedded - should not exist and does", embeddedNotExists, `{"x": {"y": 1}}`, false},
			{"embedded - should not exist and does not", embeddedNotExists, `{"x": {}}`, true},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				assertMatches(t, tc.expected, unmarshalExtJSONValue(t, tc.actual), tc.matches)
			})
		}
	})
	t.Run("type operator", func(t *testing.T) {
		singleType := unmarshalExtJSONValue(t, `{"x": {"$$type": "string"}}`)
		arrayTypes := unmarshalExtJSONValue(t, `{"x": {"$$type": ["string", "bool"]}}`)

		testCases := []struct {
			name     string
			expected bson.RawValue
			actual   string
			matches  bool
		}{
			{"single type matches", singleType, `{"x": "foo"}`, true},
			{"single type does not match", singleType, `{"x": 1}`, false},
			{"multiple types matches first", arrayTypes, `{"x": "foo"}`, true},
			{"multiple types matches second", arrayTypes, `{"x": true}`, true},
			{"multiple types does not match", arrayTypes, `{"x": 1}`, false},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				assertMatches(t, tc.expected, unmarshalExtJSONValue(t, tc.actual), tc.matches)
			})
		}
	})
	t.Run("matchesHexBytes operator", func(t *testing.T) {
		singleValue := unmarshalExtJSONValue(t, `{"$$matchesHexBytes": "DEADBEEF"}`)
		document := unmarshalExtJSONValue(t, `{"x": {"$$matchesHexBytes": "DEADBEEF"}}`)

		stringToBinary := func(str string) bson.RawValue {
			hexBytes, err := hex.DecodeString(str)
			assert.Nil(t, err, "hex.DecodeString error: %v", err)
			return bson.RawValue{
				Type:  bsontype.Binary,
				Value: bsoncore.AppendBinary(nil, 0, hexBytes),
			}
		}

		testCases := []struct {
			name     string
			expected bson.RawValue
			actual   bson.RawValue
			matches  bool
		}{
			{"single value matches", singleValue, stringToBinary("DEADBEEF"), true},
			{"single value does not match", singleValue, stringToBinary("BEEF"), false},
			{"document matches", document, marshalValue(t, bson.M{"x": stringToBinary("DEADBEEF")}), true},
			{"document does not match", document, marshalValue(t, bson.M{"x": stringToBinary("BEEF")}), false},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				assertMatches(t, tc.expected, tc.actual, tc.matches)
			})
		}
	})
	t.Run("unsetOrMatches operator", func(t *testing.T) {
		topLevel := unmarshalExtJSONValue(t, `{"$$unsetOrMatches": {"x": 1}}`)
		nested := unmarshalExtJSONValue(t, `{"x": {"$$unsetOrMatches": {"y": 1}}}`)

		testCases := []struct {
			name     string
			expected bson.RawValue
			actual   string
			matches  bool
		}{
			{"top-level unset", topLevel, "", true},
			{"top-level matches", topLevel, `{"x": 1}`, true},
			{"top-level matches with extra keys", topLevel, `{"x": 1, "y": 1}`, true},
			{"top-level does not match", topLevel, `{"x": 2}`, false},
			{"nested unset", nested, `{}`, true},
			{"nested matches", nested, `{"x": {"y": 1}}`, true},
			{"nested field exists but is null", nested, `{"x": null}`, false}, // null should not be considered unset
			{"nested does not match", nested, `{"x": {"y": 2}}`, false},
			{"nested does not match due to extra keys", nested, `{"x": {"y": 1, "z": 1}}`, false},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				assertMatches(t, tc.expected, unmarshalExtJSONValue(t, tc.actual), tc.matches)
			})
		}
	})
}
