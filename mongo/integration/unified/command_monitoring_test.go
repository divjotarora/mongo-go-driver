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
	"go.mongodb.org/mongo-driver/event"
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

type CommandMonitoringEvent struct {
	CommandStartedEvent *struct {
		Command      bson.Raw `bson:"command"`
		CommandName  *string  `bson:"commandName"`
		DatabaseName *string  `bson:"databaseName"`
	} `bson:"commandStartedEvent"`

	CommandSucceededEvent *struct {
		CommandName *string  `bson:"commandName"`
		Reply       bson.Raw `bson:"reply"`
	} `bson:"commandSucceededEvent"`

	CommandFailedEvent *struct {
		CommandName *string `bson:"commandName"`
	} `bson:"commandFailedEvent"`
}

type ExpectedEvents struct {
	ClientID        string                   `bson:"client"`
	Events          []CommandMonitoringEvent `bson:"events"`
	IgnoredCommands []string                 `bson:"ignoreCommandMonitoringEvents"`
}

type AllEvents struct {
	Started   []*event.CommandStartedEvent
	Succeeded []*event.CommandSucceededEvent
	Failed    []*event.CommandFailedEvent
}

func (e *ExpectedEvents) GetAllEvents(client *ClientEntity) *AllEvents {
	startedFilter := func(evt *event.CommandStartedEvent) bool {
		return !stringSliceContains(e.IgnoredCommands, evt.CommandName)
	}
	succeededFilter := func(evt *event.CommandSucceededEvent) bool {
		return !stringSliceContains(e.IgnoredCommands, evt.CommandName)
	}
	failedFilter := func(evt *event.CommandFailedEvent) bool {
		return !stringSliceContains(e.IgnoredCommands, evt.CommandName)
	}

	return &AllEvents{
		Started:   client.StartedEvents(startedFilter),
		Succeeded: client.SucceededEvents(succeededFilter),
		Failed:    client.FailedEvents(failedFilter),
	}
}

func VerifyEvents(ctx context.Context, expectedEvents *ExpectedEvents) error {
	client, err := Entities(ctx).Client(expectedEvents.ClientID)
	if err != nil {
		return err
	}

	if expectedEvents.Events == nil {
		return nil
	}

	events := expectedEvents.GetAllEvents(client)

	// If the Events array is nil, verify that no events were sent.
	if len(expectedEvents.Events) == 0 && (len(events.Started)+len(events.Succeeded)+len(events.Failed) != 0) {
		return fmt.Errorf("expected no events to be sent but got %s", stringifyEvents(expectedEvents, client))
	}

	for idx, evt := range expectedEvents.Events {
		switch {
		case evt.CommandStartedEvent != nil:
			if len(events.Started) == 0 {
				return newEventVerificationError(idx, expectedEvents, client, "no CommandStartedEvent published")
			}

			actual := events.Started[0]
			events.Started = events.Started[1:]

			expected := evt.CommandStartedEvent
			if expected.CommandName != nil && *expected.CommandName != actual.CommandName {
				return newEventVerificationError(idx, expectedEvents, client, "expected command name %q, got %q",
					*expected.CommandName, actual.CommandName)
			}
			if expected.DatabaseName != nil && *expected.DatabaseName != actual.DatabaseName {
				return newEventVerificationError(idx, expectedEvents, client, "expected database name %q, got %q",
					*expected.DatabaseName, actual.DatabaseName)
			}
			if expected.Command != nil {
				expectedDoc := DocumentToRawValue(expected.Command)
				actualDoc := DocumentToRawValue(fixMaxTimeMS(actual.CommandName, expected.Command, actual.Command))
				if err := VerifyValuesMatch(ctx, expectedDoc, actualDoc, true); err != nil {
					return newEventVerificationError(idx, expectedEvents, client,
						"error comparing command documents: %v", err)
				}
			}
		case evt.CommandSucceededEvent != nil:
			if len(events.Succeeded) == 0 {
				return newEventVerificationError(idx, expectedEvents, client, "no CommandSucceededEvent published")
			}

			actual := events.Succeeded[0]
			events.Succeeded = events.Succeeded[1:]

			expected := evt.CommandSucceededEvent
			if expected.CommandName != nil && *expected.CommandName != actual.CommandName {
				return newEventVerificationError(idx, expectedEvents, client, "expected command name %q, got %q",
					*expected.CommandName, actual.CommandName)
			}
			if expected.Reply != nil {
				expectedDoc := DocumentToRawValue(expected.Reply)
				actualDoc := DocumentToRawValue(actual.Reply)
				if err := VerifyValuesMatch(ctx, expectedDoc, actualDoc, true); err != nil {
					return newEventVerificationError(idx, expectedEvents, client, "error comparing reply documents: %v",
						err)
				}
			}
		case evt.CommandFailedEvent != nil:
			if len(events.Failed) == 0 {
				return newEventVerificationError(idx, expectedEvents, client, "no CommandFailedEvent published")
			}

			actual := events.Failed[0]
			events.Failed = events.Failed[1:]

			expected := evt.CommandFailedEvent
			if expected.CommandName != nil && *expected.CommandName != actual.CommandName {
				return newEventVerificationError(idx, expectedEvents, client, "expected command name %q, got %q",
					*expected.CommandName, actual.CommandName)
			}
		default:
			return newEventVerificationError(idx, expectedEvents, client,
				"no expected event set on CommandMonitoringEvent instance")
		}
	}

	// Verify that there are no remaining events.
	if len(events.Started) > 0 || len(events.Succeeded) > 0 || len(events.Failed) > 0 {
		return fmt.Errorf("extra events published; all events for client: %s", stringifyEvents(expectedEvents, client))
	}
	return nil
}

func newEventVerificationError(idx int, expected *ExpectedEvents, client *ClientEntity, msg string, args ...interface{}) error {
	fullMsg := fmt.Sprintf(msg, args...)
	return fmt.Errorf("event comparison failed at index %d: %s; all events found for client: %s", idx, fullMsg,
		stringifyEvents(expected, client))
}

func stringifyEvents(expectedEvents *ExpectedEvents, client *ClientEntity) string {
	events := expectedEvents.GetAllEvents(client)
	str := bytes.NewBuffer(nil)

	str.WriteString("\n\nStarted Events\n\n")
	for _, evt := range events.Started {
		str.WriteString(fmt.Sprintf("[%s] %s\n", evt.ConnectionID, evt.Command))
	}

	str.WriteString("\nSucceeded Events\n\n")
	for _, evt := range events.Succeeded {
		str.WriteString(fmt.Sprintf("[%s] CommandName: %s, Reply: %s\n", evt.ConnectionID, evt.CommandName, evt.Reply))
	}

	str.WriteString("\nFailed Events\n\n")
	for _, evt := range events.Failed {
		str.WriteString(fmt.Sprintf("[%s] CommandName: %s, Failure: %s\n", evt.ConnectionID, evt.CommandName, evt.Failure))
	}

	return str.String()
}

func fixMaxTimeMS(cmdName string, expectedCommand, actualCommand bson.Raw) bson.Raw {
	if _, ok := cursorCreatingCommands[cmdName]; !ok {
		return actualCommand
	}

	expectedVal, err := expectedCommand.LookupErr("maxTimeMS")
	if err != nil || expectedVal.IsNumber() {
		return actualCommand
	}

	existsFalseDoc := bsoncore.NewDocumentBuilder().
		AppendBoolean("$$exists", false).
		Build()
	if doc, ok := expectedVal.DocumentOK(); ok && bytes.Equal(doc, existsFalseDoc) {
		return actualCommand
	}

	actualCommand = actualCommand[:len(actualCommand)-1]
	actualCommand = bsoncore.AppendInt64Element(actualCommand, "maxTimeMS", 1)
	actualCommand, _ = bsoncore.AppendDocumentEnd(actualCommand, 0)
	return actualCommand
}
