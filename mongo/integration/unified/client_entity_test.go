// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package unified

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

// ClientEntity is a wrapper for a mongo.Client object that also holds additional information required during test
// execution.
type ClientEntity struct {
	*mongo.Client

	recordEvents    atomic.Value
	started         []*event.CommandStartedEvent
	succeeded       []*event.CommandSucceededEvent
	failed          []*event.CommandFailedEvent
	ignoredCommands map[string]struct{}
	originalOptions *EntityOptions
}

func NewClientEntity(ctx context.Context, entityOptions *EntityOptions) (*ClientEntity, error) {
	entity := &ClientEntity{
		// The "configureFailPoint" command should always be ignored.
		ignoredCommands: map[string]struct{}{
			"configureFailPoint": {},
		},
		originalOptions: entityOptions,
	}
	if err := entity.configureClient(ctx, entityOptions); err != nil {
		return nil, err
	}

	return entity, nil
}

func (c *ClientEntity) configureClient(ctx context.Context, entityOptions *EntityOptions, extraOptions ...*options.ClientOptions) error {
	// Construct a ClientOptions instance by first applying the cluster URI and then the URIOptions map to ensure that
	// the options specified in the test file take precedence.
	clientOpts := options.Client().ApplyURI(mtest.ClusterURI())
	if entityOptions.URIOptions != nil {
		if err := setClientOptionsFromURIOptions(clientOpts, entityOptions.URIOptions); err != nil {
			return fmt.Errorf("error parsing URI options: %v", err)
		}
	}
	// UseMultipleMongoses is only relevant if we're connected to a sharded cluster. Options changes and validation are
	// only required if the option is explicitly set. If it's unset, we make no changes because the cluster URI already
	// includes all nodes and we don't enforce any limits on the number of nodes.
	if mtest.ClusterTopologyKind() == mtest.Sharded && entityOptions.UseMultipleMongoses != nil {
		if err := evaluateUseMultipleMongoses(clientOpts, *entityOptions.UseMultipleMongoses); err != nil {
			return err
		}
	}
	if entityOptions.ObserveEvents != nil {
		// Configure a command monitor that listens for the specified event types. We don't take the IgnoredCommands
		// option into account here because it can be overridden at the test level after the entity has already been
		// created, so we store the events for now but account for it when iterating over them later.
		monitor := &event.CommandMonitor{}

		for _, eventType := range entityOptions.ObserveEvents {
			switch eventType {
			case "commandStartedEvent":
				monitor.Started = c.processStartedEvent
			case "commandSucceededEvent":
				monitor.Succeeded = c.processSucceededEvent
			case "commandFailedEvent":
				monitor.Failed = c.processFailedEvent
			default:
				return fmt.Errorf("unrecognized event type %s", eventType)
			}
		}
		clientOpts.SetMonitor(monitor)
	}
	for _, cmd := range entityOptions.IgnoredCommands {
		c.ignoredCommands[cmd] = struct{}{}
	}

	allOptions := []*options.ClientOptions{clientOpts}
	client, err := mongo.Connect(ctx, append(allOptions, extraOptions...)...)
	if err != nil {
		return fmt.Errorf("error creating mongo.Client: %v", err)
	}

	c.setRecordEvents(false)
	if c.Client != nil {
		if err := c.Client.Disconnect(ctx); err != nil {
			return fmt.Errorf("error disconnecting old Client: %v", err)
		}
	}
	c.Client = client
	c.setRecordEvents(true)
	return nil
}

func (c *ClientEntity) Clone(ctx context.Context, clientOptions *options.ClientOptions) error {
	return c.configureClient(ctx, c.originalOptions, clientOptions)
}

func (c *ClientEntity) Reset(ctx context.Context) error {
	return c.configureClient(ctx, c.originalOptions)
}

func (c *ClientEntity) StopListeningForEvents() {
	c.setRecordEvents(false)
}

// StartedEvents returns a slice of the CommandStartedEvent instances captured for this entity. Events for any commands
// that are ignored for this entity are not included. The filter parameter optionally provides a more restrictive
// filtering mechanism. Each captured event will only be included in the returned slice if the filter is nil or if
// filter(event) returns true.
func (c *ClientEntity) StartedEvents(filter func(*event.CommandStartedEvent) bool) []*event.CommandStartedEvent {
	var events []*event.CommandStartedEvent
	for _, evt := range c.started {
		if _, ok := c.ignoredCommands[evt.CommandName]; ok {
			continue
		}
		if filter == nil || filter(evt) {
			events = append(events, evt)
		}
	}

	return events
}

// StartedEvents returns a slice of the CommandSucceededEvent instances captured for this entity. Events for any
// commands that are ignored for this entity are not included. The filter parameter optionally provides a more
// restrictive filtering mechanism. Each captured event will only be included in the returned slice if the filter is nil
// or if filter(event) returns true.
func (c *ClientEntity) SucceededEvents(filter func(*event.CommandSucceededEvent) bool) []*event.CommandSucceededEvent {
	var events []*event.CommandSucceededEvent
	for _, evt := range c.succeeded {
		if _, ok := c.ignoredCommands[evt.CommandName]; ok {
			continue
		}
		if filter == nil || filter(evt) {
			events = append(events, evt)
		}
	}

	return events
}

// StartedEvents returns a slice of the CommandFailedEvent instances captured for this entity. Events for any commands
// that are ignored for this entity are not included. The filter parameter optionally provides a more restrictive
// filtering mechanism. Each captured event will only be included in the returned slice if the filter is nil or if
// filter(event) returns true.
func (c *ClientEntity) FailedEvents(filter func(*event.CommandFailedEvent) bool) []*event.CommandFailedEvent {
	var events []*event.CommandFailedEvent
	for _, evt := range c.failed {
		if _, ok := c.ignoredCommands[evt.CommandName]; ok {
			continue
		}
		if filter == nil || filter(evt) {
			events = append(events, evt)
		}
	}

	return events
}

func (c *ClientEntity) processStartedEvent(_ context.Context, evt *event.CommandStartedEvent) {
	if c.getRecordEvents() {
		c.started = append(c.started, evt)
	}
}

func (c *ClientEntity) processSucceededEvent(_ context.Context, evt *event.CommandSucceededEvent) {
	if c.getRecordEvents() {
		c.succeeded = append(c.succeeded, evt)
	}
}

func (c *ClientEntity) processFailedEvent(_ context.Context, evt *event.CommandFailedEvent) {
	if c.getRecordEvents() {
		c.failed = append(c.failed, evt)
	}
}

func (c *ClientEntity) setRecordEvents(record bool) {
	c.recordEvents.Store(record)
}

func (c *ClientEntity) getRecordEvents() bool {
	return c.recordEvents.Load().(bool)
}

func setClientOptionsFromURIOptions(clientOpts *options.ClientOptions, uriOpts bson.M) error {
	// A write concern can be constructed across multiple URI options (e.g. "w", "j", and "wTimeoutMS") so we declare an
	// empty writeConcern instance here that can be populated in the loop below.
	var wc writeConcern
	var wcSet bool

	for key, value := range uriOpts {
		switch key {
		case "appName":
			clientOpts.SetAppName(value.(string))
		case "connectTimeoutMS":
			clientOpts.SetConnectTimeout(time.Duration(value.(int32)) * time.Millisecond)
		case "heartbeatFrequencyMS":
			clientOpts.SetHeartbeatInterval(time.Duration(value.(int32)) * time.Millisecond)
		case "readConcernLevel":
			clientOpts.SetReadConcern(readconcern.New(readconcern.Level(value.(string))))
		case "retryReads":
			clientOpts.SetRetryReads(value.(bool))
		case "retryWrites":
			clientOpts.SetRetryWrites(value.(bool))
		case "socketTimeoutMS":
			clientOpts.SetSocketTimeout(time.Duration(value.(int32)) * time.Millisecond)
		case "timeoutMS":
			clientOpts.SetTimeout(time.Duration(value.(int32)) * time.Millisecond)
		case "w":
			wc.W = value
			wcSet = true
		case "waitQueueTimeoutMS":
			return NewSkipTestError("waitQueueTimeoutMS Client option is not supported")
		case "wTimeoutMS":
			wtms := value.(int32)
			wc.WTimeoutMS = &wtms
			wcSet = true
		default:
			return fmt.Errorf("unrecognized URI option %s", key)
		}
	}

	if wcSet {
		converted, err := wc.toWriteConcernOption()
		if err != nil {
			return fmt.Errorf("error creating write concern: %v", err)
		}
		clientOpts.SetWriteConcern(converted)
	}
	return nil
}

func evaluateUseMultipleMongoses(clientOpts *options.ClientOptions, useMultipleMongoses bool) error {
	hosts := mtest.ClusterConnString().Hosts

	if !useMultipleMongoses {
		clientOpts.SetHosts(hosts[:1])
		return nil
	}

	if len(hosts) < 2 {
		return fmt.Errorf("multiple mongoses required but cluster URI %q only contains one host", mtest.ClusterURI())
	}
	return nil
}
