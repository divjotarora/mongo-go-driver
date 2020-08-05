// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package internal

import (
	"context"
)

// CancellationListener listens for context cancellation in a loop until the context expires or the listener is aborted.
type CancellationListener struct {
	ctxChan     chan context.Context
	stopCurrent chan struct{}
	stopAll     chan struct{}
	abortFn     func()
}

// NewCancellationListener constructs a CancellationListener.
func NewCancellationListener(abortFn func()) *CancellationListener {
	return &CancellationListener{
		ctxChan:     make(chan context.Context, 1),
		stopCurrent: make(chan struct{}),
		stopAll:     make(chan struct{}),
		abortFn:     abortFn,
	}
}

// Run starts a persistent loop which waits for new contexts to be provided via AddContext. For each context, it waits
// until the context expires or the listener is aborted.
func (c *CancellationListener) Run() {
	for {
		// Wait until a new context is available or the listener is exiting.
		var ctx context.Context
		select {
		case ctx = <-c.ctxChan:
		case <-c.stopAll:
			return
		}

		// We don't listen for c.stopAll here because AbortCurrentContext can race with Exit. If they're called at the
		// same time and the stopAll case is selected, we'll exit the routine and AbortCurrentContext will hang forever
		// waiting for stopCurrent to be read.
		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				c.abortFn()
			}

			<-c.stopCurrent
		case <-c.stopCurrent:
		}
	}
}

// AddContext provides a new context to wait listener. The listener will loop until the context expires,
// AbortCurrentContext is called, or Exit is called.
func (c *CancellationListener) AddContext(ctx context.Context) {
	c.ctxChan <- ctx
}

// AbortCurrentContext forces the listener to stop listening to the last provided context. This function must only be
// called after AddContext.
func (c *CancellationListener) AbortCurrentContext() {
	c.stopCurrent <- struct{}{}
}

// Exit signals the routine to stop waiting for new contexts.
func (c *CancellationListener) Exit() {
	c.stopAll <- struct{}{}
}
