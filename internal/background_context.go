// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package internal

import (
	"context"
	"time"
)

// backgroundContext is an implementation of the context.Context interface that wraps a child Context. Value requests
// are forwarded to the child Context but the Done and Err functions are overridden to ensure the new context does not
// time out or get cancelled.
type backgroundContext struct {
	context.Context
	childValuesCtx context.Context
}

// NewBackgroundContext creates a new Context whose behavior matches that of context.Background(), but Value calls are
// forwarded to the provided ctx parameter. If ctx is nil, context.Background() is returned.
func NewBackgroundContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}

	return &backgroundContext{
		Context:        context.Background(),
		childValuesCtx: ctx,
	}
}

var timeoutMSCtxKey struct{}

func WrapTimeoutMSContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, timeoutMSCtxKey, true)
}

func NewContextWithTimeout(valuesCtx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	wrapped := &backgroundContext{
		Context:        ctx,
		childValuesCtx: valuesCtx,
	}

	return wrapped, cancel
}

func (b *backgroundContext) Value(key interface{}) interface{} {
	return b.childValuesCtx.Value(key)
}

// TODO: change name
func ContextHasDeadline(ctx context.Context) bool {
	return ctx.Value(timeoutMSCtxKey) != nil
}
