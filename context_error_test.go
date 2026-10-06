package valkeyqueue_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-hypercube/go-hypercube/queue"
	valkeyqueue "github.com/go-hypercube/hypercube-queue-valkey"
	"github.com/stretchr/testify/require"
	"github.com/valkey-io/valkey-go"
)

func TestBackendContextErrorsRemainDetectableBeforeContextTimerFires(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			f := newTestQueue(t)
			ctx := context.Background()
			require.NoError(t, f.q.Push(ctx, &queue.Message{QueueName: "jobs"}))
			delivery := popOne(t, f.q, "jobs")
			client := &contextFailureClient{Client: f.client, err: fmt.Errorf("backend wait: %w", cause)}
			q := valkeyqueue.New(client, valkeyqueue.WithKeyPrefix(f.prefix))
			for _, call := range []struct {
				name  string
				apply func() error
			}{
				{"Push", func() error { return q.Push(ctx, &queue.Message{QueueName: "jobs"}) }},
				{"Ack", func() error { return q.Ack(ctx, delivery) }},
				{"Retry", func() error { return q.Retry(ctx, delivery) }},
				{"DeadLetter", func() error { return q.DeadLetter(ctx, delivery) }},
			} {
				t.Run(call.name, func(t *testing.T) {
					err := call.apply()
					require.NoError(t, ctx.Err(), "simulate a backend observing cancellation before ctx.Err is published")
					require.ErrorIs(t, err, cause)
					var batch *queue.BatchError
					require.False(t, errors.As(err, &batch), "context failures are operation-wide")
				})
			}
			require.NoError(t, f.q.Ack(ctx, delivery))
		})
	}
}

type contextFailureClient struct {
	valkey.Client
	err error
}

func (c *contextFailureClient) Do(context.Context, valkey.Completed) valkey.ValkeyResult {
	return valkey.NewErrorResult(c.err)
}
