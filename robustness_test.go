package valkeyqueue_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/server"
	"github.com/go-hypercube/go-hypercube/queue"
	valkeyqueue "github.com/go-hypercube/hypercube-queue-valkey"
	"github.com/stretchr/testify/require"
	"github.com/valkey-io/valkey-go"
)

func TestOpaqueStringFieldsPreserveNonUTF8Bytes(t *testing.T) {
	f := newTestQueue(t)
	ctx := context.Background()
	original := &queue.Message{
		ID: "id:\xff\x00", QueueName: "queue:\xfe\x00",
		Namespace: "namespace:\xfd", JobName: "job:\xfc",
		Payload: []byte{}, Extra: nil,
	}
	require.NoError(t, f.q.Push(ctx, original))
	first := popOne(t, f.q, original.QueueName)
	require.Equal(t, original.ID, first.ID)
	require.Equal(t, original.QueueName, first.QueueName)
	require.Equal(t, original.Namespace, first.Namespace)
	require.Equal(t, original.JobName, first.JobName)
	require.NotNil(t, first.Payload)
	require.Empty(t, first.Payload)
	require.Nil(t, first.Extra)
	require.NoError(t, f.q.DeadLetter(ctx, first))
	n, err := f.q.Redrive(ctx, original.QueueName, 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	second := popOne(t, f.q, original.QueueName)
	require.Equal(t, original.ID, second.ID)
	require.Equal(t, original.Namespace, second.Namespace)
	require.Equal(t, original.JobName, second.JobName)
	require.NoError(t, f.q.Ack(ctx, second))
}

func TestSubmillisecondVisibilityDoesNotBecomeTheDefault(t *testing.T) {
	for _, duration := range []time.Duration{-time.Nanosecond, time.Nanosecond} {
		t.Run(duration.String(), func(t *testing.T) {
			f := newTestQueue(t)
			f.freezeTime()
			ctx := context.Background()
			require.NoError(t, f.q.Push(ctx, &queue.Message{QueueName: "jobs", VisibilityTimeout: duration}))
			delivery := popOne(t, f.q, "jobs")
			if duration > 0 {
				requireStats(t, f.q, queue.QueueStats{Name: "jobs", InFlight: 1})
				f.advanceTime(time.Millisecond)
			}
			requireStats(t, f.q, queue.QueueStats{Name: "jobs", Ready: 1})
			require.ErrorIs(t, f.q.ExtendVisibility(ctx, time.Second, delivery), queue.ErrNotFound)
			requireBatchFailures(t, f.q.Ack(ctx, delivery), []*queue.Message{delivery}, map[int]error{0: queue.ErrNotFound})
		})
	}
}

func TestRedriveCommandReplayDoesNotExceedRequestedCount(t *testing.T) {
	for _, count := range []int{1, 129} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f := newTestQueue(t)
			ctx := context.Background()
			input := make([]*queue.Message, count+10)
			for i := range input {
				input[i] = &queue.Message{QueueName: "jobs"}
			}
			require.NoError(t, f.q.Push(ctx, input...))
			deliveries := popExactly(t, f.q, "jobs", len(input))
			require.NoError(t, f.q.DeadLetter(ctx, deliveries...))
			// Simulate a lost response followed by an identical request, even though
			// automatic client retries are disabled and these scripts are mutating.
			client := &replayingClient{Client: f.client}
			q := valkeyqueue.New(client, valkeyqueue.WithKeyPrefix(f.prefix))
			n, err := q.Redrive(ctx, "jobs", count)
			require.NoError(t, err)
			require.Equal(t, count, n)
			require.Positive(t, client.replays.Load(), "the test must actually replay a successful script")
			requireStats(t, f.q, queue.QueueStats{Name: "jobs", Ready: int64(count), Dead: 10})
		})
	}
}

type replayingClient struct {
	valkey.Client
	replays atomic.Int64
}

func (c *replayingClient) Do(ctx context.Context, command valkey.Completed) valkey.ValkeyResult {
	// Do recycles the Completed and its argument slice. Copy before submitting,
	// and build a fresh command for the replay instead of submitting it twice.
	args := append([]string(nil), command.Commands()...)
	result := c.Client.Do(ctx, command)
	if result.Error() != nil || len(args) < 3 || !isScriptCommand(args[0]) {
		return result
	}
	keyCount, err := strconv.Atoi(args[2])
	if err != nil || keyCount < 0 || keyCount > len(args)-3 {
		return valkey.NewErrorResult(fmt.Errorf("invalid script key count in replay: %q", args[2]))
	}
	keyEnd := 3 + keyCount
	replay := c.B().Arbitrary(args[0]).Args(args[1:3]...).Keys(args[3:keyEnd]...).Args(args[keyEnd:]...).Build()
	c.replays.Add(1)
	return c.Client.Do(ctx, replay)
}

func TestCancellationStopsWaitingForActiveBackendCalls(t *testing.T) {
	for _, operation := range []string{"Push", "Pop", "Ack", "Retry", "DeadLetter", "ExtendVisibility", "Stats", "Queues", "Redrive"} {
		for _, deadline := range []bool{false, true} {
			name := operation + "/cancel"
			if deadline {
				name = operation + "/deadline"
			}
			t.Run(name, func(t *testing.T) {
				f := newTestQueue(t)
				ctx := context.Background()
				require.NoError(t, f.q.Push(ctx, &queue.Message{QueueName: "jobs"}, &queue.Message{QueueName: "jobs"}))
				deliveries := popExactly(t, f.q, "jobs", 2)
				require.NoError(t, f.q.DeadLetter(ctx, deliveries[1]))
				require.NoError(t, f.q.Push(ctx, &queue.Message{QueueName: "jobs"}))

				started := make(chan struct{})
				resume := make(chan struct{})
				var once sync.Once
				// Pause the server after receiving the actual native client request.
				// The client must observe its context while waiting on socket I/O;
				// no wrapper blocks Do or pretends to implement cancellation.
				f.server.Server().SetPreHook(func(_ *server.Peer, command string, _ ...string) bool {
					if isScriptCommand(command) {
						once.Do(func() { close(started) })
						<-resume
					}
					return false
				})
				defer func() {
					close(resume)
					f.server.Server().SetPreHook(nil)
				}()

				call := func(ctx context.Context) error {
					switch operation {
					case "Push":
						return f.q.Push(ctx, &queue.Message{QueueName: "jobs"})
					case "Pop":
						messages, err := f.q.Pop(ctx, "jobs", 1)
						if err != nil && messages != nil {
							return fmt.Errorf("canceled Pop returned messages with error: %v", err)
						}
						return err
					case "Ack":
						return f.q.Ack(ctx, deliveries[0])
					case "Retry":
						return f.q.Retry(ctx, deliveries[0])
					case "DeadLetter":
						return f.q.DeadLetter(ctx, deliveries[0])
					case "ExtendVisibility":
						return f.q.ExtendVisibility(ctx, time.Minute, deliveries[0])
					case "Stats":
						_, err := f.q.Stats(ctx, "jobs")
						return err
					case "Queues":
						_, err := f.q.Queues(ctx)
						return err
					default:
						_, err := f.q.Redrive(ctx, "jobs", 1)
						return err
					}
				}
				canceled, cancel := context.WithCancel(ctx)
				want := context.Canceled
				if deadline {
					cancel()
					canceled, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
					want = context.DeadlineExceeded
				}
				defer cancel()
				type callResult struct {
					err        error
					ctxErr     error
					returnedAt time.Time
				}
				done := make(chan callResult, 1)
				go func() {
					err := call(canceled)
					// Capture before sending: the receiver may observe the context later.
					ctxErr := canceled.Err()
					returnedAt := time.Now()
					done <- callResult{err: err, ctxErr: ctxErr, returnedAt: returnedAt}
				}()
				select {
				case <-started:
				case <-time.After(3 * time.Second):
					t.Fatal("operation never sent a Lua request to miniredis")
				}
				if !deadline {
					cancel()
				}
				select {
				case result := <-done:
					if !errors.Is(result.err, want) {
						assertionCtxErr := canceled.Err()
						t.Logf("cancellation mismatch: error=%T: %v; ctx.Err() at return=%T: %v; ctx.Err() at assertion=%T: %v; returnedAt=%s",
							result.err, result.err, result.ctxErr, result.ctxErr, assertionCtxErr, assertionCtxErr,
							result.returnedAt.Format(time.RFC3339Nano))
						if contextDeadline, ok := canceled.Deadline(); ok {
							t.Logf("deadline=%s; deadlinePassedAtReturn=%t; returnMinusDeadline=%s",
								contextDeadline.Format(time.RFC3339Nano), !result.returnedAt.Before(contextDeadline), result.returnedAt.Sub(contextDeadline))
						}
						var batchErr *queue.BatchError
						if errors.As(result.err, &batchErr) {
							for i, failed := range batchErr.Failed {
								t.Logf("BatchError.Failed[%d] (Index=%d).Err=%T: %v", i, failed.Index, failed.Err, failed.Err)
							}
						}
					}
					require.ErrorIs(t, result.err, want)
				case <-time.After(1500 * time.Millisecond):
					t.Fatal("operation waited for the paused backend after its context was done")
				}
				// Submitted work may complete after resume; cancellation does not
				// promise rollback, so no backend-state assertion is made here.
			})
		}
	}
}

func isScriptCommand(command string) bool {
	switch strings.ToUpper(command) {
	case "EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO":
		return true
	default:
		return false
	}
}

func TestScriptsAreNotMarkedReadOnlyOrRetryable(t *testing.T) {
	f := newTestQueue(t)
	client := &scriptRecordingClient{Client: f.client}
	q := valkeyqueue.New(client, valkeyqueue.WithKeyPrefix(f.prefix), valkeyqueue.WithPollingTimeout(0))
	ctx := context.Background()
	var delivery *queue.Message
	calls := []struct {
		name string
		call func() error
	}{
		{"Push", func() error { return q.Push(ctx, &queue.Message{QueueName: "jobs"}) }},
		{"Pop", func() error { delivery = popOne(t, q, "jobs"); return nil }},
		{"Stats", func() error { _, err := q.Stats(ctx, "jobs"); return err }},
		{"Queues", func() error { _, err := q.Queues(ctx); return err }},
	}
	for _, call := range calls {
		assertOrdinaryScript(t, client, call.name, call.call)
	}
	assertOrdinaryScript(t, client, "ExtendVisibility", func() error {
		return q.ExtendVisibility(ctx, time.Minute, delivery)
	})
	assertOrdinaryScript(t, client, "Retry", func() error { return q.Retry(ctx, delivery) })
	delivery = popOne(t, q, "jobs")
	assertOrdinaryScript(t, client, "DeadLetter", func() error { return q.DeadLetter(ctx, delivery) })
	assertOrdinaryScript(t, client, "Redrive", func() error { _, err := q.Redrive(ctx, "jobs", 1); return err })
	delivery = popOne(t, q, "jobs")
	assertOrdinaryScript(t, client, "Ack", func() error { return q.Ack(ctx, delivery) })
}

type scriptRecordingClient struct {
	valkey.Client
	commands []recordedScript
}

type recordedScript struct {
	name      string
	readOnly  bool
	retryable bool
}

func (c *scriptRecordingClient) Do(ctx context.Context, command valkey.Completed) valkey.ValkeyResult {
	// Inspect only before Do, which recycles the command. These tests call
	// this wrapper sequentially, so the recording slice needs no mutex.
	args := command.Commands()
	if len(args) > 0 && isScriptCommand(args[0]) {
		c.commands = append(c.commands, recordedScript{
			name: args[0], readOnly: command.IsReadOnly(), retryable: command.IsRetryable(),
		})
	}
	return c.Client.Do(ctx, command)
}

func assertOrdinaryScript(t *testing.T, client *scriptRecordingClient, name string, call func() error) {
	t.Helper()
	client.commands = nil
	require.NoError(t, call(), name)
	require.NotEmpty(t, client.commands, "%s must execute a native Lua script", name)
	for _, command := range client.commands {
		require.Contains(t, []string{"EVAL", "EVALSHA"}, command.name, name)
		require.False(t, command.readOnly, "%s must not mark %s readonly", name, command.name)
		require.False(t, command.retryable, "%s must not mark %s retryable", name, command.name)
	}
}

func TestNativeRESP2StateTransitionsShareWithDefaultRESP3Client(t *testing.T) {
	f := newTestQueue(t)
	f.freezeTime()
	options := valkeyClientOptions(f.addr)
	options.AlwaysRESP2 = true
	client := newValkeyClientWithOptions(t, options)
	q := valkeyqueue.New(client, valkeyqueue.WithKeyPrefix(f.prefix), valkeyqueue.WithPollingTimeout(0))
	ctx := context.Background()
	for _, tc := range []struct {
		client valkey.Client
		want   int64
	}{{f.client, 3}, {client, 2}} {
		hello, err := tc.client.Do(ctx, tc.client.B().Hello().Protover(tc.want).Build()).AsMap()
		require.NoError(t, err)
		protocol, ok := hello["proto"]
		require.True(t, ok)
		got, err := protocol.ToInt64()
		require.NoError(t, err)
		require.Equal(t, tc.want, got, "the native clients must handle both RESP3 and RESP2 replies")
	}
	original := &queue.Message{
		ID: "resp2:\xff\x00", QueueName: "jobs:\xfe\x00", Namespace: "app:\xfd", JobName: "job:\xfc",
		Payload: []byte{0, 255, 128}, Extra: []byte{}, VisibilityTimeout: time.Minute + 7*time.Nanosecond,
	}
	require.NoError(t, q.Push(ctx, original))
	requireStats(t, f.q, queue.QueueStats{Name: original.QueueName, Ready: 1})
	delivery := popOne(t, f.q, original.QueueName)
	requireMessageFields(t, *original, delivery)
	require.Equal(t, 1, delivery.Attempt)
	require.NoError(t, q.ExtendVisibility(ctx, time.Minute, delivery))
	delivery.Payload = []byte("updated through RESP2")
	delivery.Delay = time.Second
	expected := copyDelivery(delivery)
	require.NoError(t, q.Retry(ctx, delivery))
	requireStats(t, q, queue.QueueStats{Name: original.QueueName, Delayed: 1})
	f.advanceTime(time.Second)
	retried := popOne(t, q, original.QueueName)
	requireMessageFields(t, *expected, retried)
	require.Equal(t, 2, retried.Attempt)
	require.Zero(t, retried.Delay)
	require.NoError(t, q.DeadLetter(ctx, retried))
	requireStats(t, q, queue.QueueStats{Name: original.QueueName, Dead: 1})
	names, err := q.Queues(ctx)
	require.NoError(t, err)
	require.Contains(t, names, original.QueueName)
	n, err := q.Redrive(ctx, original.QueueName, 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	redriven := popOne(t, q, original.QueueName)
	requireMessageFields(t, *expected, redriven)
	require.Equal(t, 1, redriven.Attempt)
	require.Zero(t, redriven.Delay)
	require.NoError(t, f.q.Ack(ctx, redriven))
	requireStats(t, q, queue.QueueStats{Name: original.QueueName})
	requirePopEmpty(t, q, original.QueueName)
}
