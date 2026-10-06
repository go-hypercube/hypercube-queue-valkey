package valkeyqueue_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-hypercube/go-hypercube/queue"
	valkeyqueue "github.com/go-hypercube/hypercube-queue-valkey"
	"github.com/stretchr/testify/require"
	"github.com/valkey-io/valkey-go"
)

var (
	_ queue.Queue              = (*valkeyqueue.Queue)(nil)
	_ queue.VisibilityExtender = (*valkeyqueue.Queue)(nil)
	_ queue.RedriveProvider    = (*valkeyqueue.Queue)(nil)
	_ queue.StatsProvider      = (*valkeyqueue.Queue)(nil)
	_ *valkeyqueue.Queue       = (*valkeyqueue.ValkeyQueue)(nil)
)

func TestPushSnapshotsMessagesAndReportsOriginalBatchIndices(t *testing.T) {
	f := newTestQueue(t)
	ctx := context.Background()
	shared := []byte{0, 255, 128, 'a', '\n'}
	generated := &queue.Message{
		QueueName:         "jobs",
		Namespace:         "namespace / {opaque}",
		JobName:           "job:\x00雪",
		Payload:           shared,
		Extra:             shared,
		Attempt:           99,
		VisibilityTimeout: time.Minute + 123*time.Nanosecond,
		// Push must discard DriverData, even when it cannot be JSON encoded.
		DriverData: make(chan struct{}),
	}
	fixed := &queue.Message{ID: "fixed", QueueName: "jobs", Payload: []byte("fixed payload")}
	require.NoError(t, f.q.Push(ctx, fixed))
	batchFixed := &queue.Message{ID: "batch-fixed", QueueName: "jobs", Payload: []byte("batch payload")}
	duplicate := &queue.Message{ID: "fixed", QueueName: "other", Payload: []byte("overwrite")}
	other := &queue.Message{ID: "other", QueueName: "other", Extra: []byte("metadata")}
	inputs := []*queue.Message{nil, generated, batchFixed, nil, duplicate, other, nil}
	requireBatchFailures(t, f.q.Push(ctx, inputs...), inputs, map[int]error{
		0: queue.ErrInvalidArgument,
		3: queue.ErrInvalidArgument,
		4: queue.ErrAlreadyExists,
		6: queue.ErrInvalidArgument,
	})
	require.NotEmpty(t, generated.ID)
	require.NotEqual(t, fixed.ID, generated.ID)
	require.Equal(t, 99, generated.Attempt, "Push must not rewrite caller-controlled input beyond assigning ID")
	generatedID := generated.ID
	expected := queue.Message{
		ID: generatedID, QueueName: "jobs", Namespace: generated.Namespace, JobName: generated.JobName,
		Payload: append([]byte(nil), shared...), Extra: append([]byte(nil), shared...),
		VisibilityTimeout: generated.VisibilityTimeout,
	}

	// The original Push inputs remain caller-owned, including their scalar fields.
	shared[0] = 'X'
	generated.ID = "caller-reused-id"
	generated.QueueName = "caller-reused-queue"
	generated.Namespace = "changed"
	generated.JobName = "changed"
	generated.Delay = time.Hour
	generated.VisibilityTimeout = time.Nanosecond
	fixed.Payload[0] = 'X'
	batchFixed.Payload[0] = 'X'
	other.Extra[0] = 'X'

	messages := popExactly(t, f.q, "jobs", 3)
	indexed := indexByID(t, messages)
	delivery := indexed[generatedID]
	require.NotNil(t, delivery)
	require.NotSame(t, generated, delivery)
	requireMessageFields(t, expected, delivery)
	require.Equal(t, 1, delivery.Attempt)
	require.Zero(t, delivery.Delay)
	require.NotNil(t, delivery.DriverData)
	require.NotNil(t, indexed["fixed"])
	require.NotNil(t, indexed["batch-fixed"])
	require.Equal(t, []byte("fixed payload"), indexed["fixed"].Payload)
	require.Equal(t, []byte("batch payload"), indexed["batch-fixed"].Payload)

	// Even fields backed by the same input slice must be independently owned.
	delivery.Payload[0] = 'Y'
	require.Equal(t, expected.Extra, delivery.Extra)
	otherDelivery := popOne(t, f.q, "other")
	require.Equal(t, "other", otherDelivery.ID, "the duplicate must not have been enqueued in another queue")
	require.Equal(t, []byte("metadata"), otherDelivery.Extra)
	require.NoError(t, f.q.Ack(ctx, messages...))
	require.NoError(t, f.q.Ack(ctx, otherDelivery))
	requireStats(t, f.q, queue.QueueStats{Name: "jobs"})
	requireStats(t, f.q, queue.QueueStats{Name: "other"})
}

func TestDuplicateIDsWithinOnePushBatchRetainExactlyOneMessage(t *testing.T) {
	f := newTestQueue(t)
	ctx := context.Background()
	inputs := []*queue.Message{
		{ID: "duplicate", QueueName: "first", Payload: []byte("first snapshot")},
		{ID: "independent", QueueName: "unrelated"},
		{ID: "duplicate", QueueName: "second", Payload: []byte("second snapshot")},
	}
	err := f.q.Push(ctx, inputs...)
	var batch *queue.BatchError
	require.ErrorAs(t, err, &batch)
	require.Len(t, batch.Failed, 1)
	failedIndex := batch.Failed[0].Index
	require.Contains(t, []int{0, 2}, failedIndex)
	requireBatchFailures(t, err, inputs, map[int]error{failedIndex: queue.ErrAlreadyExists})
	// Push processing order, like delivery order, is not part of the contract.
	winnerIndex := 2
	if failedIndex == 2 {
		winnerIndex = 0
	}
	winner := inputs[winnerIndex]
	delivery := popOne(t, f.q, winner.QueueName)
	require.Equal(t, winner.ID, delivery.ID)
	require.Equal(t, winner.Payload, delivery.Payload)
	requireStats(t, f.q, queue.QueueStats{Name: inputs[failedIndex].QueueName})
	require.NoError(t, f.q.Ack(ctx, delivery))
	independent := popOne(t, f.q, "unrelated")
	require.Equal(t, "independent", independent.ID)
	require.NoError(t, f.q.Ack(ctx, independent))
}

func TestGeneratedIDsAreUniqueAcrossQueuesAndClients(t *testing.T) {
	f := newTestQueue(t)
	peer := f.peer(t, f.prefix)
	const count = 80
	seen := make(map[string]bool, count)
	for i := 0; i < count; i++ {
		msg := &queue.Message{QueueName: fmt.Sprintf("queue-%d", i%3)}
		q := f.q
		if i%2 != 0 {
			q = peer
		}
		require.NoError(t, q.Push(context.Background(), msg))
		require.NotEmpty(t, msg.ID)
		require.False(t, seen[msg.ID], "generated duplicate ID %q", msg.ID)
		seen[msg.ID] = true
	}
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("queue-%d", i)
		want := count / 3
		if i < count%3 {
			want++
		}
		messages := popExactly(t, peer, name, want)
		for _, msg := range messages {
			require.True(t, seen[msg.ID])
			delete(seen, msg.ID)
			require.Equal(t, 1, msg.Attempt)
		}
		require.NoError(t, f.q.Ack(context.Background(), messages...))
	}
	require.Empty(t, seen)
}

func TestDuplicateIDsRemainReservedInEveryRetainedState(t *testing.T) {
	for _, state := range []string{"ready", "delayed", "in-flight", "dead"} {
		t.Run(state, func(t *testing.T) {
			f := newTestQueue(t)
			f.freezeTime()
			peer := f.peer(t, f.prefix)
			ctx := context.Background()
			original := &queue.Message{
				ID: "global-id", QueueName: "source", Namespace: "app", JobName: "original",
				Payload: []byte("original payload"), Extra: []byte("original extra"),
				VisibilityTimeout: time.Minute,
			}
			if state == "delayed" {
				original.Delay = time.Second
			}
			require.NoError(t, f.q.Push(ctx, original))
			var active *queue.Message
			if state == "in-flight" || state == "dead" {
				active = popOne(t, f.q, "source")
			}
			if state == "dead" {
				require.NoError(t, f.q.DeadLetter(ctx, active))
				active = nil
			}
			before, err := f.q.Stats(ctx, "source")
			require.NoError(t, err)
			duplicate := &queue.Message{
				ID: original.ID, QueueName: "different queue", Namespace: "replacement",
				Payload: []byte("must not overwrite"), Extra: []byte("must not overwrite"),
			}
			requireBatchFailures(t, peer.Push(ctx, duplicate), []*queue.Message{duplicate}, map[int]error{0: queue.ErrAlreadyExists})
			requireStats(t, peer, before)
			requireStats(t, peer, queue.QueueStats{Name: "different queue"})

			if state == "delayed" {
				f.advanceTime(time.Second + 50*time.Millisecond)
			}
			if state == "dead" {
				n, err := peer.Redrive(ctx, "source", 1)
				require.NoError(t, err)
				require.Equal(t, 1, n)
			}
			if active == nil {
				active = popOne(t, peer, "source")
			}
			requireMessageFields(t, *original, active)
			require.Equal(t, 1, active.Attempt)
			require.NoError(t, peer.Ack(ctx, active))

			// Ack releases the reservation; no permanent tombstone is required.
			reused := &queue.Message{ID: original.ID, QueueName: "different queue", Payload: []byte("new logical message")}
			require.NoError(t, f.q.Push(ctx, reused))
			newDelivery := popOne(t, peer, reused.QueueName)
			require.Equal(t, []byte("new logical message"), newDelivery.Payload)
			require.Equal(t, 1, newDelivery.Attempt)
			require.NoError(t, f.q.Ack(ctx, newDelivery))
		})
	}
}

func TestSettlementBatchesContinuePastNilAndMissingItems(t *testing.T) {
	for _, operation := range []string{"Ack", "Retry", "DeadLetter"} {
		t.Run(operation, func(t *testing.T) {
			f := newTestQueue(t)
			ctx := context.Background()
			inputs := []*queue.Message{
				{ID: "one", QueueName: "jobs"},
				{ID: "two", QueueName: "jobs"},
				{ID: "three", QueueName: "jobs"},
			}
			require.NoError(t, f.q.Push(ctx, inputs...))
			deliveries := popExactly(t, f.q, "jobs", 3)
			// A known logical ID without a receipt must not settle a live delivery.
			withoutReceipt := &queue.Message{ID: deliveries[1].ID, QueueName: "jobs"}
			unknown := &queue.Message{ID: "unknown", QueueName: "jobs"}
			batch := []*queue.Message{nil, deliveries[0], withoutReceipt, nil, deliveries[1], unknown, deliveries[2], nil}
			var err error
			switch operation {
			case "Ack":
				err = f.q.Ack(ctx, batch...)
			case "Retry":
				err = f.q.Retry(ctx, batch...)
			case "DeadLetter":
				err = f.q.DeadLetter(ctx, batch...)
			}
			requireBatchFailures(t, err, batch, map[int]error{
				0: queue.ErrInvalidArgument, 2: queue.ErrNotFound, 3: queue.ErrInvalidArgument,
				5: queue.ErrNotFound, 7: queue.ErrInvalidArgument,
			})
			switch operation {
			case "Ack":
				requireStats(t, f.q, queue.QueueStats{Name: "jobs"})
			case "Retry":
				requireStats(t, f.q, queue.QueueStats{Name: "jobs", Ready: 3})
				retried := popExactly(t, f.q, "jobs", 3)
				for _, msg := range retried {
					require.Equal(t, 2, msg.Attempt)
					require.Zero(t, msg.Delay)
				}
				require.NoError(t, f.q.Ack(ctx, retried...))
			case "DeadLetter":
				requireStats(t, f.q, queue.QueueStats{Name: "jobs", Dead: 3})
				requirePopEmpty(t, f.q, "jobs")
			}
		})
	}
}

func TestEmptyAndNilOnlyBatchesDoNotContactBackend(t *testing.T) {
	f := newTestQueue(t)
	counter := &commandCounter{Client: f.client}
	f.q = valkeyqueue.New(counter, valkeyqueue.WithKeyPrefix(f.prefix), valkeyqueue.WithPollingTimeout(0))
	operations := []struct {
		name string
		call func(context.Context, ...*queue.Message) error
	}{
		{"Push", f.q.Push}, {"Ack", f.q.Ack}, {"Retry", f.q.Retry}, {"DeadLetter", f.q.DeadLetter},
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	// Closing the client also makes any accidentally issued backend request fail.
	f.client.Close()
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			for _, ctx := range []context.Context{context.Background(), canceled, expired} {
				require.NoError(t, operation.call(ctx))
			}
			inputs := []*queue.Message{nil, nil, nil}
			requireBatchFailures(t, operation.call(context.Background(), inputs...), inputs, map[int]error{
				0: queue.ErrInvalidArgument, 1: queue.ErrInvalidArgument, 2: queue.ErrInvalidArgument,
			})
		})
	}
	require.Zero(t, counter.calls.Load(), "empty batches and nil pointers must never be sent to Valkey")
}

func TestRetryPreservesUpdatesResetsDelayAndNeverLimitsAttempts(t *testing.T) {
	f := newTestQueue(t)
	f.freezeTime()
	ctx := context.Background()
	original := &queue.Message{
		ID: "retry", QueueName: "jobs", Namespace: "app", JobName: "work",
		Payload: []byte("before"), Extra: []byte("before-extra"),
		Delay: time.Second, VisibilityTimeout: time.Minute,
	}
	require.NoError(t, f.q.Push(ctx, original))
	requirePopEmpty(t, f.q, "jobs")
	f.advanceTime(time.Second + 50*time.Millisecond)
	first := popOne(t, f.q, "jobs")
	require.Zero(t, first.Delay)
	first.Payload = []byte{0, 255, 'n', 'e', 'w'}
	first.Extra = []byte("updated failure metadata")
	first.Delay = time.Second
	expected := copyDelivery(first)
	stale := copyDelivery(first)
	require.NoError(t, f.q.Retry(ctx, first))
	requireStats(t, f.q, queue.QueueStats{Name: "jobs", Delayed: 1})
	requireStaleDelivery(t, f.q, stale)
	requirePopEmpty(t, f.q, "jobs")
	f.advanceTime(time.Second + 50*time.Millisecond)

	current := popOne(t, f.q, "jobs")
	requireMessageFields(t, *expected, current)
	require.Equal(t, 2, current.Attempt)
	require.Zero(t, current.Delay)
	require.NotEqual(t, stale.DriverData, current.DriverData)
	// Without setting another Delay, every later Retry must be immediately visible.
	for attempt := 3; attempt <= 20; attempt++ {
		previous := copyDelivery(current)
		require.NoError(t, f.q.Retry(ctx, current))
		current = popOne(t, f.q, "jobs")
		requireMessageFields(t, *expected, current)
		require.Equal(t, attempt, current.Attempt)
		require.Zero(t, current.Delay)
		require.NotEqual(t, previous.DriverData, current.DriverData)
	}
	require.NoError(t, f.q.Ack(ctx, current))
}

func TestVisibilityExpiryRejectsStaleTokensBeforeAndAfterRedelivery(t *testing.T) {
	f := newTestQueue(t)
	f.freezeTime()
	ctx := context.Background()
	const lease = time.Second
	original := &queue.Message{
		ID: "expires", QueueName: "jobs", Payload: []byte("original"), Extra: []byte("original-extra"),
		VisibilityTimeout: lease,
	}
	require.NoError(t, f.q.Push(ctx, original))
	first := popOne(t, f.q, "jobs")
	first.Payload[0] = 'X'
	first.Extra[0] = 'X'
	firstToken := first.DriverData
	f.advanceTime(lease + 50*time.Millisecond)

	// These checks deliberately precede Stats/Pop: settlement must itself check expiry.
	requireStaleDelivery(t, f.q, first)
	requireStats(t, f.q, queue.QueueStats{Name: "jobs", Ready: 1})
	second := popOne(t, f.q, "jobs")
	require.NoError(t, f.q.ExtendVisibility(ctx, 5*time.Second, second))
	require.NotSame(t, first, second)
	requireMessageFields(t, *original, second)
	require.Equal(t, 2, second.Attempt)
	require.NotEqual(t, firstToken, second.DriverData)
	require.Equal(t, 1, first.Attempt, "redelivery must not mutate an earlier delivery")
	require.Equal(t, firstToken, first.DriverData)
	first.Payload[1] = 'Y'
	first.Extra[1] = 'Y'
	require.Equal(t, original.Payload, second.Payload)
	require.Equal(t, original.Extra, second.Extra)
	requireStaleDelivery(t, f.q, first)
	requireStats(t, f.q, queue.QueueStats{Name: "jobs", InFlight: 1})
	require.NoError(t, f.q.Ack(ctx, second))
}

func TestExtendVisibilityReplacesDeadlineAndKeepsDeliveryState(t *testing.T) {
	f := newTestQueue(t)
	f.freezeTime()
	ctx := context.Background()
	require.NoError(t, f.q.Push(ctx, &queue.Message{
		ID: "extend", QueueName: "jobs", Payload: []byte("stored"), VisibilityTimeout: time.Second,
	}))
	delivery := popOne(t, f.q, "jobs")
	token := delivery.DriverData
	delivery.Payload[0] = 'X'
	f.advanceTime(200 * time.Millisecond)
	require.NoError(t, f.q.ExtendVisibility(ctx, 3*time.Second, delivery))
	require.Equal(t, token, delivery.DriverData)
	require.Equal(t, 1, delivery.Attempt)
	require.ErrorIs(t, f.q.ExtendVisibility(ctx, 0, delivery), queue.ErrInvalidArgument)
	require.ErrorIs(t, f.q.ExtendVisibility(ctx, -time.Second, delivery), queue.ErrInvalidArgument)
	require.ErrorIs(t, f.q.ExtendVisibility(ctx, time.Second, nil), queue.ErrInvalidArgument)
	f.advanceTime(time.Second)
	requireStats(t, f.q, queue.QueueStats{Name: "jobs", InFlight: 1})
	requirePopEmpty(t, f.q, "jobs")

	// A shorter extension must shorten the lease, not add to its old deadline.
	require.NoError(t, f.q.ExtendVisibility(ctx, 300*time.Millisecond, delivery))
	f.advanceTime(350 * time.Millisecond)
	require.ErrorIs(t, f.q.ExtendVisibility(ctx, time.Second, delivery), queue.ErrNotFound)
	second := popOne(t, f.q, "jobs")
	require.Equal(t, 2, second.Attempt)
	require.Equal(t, []byte("stored"), second.Payload, "heartbeats must not persist caller mutations")
	require.NotEqual(t, token, second.DriverData)
	require.NoError(t, f.q.Ack(ctx, second))
}

func TestDefaultVisibilityTimeoutAndNonpositiveOptions(t *testing.T) {
	cases := []struct {
		name       string
		options    []valkeyqueue.Option
		visibility time.Duration
		lease      time.Duration
	}{
		{name: "default is thirty seconds", lease: 30 * time.Second},
		{name: "nil option is ignored", options: []valkeyqueue.Option{nil}, lease: 30 * time.Second},
		{name: "custom default", options: []valkeyqueue.Option{valkeyqueue.WithDefaultVisibilityTimeout(2 * time.Second)}, lease: 2 * time.Second},
		{name: "zero does not replace custom default", options: []valkeyqueue.Option{valkeyqueue.WithDefaultVisibilityTimeout(2 * time.Second), valkeyqueue.WithDefaultVisibilityTimeout(0)}, lease: 2 * time.Second},
		{name: "negative does not replace custom default", options: []valkeyqueue.Option{valkeyqueue.WithDefaultVisibilityTimeout(2 * time.Second), valkeyqueue.WithDefaultVisibilityTimeout(-time.Second)}, lease: 2 * time.Second},
		{name: "per message overrides default", options: []valkeyqueue.Option{valkeyqueue.WithDefaultVisibilityTimeout(time.Minute)}, visibility: time.Second, lease: time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newTestQueue(t, tc.options...)

			f.freezeTime()
			require.NoError(t, f.q.Push(context.Background(), &queue.Message{QueueName: "jobs", VisibilityTimeout: tc.visibility}))
			delivery := popOne(t, f.q, "jobs")
			f.advanceTime(tc.lease - time.Millisecond)
			requireStats(t, f.q, queue.QueueStats{Name: "jobs", InFlight: 1})
			f.advanceTime(2 * time.Millisecond)
			require.ErrorIs(t, f.q.ExtendVisibility(context.Background(), time.Second, delivery), queue.ErrNotFound)
			requireStats(t, f.q, queue.QueueStats{Name: "jobs", Ready: 1})
			redelivered := popOne(t, f.q, "jobs")
			require.Equal(t, 2, redelivered.Attempt)
			require.NoError(t, f.q.Ack(context.Background(), redelivered))
		})
	}
}

func TestSettledDeliveryTokensCannotAffectCurrentMessages(t *testing.T) {
	for _, operation := range []string{"Ack", "Retry", "DeadLetter"} {
		t.Run(operation, func(t *testing.T) {
			f := newTestQueue(t)
			ctx := context.Background()
			require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "same-id", QueueName: "jobs"}))
			delivery := popOne(t, f.q, "jobs")
			stale := copyDelivery(delivery)
			switch operation {
			case "Ack":
				require.NoError(t, f.q.Ack(ctx, delivery))
				// Reusing the logical ID must not resurrect the old receipt.
				require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "same-id", QueueName: "jobs"}))
			case "Retry":
				require.NoError(t, f.q.Retry(ctx, delivery))
			case "DeadLetter":
				require.NoError(t, f.q.DeadLetter(ctx, delivery))
				requireStaleDelivery(t, f.q, stale)
				n, err := f.q.Redrive(ctx, "jobs", 1)
				require.NoError(t, err)
				require.Equal(t, 1, n)
			}
			requireStaleDelivery(t, f.q, stale)
			current := popOne(t, f.q, "jobs")
			require.NotEqual(t, stale.DriverData, current.DriverData)
			requireStaleDelivery(t, f.q, stale)
			requireStats(t, f.q, queue.QueueStats{Name: "jobs", InFlight: 1})
			require.NoError(t, f.q.Ack(ctx, current))
		})
	}
}

func TestDeadLetterRedrivePreservesUpdatedOpaqueMessageState(t *testing.T) {
	f := newTestQueue(t)
	ctx := context.Background()
	original := &queue.Message{
		ID: "dead", QueueName: "jobs", Namespace: "app", JobName: "send",
		Payload: []byte("original"), Extra: []byte("original extra"), VisibilityTimeout: time.Minute + 7*time.Nanosecond,
	}
	require.NoError(t, f.q.Push(ctx, original))
	first := popOne(t, f.q, "jobs")
	require.NoError(t, f.q.Retry(ctx, first))
	second := popOne(t, f.q, "jobs")
	require.Equal(t, 2, second.Attempt)
	second.Payload = []byte{0, 255, 254, '\n'}
	second.Extra = []byte("{failure metadata is not necessarily JSON")
	second.Delay = time.Hour
	expected := copyDelivery(second)
	stale := copyDelivery(second)
	require.NoError(t, f.q.DeadLetter(ctx, second))
	requireStats(t, f.q, queue.QueueStats{Name: "jobs", Dead: 1})
	requirePopEmpty(t, f.q, "jobs")
	requireStaleDelivery(t, f.q, stale)
	names, err := f.q.Queues(ctx)
	require.NoError(t, err)
	require.Contains(t, names, "jobs", "dead-only queues must remain discoverable")
	n, err := f.q.Redrive(ctx, "another queue", 5)
	require.NoError(t, err)
	require.Zero(t, n)
	n, err = f.q.Redrive(ctx, "jobs", 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	requireStats(t, f.q, queue.QueueStats{Name: "jobs", Ready: 1})
	redriven := popOne(t, f.q, "jobs")
	requireMessageFields(t, *expected, redriven)
	require.Equal(t, 1, redriven.Attempt)
	require.Zero(t, redriven.Delay, "dead-letter Delay must not postpone redrive")
	require.NotEqual(t, stale.DriverData, redriven.DriverData)
	requireStaleDelivery(t, f.q, stale)
	require.NoError(t, f.q.Ack(ctx, redriven))
	n, err = f.q.Redrive(ctx, "jobs", 10)
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestLargeNanosecondVisibilityTimeoutsRoundTripExactly(t *testing.T) {
	// All values exceed Lua's exact-integer range for nanoseconds. Scheduling can
	// use milliseconds, but decoding/re-encoding the JSON blob in Lua loses bits.
	for _, visibility := range []time.Duration{(1 << 53) + 123456789, (1 << 62) + 987654321, (1 << 63) - 19} {
		t.Run(fmt.Sprint(int64(visibility)), func(t *testing.T) {
			f := newTestQueue(t)
			ctx := context.Background()
			original := &queue.Message{
				ID: "precise", QueueName: "jobs", Namespace: "opaque", JobName: "job",
				Payload: []byte{0, 255, 128}, Extra: []byte("metadata"), VisibilityTimeout: visibility,
			}
			require.NoError(t, f.q.Push(ctx, original))
			first := popOne(t, f.q, "jobs")
			requireMessageFields(t, *original, first)
			require.NoError(t, f.q.Retry(ctx, first))
			second := popOne(t, f.q, "jobs")
			requireMessageFields(t, *original, second)
			require.Equal(t, 2, second.Attempt)
			require.NoError(t, f.q.DeadLetter(ctx, second))
			n, err := f.q.Redrive(ctx, "jobs", 1)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			third := popOne(t, f.q, "jobs")
			requireMessageFields(t, *original, third)
			require.Equal(t, 1, third.Attempt)
			require.NoError(t, f.q.Ack(ctx, third))
		})
	}
}

func TestStatsAndDiscoveryIncludeAllRetainedStates(t *testing.T) {
	f := newTestQueue(t)
	f.freezeTime()
	ctx := context.Background()
	names, err := f.q.Queues(ctx)
	require.NoError(t, err)
	require.Empty(t, names)
	requireStats(t, f.q, queue.QueueStats{Name: "unknown:{queue}"})
	require.NoError(t, f.q.Push(ctx,
		&queue.Message{ID: "flight", QueueName: "alpha", VisibilityTimeout: 2 * time.Hour},
		&queue.Message{ID: "dead", QueueName: "alpha"},
		&queue.Message{ID: "delayed", QueueName: "alpha", Delay: time.Hour},
		&queue.Message{ID: "beta", QueueName: "beta"},
	))
	indexed := indexByID(t, popExactly(t, f.q, "alpha", 2))
	require.NoError(t, f.q.DeadLetter(ctx, indexed["dead"]))
	require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "ready", QueueName: "alpha"}))
	requireStats(t, f.q, queue.QueueStats{Name: "alpha", Ready: 1, Delayed: 1, InFlight: 1, Dead: 1})
	requireStats(t, f.q, queue.QueueStats{Name: "beta", Ready: 1})
	names, err = f.q.Queues(ctx)
	require.NoError(t, err)
	// The contract does not promise sorted names or omission of empty queues.
	require.Contains(t, names, "alpha")
	require.Contains(t, names, "beta")
	require.Equal(t, 1, countName(names, "alpha"))
	require.Equal(t, 1, countName(names, "beta"))
	f.advanceTime(time.Hour + time.Millisecond)
	requireStats(t, f.q, queue.QueueStats{Name: "alpha", Ready: 2, InFlight: 1, Dead: 1})
	require.NoError(t, f.q.Ack(ctx, indexed["flight"]))
	beta := popOne(t, f.q, "beta")
	require.NoError(t, f.q.Ack(ctx, beta))
	requireStats(t, f.q, queue.QueueStats{Name: "beta"})
}

func TestSamePrefixSharesStorageAndReceiptsBetweenIndependentClients(t *testing.T) {
	f := newTestQueue(t)
	peer := f.peer(t, f.prefix)
	ctx := context.Background()
	require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "shared", QueueName: "jobs", Payload: []byte("before")}))
	delivery := popOne(t, peer, "jobs")
	requireStats(t, f.q, queue.QueueStats{Name: "jobs", InFlight: 1})
	requirePopEmpty(t, f.q, "jobs")
	require.NoError(t, f.q.ExtendVisibility(ctx, time.Minute, delivery))
	delivery.Payload = []byte("after")
	require.NoError(t, f.q.Retry(ctx, delivery))
	retried := popOne(t, f.q, "jobs")
	require.Equal(t, []byte("after"), retried.Payload)
	require.Equal(t, 2, retried.Attempt)
	require.NoError(t, peer.DeadLetter(ctx, retried))
	requireStats(t, f.q, queue.QueueStats{Name: "jobs", Dead: 1})
	n, err := f.q.Redrive(ctx, "jobs", 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	redriven := popOne(t, peer, "jobs")
	require.Equal(t, 1, redriven.Attempt)
	require.Equal(t, []byte("after"), redriven.Payload)
	require.NoError(t, f.q.Ack(ctx, redriven))
	requireStats(t, peer, queue.QueueStats{Name: "jobs"})
}

func TestDefaultKeyPrefixSharesWithExplicitHypercubeQueuePrefix(t *testing.T) {
	server := miniredis.RunT(t)
	first := valkeyqueue.New(newValkeyClient(t, server.Addr()), valkeyqueue.WithPollingTimeout(0))
	second := valkeyqueue.New(newValkeyClient(t, server.Addr()), valkeyqueue.WithKeyPrefix("hypercube:queue"), valkeyqueue.WithPollingTimeout(0))
	require.NoError(t, first.Push(context.Background(), &queue.Message{ID: "default-prefix", QueueName: "jobs"}))
	delivery := popOne(t, second, "jobs")
	require.Equal(t, "default-prefix", delivery.ID)
	require.NoError(t, first.Ack(context.Background(), delivery))
}

func TestPrefixesAndOpaqueQueueNamesDoNotCollide(t *testing.T) {
	f := newTestQueue(t)
	ctx := context.Background()
	suffixes := []string{"tenant", "tenant:pending", "{same}:one", "{same}:two", "}unbalanced:{", "wild*[?]\\:雪"}
	names := []string{"", "jobs", "jobs:dead", "{jobs}", "a}b{c", "雪 /:*?[x]\\\x00", "ready:pending", "%7Bjobs%7D"}
	queues := make([]*valkeyqueue.Queue, len(suffixes))
	for i, suffix := range suffixes {
		queues[i] = f.peer(t, f.prefix+suffix)
		messages := make([]*queue.Message, len(names))
		for j, name := range names {
			messages[j] = &queue.Message{
				ID: fmt.Sprintf("same-id-%d", j), QueueName: name, Namespace: "{opaque}", JobName: "no validation",
				Payload: []byte(fmt.Sprintf("tenant %d / queue %d", i, j)), Extra: []byte(name),
			}
		}
		require.NoError(t, queues[i].Push(ctx, messages...))
	}
	for i, q := range queues {
		discovered, err := q.Queues(ctx)
		require.NoError(t, err)
		require.ElementsMatch(t, names, discovered)
		for j, name := range names {
			requireStats(t, q, queue.QueueStats{Name: name, Ready: 1})
			delivery := popOne(t, q, name)
			require.Equal(t, fmt.Sprintf("same-id-%d", j), delivery.ID)
			require.Equal(t, name, delivery.QueueName)
			require.Equal(t, []byte(fmt.Sprintf("tenant %d / queue %d", i, j)), delivery.Payload)
			require.Equal(t, []byte(name), delivery.Extra)
			require.NoError(t, q.Ack(ctx, delivery))
		}
	}
	requireStats(t, f.q, queue.QueueStats{Name: "jobs"})

	// Identical logical IDs in two namespaces must still have isolated leases and DLQs.
	for _, q := range queues[:2] {
		require.NoError(t, q.Push(ctx, &queue.Message{ID: "same", QueueName: "jobs"}))
	}
	one := popOne(t, queues[0], "jobs")
	two := popOne(t, queues[1], "jobs")
	requireStaleDelivery(t, queues[1], copyDelivery(one))
	requireStats(t, queues[1], queue.QueueStats{Name: "jobs", InFlight: 1})
	require.NoError(t, queues[0].DeadLetter(ctx, one))
	n, err := queues[1].Redrive(ctx, "jobs", 10)
	require.NoError(t, err)
	require.Zero(t, n)
	requireStats(t, queues[0], queue.QueueStats{Name: "jobs", Dead: 1})
	require.NoError(t, queues[1].Ack(ctx, two))
}

func TestLargeBatchesAndRedriveAllowShortResultsWithoutLosingMessages(t *testing.T) {
	f := newTestQueue(t, valkeyqueue.WithDefaultVisibilityTimeout(5*time.Minute))
	ctx := context.Background()
	const count = 300
	inputs := make([]*queue.Message, 0, count+2)
	expected := make(map[string]queue.Message, count)
	failures := make(map[int]error)
	for i := 0; i < count; i++ {
		msg := &queue.Message{
			ID: fmt.Sprintf("bulk-%03d", i), QueueName: "bulk", Namespace: "bulk app", JobName: "bulk job",
			Payload: []byte(fmt.Sprintf("payload %d", i)), Extra: []byte{byte(i), 0, 255}, VisibilityTimeout: time.Minute,
		}
		expected[msg.ID] = *msg
		if i == 0 {
			// Reserve the later duplicate before the batch, avoiding assumptions
			// about which of two identical IDs wins concurrent batch processing.
			require.NoError(t, f.q.Push(ctx, msg))
		} else {
			inputs = append(inputs, msg)
		}
		if i == 129 {
			failures[len(inputs)] = queue.ErrInvalidArgument
			inputs = append(inputs, nil)
		}
		if i == 256 {
			failures[len(inputs)] = queue.ErrAlreadyExists
			inputs = append(inputs, &queue.Message{ID: "bulk-000", QueueName: "different"})
		}
	}
	requireBatchFailures(t, f.q.Push(ctx, inputs...), inputs, failures)
	requireStats(t, f.q, queue.QueueStats{Name: "bulk", Ready: count})
	deliveries := popExactly(t, f.q, "bulk", count)
	oldTokens := make(map[string]any, count)
	for _, delivery := range deliveries {
		requireMessageFields(t, expected[delivery.ID], delivery)
		require.Equal(t, 1, delivery.Attempt)
		oldTokens[delivery.ID] = delivery.DriverData
		delivery.Delay = time.Hour
	}
	deadBatch := append([]*queue.Message(nil), deliveries[:129]...)
	deadBatch = append(deadBatch, nil)
	deadBatch = append(deadBatch, deliveries[129:]...)
	requireBatchFailures(t, f.q.DeadLetter(ctx, deadBatch...), deadBatch, map[int]error{129: queue.ErrInvalidArgument})
	requireStats(t, f.q, queue.QueueStats{Name: "bulk", Dead: count})
	requirePopEmpty(t, f.q, "bulk")

	// A Lua script may cap a transfer at 128 even when count is larger.
	// Both Pop and Redrive are "up to" APIs, so repeatedly accept short results.
	moved := 0
	for calls := 0; moved < count && calls < count; calls++ {
		n, err := f.q.Redrive(ctx, "bulk", count-moved)
		require.NoError(t, err)
		require.Greater(t, n, 0, "redrive made no progress while dead messages remain")
		require.LessOrEqual(t, n, count-moved)
		moved += n
	}
	require.Equal(t, count, moved)
	requireStats(t, f.q, queue.QueueStats{Name: "bulk", Ready: count})
	redriven := popExactly(t, f.q, "bulk", count)
	indexed := indexByID(t, redriven)
	require.Len(t, indexed, count)
	for id, delivery := range indexed {
		want, ok := expected[id]
		require.True(t, ok, "unexpected redriven ID %q", id)
		requireMessageFields(t, want, delivery)
		require.Equal(t, 1, delivery.Attempt)
		require.Zero(t, delivery.Delay)
		require.NotEqual(t, oldTokens[id], delivery.DriverData)
	}
	require.NoError(t, f.q.Ack(ctx, redriven...))
	requireStats(t, f.q, queue.QueueStats{Name: "bulk"})
	n, err := f.q.Redrive(ctx, "bulk", count)
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestConcurrentPushReservesGlobalIDAtomically(t *testing.T) {
	f := newTestQueue(t)
	const workers = 12
	queues := make([]*valkeyqueue.Queue, workers)
	for i := range queues {
		queues[i] = f.peer(t, f.prefix)
	}
	type result struct {
		msg *queue.Message
		err error
	}
	results := make(chan result, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i, q := range queues {
		wg.Add(1)
		go func(i int, q *valkeyqueue.Queue) {
			defer wg.Done()
			<-start
			msg := &queue.Message{ID: "contended", QueueName: fmt.Sprintf("queue-%d", i%2), Payload: []byte(fmt.Sprint(i))}
			results <- result{msg: msg, err: q.Push(ctx, msg)}
		}(i, q)
	}
	close(start)
	wg.Wait()
	close(results)
	successes := 0
	var winner *queue.Message
	for result := range results {
		if result.err == nil {
			successes++
			winner = result.msg
		} else {
			requireBatchFailures(t, result.err, []*queue.Message{result.msg}, map[int]error{0: queue.ErrAlreadyExists})
		}
	}
	require.Equal(t, 1, successes)
	delivery := popOne(t, f.q, winner.QueueName)
	require.Equal(t, winner.Payload, delivery.Payload)
	requirePopEmpty(t, f.q, "queue-0")
	requirePopEmpty(t, f.q, "queue-1")
	require.NoError(t, f.q.Ack(context.Background(), delivery))
}

func TestConcurrentWorkersClaimEachMessageOnlyOnce(t *testing.T) {
	f := newTestQueue(t, valkeyqueue.WithDefaultVisibilityTimeout(5*time.Minute))
	const workers, count = 8, 160
	messages := make([]*queue.Message, count)
	for i := range messages {
		messages[i] = &queue.Message{ID: fmt.Sprintf("job-%03d", i), QueueName: "jobs", Payload: []byte(fmt.Sprint(i))}
	}
	require.NoError(t, f.q.Push(context.Background(), messages...))
	queues := make([]*valkeyqueue.Queue, workers)
	for i := range queues {
		queues[i] = f.peer(t, f.prefix, valkeyqueue.WithDefaultVisibilityTimeout(5*time.Minute))
	}
	type result struct {
		messages []*queue.Message
		err      error
	}
	// Bound even a broken driver's duplicate-delivery loop without blocking a
	// worker on a full result channel before the parent can inspect the failure.
	results := make(chan result, workers*(count+1))
	start := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, q := range queues {
		wg.Add(1)
		go func(q *valkeyqueue.Queue) {
			defer wg.Done()
			<-start
			for calls := 0; calls < count; calls++ {
				batch, err := q.Pop(ctx, "jobs", 7)
				if errors.Is(err, queue.ErrEmpty) {
					results <- result{messages: batch, err: err}
					return
				}
				if err != nil || len(batch) == 0 || len(batch) > 7 {
					results <- result{messages: batch, err: fmt.Errorf("invalid worker Pop result (%d messages): %v", len(batch), err)}
					return
				}
				results <- result{messages: batch}
			}
			results <- result{err: fmt.Errorf("worker did not exhaust %d messages after %d Pop calls", count, count)}
		}(q)
	}
	close(start)
	wg.Wait()
	close(results)
	all := make([]*queue.Message, 0, count)
	emptyWorkers := 0
	for result := range results {
		if errors.Is(result.err, queue.ErrEmpty) {
			require.Nil(t, result.messages)
			emptyWorkers++
			continue
		}
		require.NoError(t, result.err)
		all = append(all, result.messages...)
	}
	require.Equal(t, workers, emptyWorkers)
	require.Len(t, all, count)
	indexed := indexByID(t, all)
	for _, input := range messages {
		delivery := indexed[input.ID]
		require.NotNil(t, delivery)
		require.Equal(t, input.Payload, delivery.Payload)
		require.Equal(t, 1, delivery.Attempt)
		require.NotNil(t, delivery.DriverData)
	}
	// Keep all receipts live until every worker has stopped, so duplicate claims
	// cannot be hidden by immediately Acking the first delivery.
	requireStats(t, f.q, queue.QueueStats{Name: "jobs", InFlight: count})
	require.NoError(t, f.q.Ack(context.Background(), all...))
}

func TestPopHonorsFullPollingTimeoutAndCapsFinalWait(t *testing.T) {
	cases := []struct {
		name     string
		timeout  time.Duration
		options  []valkeyqueue.Option
		defaults bool
	}{
		{name: "zero is immediate", timeout: 0, options: []valkeyqueue.Option{valkeyqueue.WithPollingTimeout(0), valkeyqueue.WithPollingInterval(10 * time.Second)}},
		{name: "multiple empty responses do not end polling", timeout: 180 * time.Millisecond, options: []valkeyqueue.Option{valkeyqueue.WithPollingTimeout(180 * time.Millisecond), valkeyqueue.WithPollingInterval(20 * time.Millisecond)}},
		{name: "interval longer than remaining timeout", timeout: 137*time.Millisecond + 123456*time.Nanosecond, options: []valkeyqueue.Option{valkeyqueue.WithPollingTimeout(137*time.Millisecond + 123456*time.Nanosecond), valkeyqueue.WithPollingInterval(10 * time.Second)}},
		{name: "nonpositive intervals are ignored", timeout: 160 * time.Millisecond, options: []valkeyqueue.Option{valkeyqueue.WithPollingTimeout(160 * time.Millisecond), valkeyqueue.WithPollingInterval(20 * time.Millisecond), valkeyqueue.WithPollingInterval(0), valkeyqueue.WithPollingInterval(-time.Second)}},
		{name: "default is one second", timeout: time.Second, defaults: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newTestQueue(t, tc.options...)
			if tc.defaults {
				f.q = valkeyqueue.New(f.client, valkeyqueue.WithKeyPrefix(f.prefix))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			started := time.Now()
			messages, err := f.q.Pop(ctx, "empty", 3)
			elapsed := time.Since(started)
			require.Nil(t, messages)
			require.ErrorIs(t, err, queue.ErrEmpty)
			require.GreaterOrEqual(t, elapsed, tc.timeout, "Pop must not return before the configured timeout")
			require.Less(t, elapsed, tc.timeout+1500*time.Millisecond, "the final wait must be bounded by the timeout, not a ten-second polling interval")
		})
	}
}

func TestPopWaitsForPushDelayAndLeaseExpiry(t *testing.T) {
	t.Run("another client publishes while waiting", func(t *testing.T) {
		f := newTestQueue(t, valkeyqueue.WithPollingTimeout(2*time.Second))
		publisher := f.peer(t, f.prefix)
		type result struct {
			messages []*queue.Message
			err      error
		}
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		results := make(chan result, 1)
		go func() {
			messages, err := f.q.Pop(ctx, "jobs", 10)
			results <- result{messages: messages, err: err}
		}()
		select {
		case got := <-results:
			t.Fatalf("Pop returned before any message was visible: %v, %v", got.messages, got.err)
		case <-time.After(100 * time.Millisecond):
		}
		require.NoError(t, publisher.Push(ctx, &queue.Message{ID: "wake", QueueName: "jobs"}))
		select {
		case got := <-results:
			require.NoError(t, got.err)
			require.Len(t, got.messages, 1)
			require.Equal(t, "wake", got.messages[0].ID)
			require.NoError(t, publisher.Ack(ctx, got.messages...))
		case <-time.After(1500 * time.Millisecond):
			t.Fatal("Pop did not discover a message published by another client")
		}
	})

	t.Run("delayed message becomes visible using wall-clock Redis TIME", func(t *testing.T) {
		f := newTestQueue(t, valkeyqueue.WithPollingTimeout(2*time.Second), valkeyqueue.WithPollingInterval(15*time.Millisecond))
		const delay = 400 * time.Millisecond
		started := time.Now()
		require.NoError(t, f.q.Push(context.Background(), &queue.Message{ID: "delayed", QueueName: "jobs", Delay: delay}))
		delivery := popOne(t, f.q, "jobs")
		require.GreaterOrEqual(t, time.Since(started), delay-2*time.Millisecond, "millisecond scheduling must not deliver early")
		require.Equal(t, "delayed", delivery.ID)
		require.Zero(t, delivery.Delay)
		require.NoError(t, f.q.Ack(context.Background(), delivery))
	})

	t.Run("expired lease becomes visible while polling", func(t *testing.T) {
		f := newTestQueue(t, valkeyqueue.WithPollingTimeout(2*time.Second))
		const lease = 400 * time.Millisecond
		require.NoError(t, f.q.Push(context.Background(), &queue.Message{ID: "lease", QueueName: "jobs", VisibilityTimeout: lease}))
		started := time.Now()
		first := popOne(t, f.q, "jobs")
		second := popOne(t, f.q, "jobs")
		require.GreaterOrEqual(t, time.Since(started), lease-2*time.Millisecond)
		require.Equal(t, first.ID, second.ID)
		require.Equal(t, 2, second.Attempt)
		require.NotEqual(t, first.DriverData, second.DriverData)
		require.NoError(t, f.q.Ack(context.Background(), second))
	})
}

func TestPopCancellationInterruptsLongPollingInterval(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "cancel"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			f := newTestQueue(t, valkeyqueue.WithPollingTimeout(10*time.Second), valkeyqueue.WithPollingInterval(10*time.Second))
			ctx, cancel := context.WithCancel(context.Background())
			want := context.Canceled
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 120*time.Millisecond)
				want = context.DeadlineExceeded
			} else {
				timer := time.AfterFunc(120*time.Millisecond, cancel)
				defer timer.Stop()
			}
			defer cancel()
			started := time.Now()
			messages, err := f.q.Pop(ctx, "jobs", 1)
			require.Nil(t, messages)
			require.ErrorIs(t, err, want)
			require.Less(t, time.Since(started), 1500*time.Millisecond, "context cancellation must interrupt the polling sleep")
		})
	}
}

func TestNonemptyOperationsObserveAlreadyDoneContexts(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "canceled"
		if deadline {
			name = "deadline exceeded"
		}
		t.Run(name, func(t *testing.T) {
			f := newTestQueue(t)
			require.NoError(t, f.q.Push(context.Background(), &queue.Message{ID: "live", QueueName: "jobs"}))
			live := popOne(t, f.q, "jobs")
			ctx, cancel := context.WithCancel(context.Background())
			want := context.Canceled
			if deadline {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				want = context.DeadlineExceeded
			} else {
				cancel()
			}
			defer cancel()
			require.ErrorIs(t, f.q.Push(ctx, &queue.Message{ID: "canceled-push", QueueName: "jobs"}), want)
			messages, err := f.q.Pop(ctx, "empty", 1)
			require.Nil(t, messages)
			require.ErrorIs(t, err, want)
			require.ErrorIs(t, f.q.Ack(ctx, live), want)
			require.ErrorIs(t, f.q.Retry(ctx, live), want)
			require.ErrorIs(t, f.q.DeadLetter(ctx, live), want)
			require.ErrorIs(t, f.q.ExtendVisibility(ctx, time.Second, live), want)
			_, err = f.q.Stats(ctx, "jobs")
			require.ErrorIs(t, err, want)
			_, err = f.q.Queues(ctx)
			require.ErrorIs(t, err, want)
			_, err = f.q.Redrive(ctx, "jobs", 1)
			require.ErrorIs(t, err, want)
			// Remote operations can have unknown outcomes on cancellation; do not
			// assert that a canceled call necessarily left backend state unchanged.
		})
	}
}

func TestInvalidPopAndRedriveCounts(t *testing.T) {
	f := newTestQueue(t)
	for _, count := range []int{0, -1, -128} {
		messages, err := f.q.Pop(context.Background(), "jobs", count)
		require.Nil(t, messages)
		require.ErrorIs(t, err, queue.ErrInvalidArgument)
		n, err := f.q.Redrive(context.Background(), "jobs", count)
		require.Zero(t, n)
		require.ErrorIs(t, err, queue.ErrInvalidArgument)
	}
}

func TestBackendFailuresAreNotReportedAsEmptyOrSuccessful(t *testing.T) {
	f := newTestQueue(t)
	ctx := context.Background()
	require.NoError(t, f.q.Push(ctx,
		&queue.Message{ID: "live", QueueName: "jobs"},
		&queue.Message{ID: "dead", QueueName: "jobs"},
	))
	indexed := indexByID(t, popExactly(t, f.q, "jobs", 2))
	live := indexed["live"]
	require.NoError(t, f.q.DeadLetter(ctx, indexed["dead"]))
	require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "ready", QueueName: "jobs"}))
	q := f.q
	marker := "injected queue backend failure"
	f.server.SetError("ERR " + marker)
	t.Cleanup(func() { f.server.SetError("") })
	check := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		require.False(t, errors.Is(err, queue.ErrEmpty), "infrastructure failures must not masquerade as an empty queue")
		var batch *queue.BatchError
		if errors.As(err, &batch) {
			require.NotEmpty(t, batch.Failed)
			for _, item := range batch.Failed {
				require.ErrorContains(t, item.Err, marker)
			}
		} else {
			require.ErrorContains(t, err, marker)
		}
	}
	operations := []struct {
		name string
		call func(*testing.T) error
	}{
		{"Push", func(*testing.T) error { return q.Push(ctx, &queue.Message{ID: "new", QueueName: "jobs"}) }},
		{"Ack", func(*testing.T) error { return q.Ack(ctx, live) }},
		{"Retry", func(*testing.T) error { return q.Retry(ctx, live) }},
		{"DeadLetter", func(*testing.T) error { return q.DeadLetter(ctx, live) }},
		{"ExtendVisibility", func(*testing.T) error { return q.ExtendVisibility(ctx, time.Minute, live) }},
		{"Pop empty", func(t *testing.T) error {
			messages, err := q.Pop(ctx, "unknown", 1)
			require.Nil(t, messages)
			return err
		}},
		{"Pop ready", func(t *testing.T) error {
			messages, err := q.Pop(ctx, "jobs", 1)
			require.Nil(t, messages)
			return err
		}},
		{"Stats", func(*testing.T) error { _, err := q.Stats(ctx, "jobs"); return err }},
		{"Queues", func(*testing.T) error { _, err := q.Queues(ctx); return err }},
		{"Redrive", func(t *testing.T) error {
			n, err := q.Redrive(ctx, "jobs", 1)
			require.Zero(t, n)
			return err
		}},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) { check(t, operation.call(t)) })
	}
	f.server.SetError("")
	requireStats(t, f.q, queue.QueueStats{Name: "jobs", Ready: 1, InFlight: 1, Dead: 1})
}

// Every test uses its own miniredis test double, never an external Redis/Valkey server.
func newTestQueue(t *testing.T, options ...valkeyqueue.Option) *testQueue {
	t.Helper()
	f := &testQueue{server: miniredis.RunT(t)}
	f.addr = f.server.Addr()
	var random [16]byte
	_, err := rand.Read(random[:])
	require.NoError(t, err)
	f.prefix = "hypercube:queue:test:" + hex.EncodeToString(random[:]) + ":"
	f.client = newValkeyClient(t, f.addr)
	settings := []valkeyqueue.Option{valkeyqueue.WithKeyPrefix(f.prefix), valkeyqueue.WithPollingTimeout(0)}
	settings = append(settings, options...)
	f.q = valkeyqueue.New(f.client, settings...)
	return f
}

type testQueue struct {
	q      *valkeyqueue.Queue
	client valkey.Client
	server *miniredis.Miniredis
	addr   string
	prefix string
	clock  time.Time
}

func (f *testQueue) peer(t *testing.T, prefix string, options ...valkeyqueue.Option) *valkeyqueue.Queue {
	t.Helper()
	settings := []valkeyqueue.Option{valkeyqueue.WithKeyPrefix(prefix), valkeyqueue.WithPollingTimeout(0)}
	settings = append(settings, options...)
	return valkeyqueue.New(newValkeyClient(t, f.addr), settings...)
}

func valkeyClientOptions(addr string) valkey.ClientOption {
	return valkey.ClientOption{
		InitAddress:       []string{addr},
		ForceSingleClient: true,
		DisableCache:      true,
		DisableRetry:      true,
		Dialer:            net.Dialer{Timeout: time.Second},
		ConnWriteTimeout:  3 * time.Second,
	}
}

func newValkeyClient(t *testing.T, addr string) valkey.Client {
	t.Helper()
	return newValkeyClientWithOptions(t, valkeyClientOptions(addr))
}

func newValkeyClientWithOptions(t *testing.T, options valkey.ClientOption) valkey.Client {
	t.Helper()
	client, err := valkey.NewClient(options)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, client.Do(ctx, client.B().Ping().Build()).Error(), "miniredis test backend must be reachable")
	return &miniredisClient{Client: client}
}

func (f *testQueue) freezeTime() {
	f.clock = time.Unix(1700000000, 0)
	f.server.SetTime(f.clock)
}

func (f *testQueue) advanceTime(duration time.Duration) {
	if !f.clock.IsZero() {
		f.clock = f.clock.Add(duration)
		f.server.SetTime(f.clock)
		return
	}
	time.Sleep(duration)
}

func popOne(t *testing.T, q queue.Queue, name string) *queue.Message {
	t.Helper()
	return popExactly(t, q, name, 1)[0]
}

func popExactly(t *testing.T, q queue.Queue, name string, count int) []*queue.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	messages := make([]*queue.Message, 0, count)
	for len(messages) < count {
		remaining := count - len(messages)
		batch, err := q.Pop(ctx, name, remaining)
		require.NoError(t, err)
		require.NotEmpty(t, batch, "Pop must not return an empty successful result")
		require.LessOrEqual(t, len(batch), remaining)
		for _, message := range batch {
			require.NotNil(t, message)
		}
		messages = append(messages, batch...)
	}
	return messages
}

func requirePopEmpty(t *testing.T, q queue.Queue, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	messages, err := q.Pop(ctx, name, 1)
	require.Nil(t, messages)
	require.ErrorIs(t, err, queue.ErrEmpty)
}

func requireStats(t *testing.T, q queue.StatsProvider, expected queue.QueueStats) {
	t.Helper()
	actual, err := q.Stats(context.Background(), expected.Name)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func requireBatchFailures(t *testing.T, err error, inputs []*queue.Message, expected map[int]error) {
	t.Helper()
	require.Error(t, err)
	var batch *queue.BatchError
	require.ErrorAs(t, err, &batch)
	require.Len(t, batch.Failed, len(expected))
	seen := make(map[int]bool, len(expected))
	for _, failure := range batch.Failed {
		want, ok := expected[failure.Index]
		require.True(t, ok, "unexpected failure index %d", failure.Index)
		require.False(t, seen[failure.Index], "duplicate failure index %d", failure.Index)
		seen[failure.Index] = true
		require.True(t, failure.Msg == inputs[failure.Index], "failure must retain the original pointer at input index %d", failure.Index)
		require.ErrorIs(t, failure.Err, want)
	}
}

func requireStaleDelivery(t *testing.T, q *valkeyqueue.Queue, stale *queue.Message) {
	t.Helper()
	ctx := context.Background()
	inputs := []*queue.Message{stale}
	requireBatchFailures(t, q.Ack(ctx, stale), inputs, map[int]error{0: queue.ErrNotFound})
	requireBatchFailures(t, q.Retry(ctx, stale), inputs, map[int]error{0: queue.ErrNotFound})
	requireBatchFailures(t, q.DeadLetter(ctx, stale), inputs, map[int]error{0: queue.ErrNotFound})
	require.ErrorIs(t, q.ExtendVisibility(ctx, time.Second, stale), queue.ErrNotFound)
}

func requireMessageFields(t *testing.T, expected queue.Message, actual *queue.Message) {
	t.Helper()
	require.NotNil(t, actual)
	require.Equal(t, expected.ID, actual.ID)
	require.Equal(t, expected.QueueName, actual.QueueName)
	require.Equal(t, expected.Namespace, actual.Namespace)
	require.Equal(t, expected.JobName, actual.JobName)
	require.Equal(t, expected.Payload, actual.Payload)
	require.Equal(t, expected.Extra, actual.Extra)
	require.Equal(t, expected.VisibilityTimeout, actual.VisibilityTimeout)
}

// Save an independent pre-settlement probe instead of reading a delivery after
// successful Ack/Retry/DeadLetter has transferred its ownership back to the driver.
// DriverData is kept opaque: tests never assume a concrete receipt representation.
func copyDelivery(message *queue.Message) *queue.Message {
	copy := *message
	copy.Payload = bytes.Clone(message.Payload)
	copy.Extra = bytes.Clone(message.Extra)
	return &copy
}

func indexByID(t *testing.T, messages []*queue.Message) map[string]*queue.Message {
	t.Helper()
	indexed := make(map[string]*queue.Message, len(messages))
	for _, message := range messages {
		require.NotNil(t, message)
		_, exists := indexed[message.ID]
		require.False(t, exists, "logical message %q was claimed twice", message.ID)
		indexed[message.ID] = message
	}
	return indexed
}

func countName(names []string, name string) int {
	count := 0
	for _, candidate := range names {
		if candidate == name {
			count++
		}
	}
	return count
}

type commandCounter struct {
	valkey.Client
	calls atomic.Int64
}

func (counter *commandCounter) Do(ctx context.Context, command valkey.Completed) valkey.ValkeyResult {
	counter.calls.Add(1)
	return counter.Client.Do(ctx, command)
}

func (counter *commandCounter) DoMulti(ctx context.Context, commands ...valkey.Completed) []valkey.ValkeyResult {
	counter.calls.Add(int64(len(commands)))
	return counter.Client.DoMulti(ctx, commands...)
}
