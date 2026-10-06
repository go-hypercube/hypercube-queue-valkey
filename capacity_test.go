package valkeyqueue_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/server"
	"github.com/go-hypercube/go-hypercube/queue"
	valkeyqueue "github.com/go-hypercube/hypercube-queue-valkey"
	"github.com/stretchr/testify/require"
)

func TestCapacityDefaultsAndNonpositiveOptionsCannotDisableSafeguards(t *testing.T) {
	require.Equal(t, 7*24*time.Hour, valkeyqueue.DefaultDeadLetterRetention)
	require.Equal(t, 100_000, valkeyqueue.DefaultMaxRetainedMessages)
	require.Equal(t, 1<<20, valkeyqueue.DefaultMaxMessageBytes)
	require.EqualValues(t, 64<<20, valkeyqueue.DefaultMaxRetainedBytes)
	require.Equal(t, time.Minute, valkeyqueue.DefaultMaintenanceInterval)

	f := newTestQueue(t,
		valkeyqueue.WithDeadLetterRetention(0), valkeyqueue.WithDeadLetterRetention(-time.Second),
		valkeyqueue.WithMaxRetainedMessages(0), valkeyqueue.WithMaxRetainedMessages(-1),
		valkeyqueue.WithMaxMessageBytes(0), valkeyqueue.WithMaxMessageBytes(-1),
		valkeyqueue.WithMaxRetainedBytes(0), valkeyqueue.WithMaxRetainedBytes(-1),
	)
	f.freezeTime()
	ctx := context.Background()
	msg := &queue.Message{ID: "default-retention", QueueName: "jobs"}
	require.NoError(t, f.q.Push(ctx, msg))
	peer := f.peer(t, f.prefix,
		valkeyqueue.WithDeadLetterRetention(valkeyqueue.DefaultDeadLetterRetention),
		valkeyqueue.WithMaxRetainedMessages(valkeyqueue.DefaultMaxRetainedMessages),
		valkeyqueue.WithMaxMessageBytes(valkeyqueue.DefaultMaxMessageBytes),
		valkeyqueue.WithMaxRetainedBytes(valkeyqueue.DefaultMaxRetainedBytes),
	)
	requireStats(t, peer, queue.QueueStats{Name: "jobs", Ready: 1})
	tooBig := &queue.Message{ID: "too-big", QueueName: "jobs", Payload: make([]byte, valkeyqueue.DefaultMaxMessageBytes+1)}
	requireBatchFailures(t, f.q.Push(ctx, tooBig), []*queue.Message{tooBig}, map[int]error{0: valkeyqueue.ErrMessageTooLarge})
	require.NoError(t, peer.DeadLetter(ctx, popOne(t, peer, "jobs")))
	f.advanceTime(valkeyqueue.DefaultDeadLetterRetention - time.Millisecond)
	requireStats(t, f.q, queue.QueueStats{Name: "jobs", Dead: 1})
	n, err := f.q.PruneDeadLetters(ctx)
	require.NoError(t, err)
	require.Zero(t, n)
	f.advanceTime(time.Millisecond)
	n, err = peer.PruneDeadLetters(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n, "the absolute retention deadline is inclusive")
}

func TestCapacityNonpositiveOptionsPreserveEarlierPositiveSettings(t *testing.T) {
	options := []valkeyqueue.Option{
		valkeyqueue.WithDeadLetterRetention(time.Second),
		valkeyqueue.WithMaxRetainedMessages(1),
		valkeyqueue.WithMaxMessageBytes(1024),
		valkeyqueue.WithMaxRetainedBytes(2048),
	}
	ignored := []valkeyqueue.Option{
		valkeyqueue.WithDeadLetterRetention(0), valkeyqueue.WithDeadLetterRetention(-time.Second),
		valkeyqueue.WithMaxRetainedMessages(0), valkeyqueue.WithMaxRetainedMessages(-1),
		valkeyqueue.WithMaxMessageBytes(0), valkeyqueue.WithMaxMessageBytes(-1),
		valkeyqueue.WithMaxRetainedBytes(0), valkeyqueue.WithMaxRetainedBytes(-1),
	}
	f := newTestQueue(t, append(options, ignored...)...)
	f.freezeTime()
	ctx := context.Background()
	require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "retained", QueueName: "jobs"}))
	peer := f.peer(t, f.prefix, options...)
	requireStats(t, peer, queue.QueueStats{Name: "jobs", Ready: 1})
	blocked := &queue.Message{ID: "blocked", QueueName: "other"}
	requireBatchFailures(t, peer.Push(ctx, blocked), []*queue.Message{blocked}, map[int]error{0: valkeyqueue.ErrCapacityExceeded})
	require.NoError(t, peer.DeadLetter(ctx, popOne(t, peer, "jobs")))
	f.advanceTime(time.Second)
	n, err := f.q.PruneDeadLetters(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, f.q.Push(ctx, blocked))
}

func TestCapacityConstructionIsLazyAndFirstOperationEstablishesPolicy(t *testing.T) {
	f := newTestQueue(t, valkeyqueue.WithMaxRetainedMessages(1))
	var commands atomic.Int64
	f.server.Server().SetPreHook(func(_ *server.Peer, _ string, _ ...string) bool {
		commands.Add(1)
		return false
	})
	t.Cleanup(func() { f.server.Server().SetPreHook(nil) })
	peer := valkeyqueue.New(f.client, valkeyqueue.WithKeyPrefix(f.prefix), valkeyqueue.WithMaxRetainedMessages(2))
	time.Sleep(30 * time.Millisecond)
	require.Zero(t, commands.Load(), "New must neither contact the backend nor start maintenance")
	require.Empty(t, f.server.Keys())
	require.NoError(t, f.q.Push(context.Background()))
	requireBatchFailures(t, f.q.Push(context.Background(), nil), []*queue.Message{nil}, map[int]error{0: queue.ErrInvalidArgument})
	require.Empty(t, f.server.Keys(), "empty and nil-only batches must not establish policy")
	// A read-only-looking queue operation is nevertheless a real operation.
	requireStats(t, peer, queue.QueueStats{Name: "unseen"})
	_, err := f.q.Stats(context.Background(), "unseen")
	require.ErrorIs(t, err, valkeyqueue.ErrConfigurationMismatch)
	require.NoError(t, peer.Push(context.Background(), &queue.Message{QueueName: "jobs"}))
}

func TestCapacityPersistedPolicyRejectsEveryChangedSafeguard(t *testing.T) {
	f := newTestQueue(t,
		valkeyqueue.WithDeadLetterRetention(time.Second), valkeyqueue.WithMaxRetainedMessages(10),
		valkeyqueue.WithMaxMessageBytes(1024), valkeyqueue.WithMaxRetainedBytes(4096),
	)
	ctx := context.Background()
	require.NoError(t, f.q.Push(ctx, &queue.Message{QueueName: "jobs"}))
	for _, change := range []struct {
		name   string
		option valkeyqueue.Option
	}{
		{"retention", valkeyqueue.WithDeadLetterRetention(2 * time.Second)},
		{"count", valkeyqueue.WithMaxRetainedMessages(11)},
		{"message-bytes", valkeyqueue.WithMaxMessageBytes(1025)},
		{"retained-bytes", valkeyqueue.WithMaxRetainedBytes(4097)},
	} {
		t.Run(change.name, func(t *testing.T) {
			peer := f.peer(t, f.prefix,
				valkeyqueue.WithDeadLetterRetention(time.Second), valkeyqueue.WithMaxRetainedMessages(10),
				valkeyqueue.WithMaxMessageBytes(1024), valkeyqueue.WithMaxRetainedBytes(4096), change.option,
			)
			_, err := peer.Stats(ctx, "jobs")
			require.ErrorIs(t, err, valkeyqueue.ErrConfigurationMismatch)
		})
	}
	delivery := popOne(t, f.q, "jobs")
	require.NoError(t, f.q.Ack(ctx, delivery))
	wrong := f.peer(t, f.prefix)
	_, err := wrong.Stats(ctx, "jobs")
	require.ErrorIs(t, err, valkeyqueue.ErrConfigurationMismatch, "emptying a prefix must not reset its policy")
}

func TestCapacityConfigurationMismatchCannotBypassAnyOperation(t *testing.T) {
	f := newTestQueue(t, valkeyqueue.WithMaxRetainedMessages(10))
	ctx := context.Background()
	require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "active", QueueName: "jobs"}, &queue.Message{ID: "dead", QueueName: "jobs"}))
	index := indexByID(t, popExactly(t, f.q, "jobs", 2))
	require.NoError(t, f.q.DeadLetter(ctx, index["dead"]))
	wrong := f.peer(t, f.prefix, valkeyqueue.WithMaxRetainedMessages(11))
	calls := []struct {
		name string
		call func() error
	}{
		{"Push", func() error { return wrong.Push(ctx, &queue.Message{ID: "new", QueueName: "jobs"}) }},
		{"Pop", func() error { _, err := wrong.Pop(ctx, "jobs", 1); return err }},
		{"Ack", func() error { return wrong.Ack(ctx, index["active"]) }},
		{"Retry", func() error { return wrong.Retry(ctx, index["active"]) }},
		{"DeadLetter", func() error { return wrong.DeadLetter(ctx, index["active"]) }},
		{"ExtendVisibility", func() error { return wrong.ExtendVisibility(ctx, time.Minute, index["active"]) }},
		{"Stats", func() error { _, err := wrong.Stats(ctx, "jobs"); return err }},
		{"Queues", func() error { _, err := wrong.Queues(ctx); return err }},
		{"Redrive", func() error { _, err := wrong.Redrive(ctx, "jobs", 1); return err }},
		{"PruneDeadLetters", func() error { _, err := wrong.PruneDeadLetters(ctx); return err }},
		{"RunMaintenance", func() error {
			bounded, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			return wrong.RunMaintenance(bounded, 10*time.Millisecond)
		}},
	}
	for _, call := range calls {
		t.Run(call.name, func(t *testing.T) {
			requireCapacityOperationError(t, call.call(), valkeyqueue.ErrConfigurationMismatch)
		})
	}
	requireStats(t, f.q, queue.QueueStats{Name: "jobs", InFlight: 1, Dead: 1})
}

func TestCapacityPollingAndDefaultVisibilityAreNotPersistedPolicy(t *testing.T) {
	f := newTestQueue(t, valkeyqueue.WithMaxRetainedMessages(2))
	f.freezeTime()
	ctx := context.Background()
	require.NoError(t, f.q.Push(ctx, &queue.Message{QueueName: "jobs"}))
	peer := f.peer(t, f.prefix, valkeyqueue.WithMaxRetainedMessages(2),
		valkeyqueue.WithPollingTimeout(15*time.Millisecond), valkeyqueue.WithPollingInterval(time.Millisecond),
		valkeyqueue.WithDefaultVisibilityTimeout(time.Hour),
	)
	delivery := popOne(t, peer, "jobs")
	f.advanceTime(valkeyqueue.DefaultVisibilityTimeout + time.Second)
	requireStats(t, f.q, queue.QueueStats{Name: "jobs", InFlight: 1})
	require.NoError(t, f.q.Ack(ctx, delivery))
}

func TestCapacityConcurrentFirstOperationsSelectExactlyOnePolicy(t *testing.T) {
	f := newTestQueue(t, valkeyqueue.WithMaxRetainedMessages(1))
	peer := f.peer(t, f.prefix, valkeyqueue.WithMaxRetainedMessages(2))
	clients := []*valkeyqueue.Queue{f.q, peer}
	start := make(chan struct{})
	results := make(chan struct {
		index int
		err   error
	}, 2)
	for i, q := range clients {
		go func(i int, q *valkeyqueue.Queue) {
			<-start
			results <- struct {
				index int
				err   error
			}{i, q.Push(context.Background(), &queue.Message{ID: fmt.Sprintf("first-%d", i), QueueName: "jobs"})}
		}(i, q)
	}
	close(start)
	winner := -1
	for range clients {
		select {
		case result := <-results:
			if result.err == nil {
				require.Equal(t, -1, winner, "incompatible policies must not both win initialization")
				winner = result.index
			} else {
				requireCapacityOperationError(t, result.err, valkeyqueue.ErrConfigurationMismatch)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent policy initialization did not finish")
		}
	}
	require.NotEqual(t, -1, winner)
	requireStats(t, clients[winner], queue.QueueStats{Name: "jobs", Ready: 1})
	_, err := clients[1-winner].Stats(context.Background(), "jobs")
	require.ErrorIs(t, err, valkeyqueue.ErrConfigurationMismatch)
}

func TestCapacityCountsEveryRetainedStateGloballyAndNeverExpiresActiveWork(t *testing.T) {
	f := newTestQueue(t, valkeyqueue.WithMaxRetainedMessages(4), valkeyqueue.WithDeadLetterRetention(time.Second))
	f.freezeTime()
	ctx := context.Background()
	require.NoError(t, f.q.Push(ctx,
		&queue.Message{ID: "ready", QueueName: "ready"},
		&queue.Message{ID: "delayed", QueueName: "delayed", Delay: time.Hour},
		&queue.Message{ID: "inflight", QueueName: "inflight", VisibilityTimeout: time.Hour},
		&queue.Message{ID: "dead", QueueName: "dead"},
	))
	active := popOne(t, f.q, "inflight")
	require.NoError(t, f.q.DeadLetter(ctx, popOne(t, f.q, "dead")))
	peer := f.peer(t, f.prefix, valkeyqueue.WithMaxRetainedMessages(4), valkeyqueue.WithDeadLetterRetention(time.Second))
	blocked := &queue.Message{ID: "extra", QueueName: "another"}
	requireBatchFailures(t, peer.Push(ctx, blocked), []*queue.Message{blocked}, map[int]error{0: valkeyqueue.ErrCapacityExceeded})
	f.advanceTime(time.Second)
	n, err := peer.PruneDeadLetters(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	requireStats(t, peer, queue.QueueStats{Name: "ready", Ready: 1})
	requireStats(t, peer, queue.QueueStats{Name: "delayed", Delayed: 1})
	requireStats(t, peer, queue.QueueStats{Name: "inflight", InFlight: 1})
	require.NoError(t, peer.Push(ctx, blocked))
	active.Payload = make([]byte, 2*valkeyqueue.DefaultMaxMessageBytes)
	require.NoError(t, peer.Ack(ctx, active), "Ack discards caller data and must always be able to free capacity")
	require.NoError(t, peer.Push(ctx, &queue.Message{ID: "inflight", QueueName: "reused"}))
}

func TestCapacityPrefixesHaveIndependentPoliciesBudgetsAndExpiry(t *testing.T) {
	f := newTestQueue(t, valkeyqueue.WithMaxRetainedMessages(1), valkeyqueue.WithDeadLetterRetention(time.Second))
	f.freezeTime()
	other := f.peer(t, f.prefix+"other:{opaque}\xff", valkeyqueue.WithMaxRetainedMessages(2), valkeyqueue.WithDeadLetterRetention(2*time.Second))
	ctx := context.Background()
	for _, q := range []*valkeyqueue.Queue{f.q, other} {
		require.NoError(t, q.Push(ctx, &queue.Message{ID: "same-id", QueueName: "jobs"}))
		require.NoError(t, q.DeadLetter(ctx, popOne(t, q, "jobs")))
	}
	f.advanceTime(time.Second)
	n, err := f.q.PruneDeadLetters(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	requireStats(t, other, queue.QueueStats{Name: "jobs", Dead: 1})
	require.NoError(t, other.Push(ctx, &queue.Message{ID: "second", QueueName: "jobs"}))
	require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "same-id", QueueName: "jobs"}))
}

func TestCapacityConcurrentReservationsAcrossQueuesAndClients(t *testing.T) {
	for _, budget := range []string{"messages", "bytes"} {
		t.Run(budget, func(t *testing.T) {
			const workers, slots = 24, 5
			prototype := &queue.Message{ID: "id-00", QueueName: "jobs-0", Payload: bytes.Repeat([]byte("x"), 128)}
			size := capacityEncodedSize(t, prototype)
			options := []valkeyqueue.Option{valkeyqueue.WithMaxRetainedMessages(slots)}
			if budget == "bytes" {
				options = []valkeyqueue.Option{valkeyqueue.WithMaxRetainedMessages(workers), valkeyqueue.WithMaxRetainedBytes(int64(slots * size))}
			}
			f := newTestQueue(t, options...)
			peer := f.peer(t, f.prefix, options...)
			start := make(chan struct{})
			type result struct {
				msg *queue.Message
				err error
			}
			results := make(chan result, workers)
			var wg sync.WaitGroup
			for i := range workers {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					msg := &queue.Message{ID: fmt.Sprintf("id-%02d", i), QueueName: fmt.Sprintf("jobs-%d", i%3), Payload: bytes.Clone(prototype.Payload)}
					q := f.q
					if i%2 != 0 {
						q = peer
					}
					<-start
					results <- result{msg, q.Push(context.Background(), msg)}
				}(i)
			}
			close(start)
			wg.Wait()
			close(results)
			winners := make(map[string]*queue.Message)
			for result := range results {
				if result.err == nil {
					winners[result.msg.ID] = result.msg
				} else {
					requireBatchFailures(t, result.err, []*queue.Message{result.msg}, map[int]error{0: valkeyqueue.ErrCapacityExceeded})
				}
			}
			require.Len(t, winners, slots)
			require.Equal(t, int64(slots*size), capacityStoredBytes(t, f))
			for i := range 3 {
				name := fmt.Sprintf("jobs-%d", i)
				count := 0
				for _, msg := range winners {
					if msg.QueueName == name {
						count++
					}
				}
				requireStats(t, peer, queue.QueueStats{Name: name, Ready: int64(count)})
				if count != 0 {
					deliveries := popExactly(t, peer, name, count)
					for _, delivery := range deliveries {
						require.Contains(t, winners, delivery.ID)
					}
					require.NoError(t, f.q.Ack(context.Background(), deliveries...))
				}
			}
			require.Zero(t, capacityStoredBytes(t, f))
			require.NoError(t, peer.Push(context.Background(), prototype), "all reservations must be reusable after Ack")
		})
	}
}

func TestCapacityMessageLimitUsesEntireEncodedBlobAndIncludesExactBoundary(t *testing.T) {
	for _, field := range []string{"Payload", "Extra", "Namespace", "JobName", "QueueName", "ID"} {
		t.Run(field, func(t *testing.T) {
			msg := &queue.Message{ID: "encoded-size", QueueName: "jobs"}
			large := bytes.Repeat([]byte{0xff, 0, ':'}, 256)
			switch field {
			case "Payload":
				msg.Payload = large
			case "Extra":
				msg.Extra = large
			case "Namespace":
				msg.Namespace = string(large)
			case "JobName":
				msg.JobName = string(large)
			case "QueueName":
				msg.QueueName = string(large)
			case "ID":
				msg.ID = string(large)
			}
			size := capacityEncodedSize(t, msg)
			require.Greater(t, size, len(large), "the encoded budget includes JSON and base64 overhead")
			for _, offset := range []int{0, -1} {
				t.Run(fmt.Sprint(offset), func(t *testing.T) {
					f := newTestQueue(t, valkeyqueue.WithMaxMessageBytes(size+offset))
					input := copyDelivery(msg)
					err := f.q.Push(context.Background(), input)
					if offset == 0 {
						require.NoError(t, err)
						require.Len(t, capacityBlob(t, f, input.ID), size)
					} else {
						requireBatchFailures(t, err, []*queue.Message{input}, map[int]error{0: valkeyqueue.ErrMessageTooLarge})
						require.Zero(t, capacityStoredBytes(t, f))
						// A size failure must not reserve the logical ID.
						replacement := &queue.Message{ID: input.ID, QueueName: "jobs"}
						if field == "ID" {
							// Keep the opaque ID but eliminate enough other encoded data.
							replacement.QueueName = "q"
						}
						require.NoError(t, f.q.Push(context.Background(), replacement))
					}
				})
			}
		})
	}
}

func TestCapacityByteBudgetIsSumOfBlobsNotRawPayloadOrIndexOverhead(t *testing.T) {
	prototype := &queue.Message{ID: "byte-a", QueueName: "jobs-a", Payload: bytes.Repeat([]byte("x"), 128)}
	size := capacityEncodedSize(t, prototype)
	for _, offset := range []int64{0, -1} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			budget := int64(2*size) + offset
			require.Less(t, int64(2*len(prototype.Payload)), budget)
			f := newTestQueue(t, valkeyqueue.WithMaxRetainedBytes(budget))
			ctx := context.Background()
			first := copyDelivery(prototype)
			second := copyDelivery(prototype)
			second.ID, second.QueueName = "byte-b", "jobs-b"
			require.NoError(t, f.q.Push(ctx, first))
			err := f.q.Push(ctx, second)
			if offset == 0 {
				require.NoError(t, err, "exact blob budget fits despite additional hashes and indexes")
				require.Equal(t, budget, capacityStoredBytes(t, f))
			} else {
				requireBatchFailures(t, err, []*queue.Message{second}, map[int]error{0: valkeyqueue.ErrCapacityExceeded})
				require.Equal(t, int64(size), capacityStoredBytes(t, f))
				require.NoError(t, f.q.Ack(ctx, popOne(t, f.q, first.QueueName)))
				require.NoError(t, f.q.Push(ctx, second))
			}
		})
	}
}

func TestCapacityPushMixedBatchKeepsOriginalIndicesAndErrorPrecedence(t *testing.T) {
	f := newTestQueue(t, valkeyqueue.WithMaxRetainedMessages(2), valkeyqueue.WithMaxMessageBytes(1024))
	ctx := context.Background()
	require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "duplicate", QueueName: "jobs", Payload: []byte("original")}))
	oversized := bytes.Repeat([]byte("x"), 4096)
	accepted := &queue.Message{ID: "accepted", QueueName: "jobs", Payload: []byte("snapshot"), DriverData: make(chan struct{})}
	inputs := []*queue.Message{
		nil,
		{ID: "oversized", QueueName: "jobs", Payload: oversized},
		{ID: "duplicate", QueueName: "other", Payload: oversized},
		accepted,
		nil,
	}
	requireBatchFailures(t, f.q.Push(ctx, inputs...), inputs, map[int]error{
		0: queue.ErrInvalidArgument, 1: valkeyqueue.ErrMessageTooLarge, 2: queue.ErrAlreadyExists, 4: queue.ErrInvalidArgument,
	})
	accepted.Payload[0] = 'X'
	accepted.Namespace = "caller changed"
	blocked := []*queue.Message{
		{ID: "capacity", QueueName: "third"},
		{ID: "duplicate", QueueName: "third", Payload: oversized},
	}
	requireBatchFailures(t, f.q.Push(ctx, blocked...), blocked, map[int]error{0: valkeyqueue.ErrCapacityExceeded, 1: queue.ErrAlreadyExists})
	index := indexByID(t, popExactly(t, f.q, "jobs", 2))
	require.Equal(t, []byte("snapshot"), index["accepted"].Payload)
	require.Empty(t, index["accepted"].Namespace)
	require.Equal(t, []byte("original"), index["duplicate"].Payload)
	requireStats(t, f.q, queue.QueueStats{Name: "other"})
	require.NoError(t, f.q.Ack(ctx, index["accepted"], index["duplicate"]))
	require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "oversized", QueueName: "jobs"}))
}

func TestCapacitySettlementMixedBatchPreservesFailedItemsAndStaleErrorPrecedence(t *testing.T) {
	for _, operation := range []string{"Retry", "DeadLetter"} {
		t.Run(operation, func(t *testing.T) {
			f := newTestQueue(t, valkeyqueue.WithMaxMessageBytes(1024))
			f.freezeTime()
			ctx := context.Background()
			require.NoError(t, f.q.Push(ctx,
				&queue.Message{ID: "growth", QueueName: "jobs", Payload: []byte("stored")},
				&queue.Message{ID: "stale", QueueName: "jobs", VisibilityTimeout: time.Millisecond},
				&queue.Message{ID: "success", QueueName: "jobs"},
			))
			index := indexByID(t, popExactly(t, f.q, "jobs", 3))
			f.advanceTime(time.Millisecond)
			current := popOne(t, f.q, "jobs")
			require.Equal(t, "stale", current.ID)
			before := capacityBlob(t, f, "growth")
			index["growth"].Payload = bytes.Repeat([]byte("x"), 4096)
			index["stale"].Payload = bytes.Repeat([]byte("y"), 4096)
			index["success"].Payload = []byte("updated")
			inputs := []*queue.Message{nil, index["growth"], index["stale"], index["success"], nil}
			settle := f.q.Retry
			if operation == "DeadLetter" {
				settle = f.q.DeadLetter
			}
			requireBatchFailures(t, settle(ctx, inputs...), inputs, map[int]error{
				0: queue.ErrInvalidArgument, 1: valkeyqueue.ErrMessageTooLarge, 2: queue.ErrNotFound, 4: queue.ErrInvalidArgument,
			})
			require.Equal(t, before, capacityBlob(t, f, "growth"))
			require.Len(t, index["growth"].Payload, 4096, "failure leaves the caller's item owned and unchanged")
			require.NoError(t, f.q.ExtendVisibility(ctx, time.Minute, index["growth"]), "failed growth must not settle its receipt")
			if operation == "DeadLetter" {
				n, err := f.q.Redrive(ctx, "jobs", 1)
				require.NoError(t, err)
				require.Equal(t, 1, n)
			}
			success := popOne(t, f.q, "jobs")
			require.Equal(t, "success", success.ID)
			require.Equal(t, []byte("updated"), success.Payload)
			require.NoError(t, f.q.Ack(ctx, index["growth"], current, success))
		})
	}
}

func TestCapacityGrowthFailureLeavesSnapshotAndDeliveryIntactForBothBudgets(t *testing.T) {
	for _, operation := range []string{"Retry", "DeadLetter"} {
		for _, budget := range []string{"message", "global-bytes"} {
			t.Run(operation+"/"+budget, func(t *testing.T) {
				original := &queue.Message{ID: "item-a", QueueName: "jobs", Payload: []byte("original")}
				grown := copyDelivery(original)
				grown.Payload = bytes.Repeat([]byte("x"), 4096)
				baseSize, grownSize := capacityEncodedSize(t, original), capacityEncodedSize(t, grown)
				options := []valkeyqueue.Option{valkeyqueue.WithMaxRetainedMessages(2)}
				want := valkeyqueue.ErrCapacityExceeded
				if budget == "message" {
					options = append(options, valkeyqueue.WithMaxMessageBytes(grownSize-1))
					want = valkeyqueue.ErrMessageTooLarge
				} else {
					options = append(options, valkeyqueue.WithMaxRetainedBytes(int64(baseSize+grownSize-1)))
				}
				f := newTestQueue(t, options...)
				f.freezeTime()
				ctx := context.Background()
				other := copyDelivery(original)
				other.ID = "item-b"
				require.NoError(t, f.q.Push(ctx, original, other))
				index := indexByID(t, popExactly(t, f.q, "jobs", 2))
				before := capacityBlob(t, f, original.ID)
				failed := index[original.ID]
				failed.Payload = bytes.Clone(grown.Payload)
				settle := f.q.Retry
				if operation == "DeadLetter" {
					settle = f.q.DeadLetter
				}
				requireBatchFailures(t, settle(ctx, failed), []*queue.Message{failed}, map[int]error{0: want})
				require.Equal(t, before, capacityBlob(t, f, original.ID))
				require.Equal(t, int64(2*baseSize), capacityStoredBytes(t, f))
				requireStats(t, f.q, queue.QueueStats{Name: "jobs", InFlight: 2})
				f.advanceTime(valkeyqueue.DefaultVisibilityTimeout)
				redelivered := indexByID(t, popExactly(t, f.q, "jobs", 2))
				require.Equal(t, original.Payload, redelivered[original.ID].Payload)
				// A rejected snapshot must not poison future Ack with oversized caller data.
				redelivered[original.ID].Payload = bytes.Clone(grown.Payload)
				require.NoError(t, f.q.Ack(ctx, redelivered[original.ID], redelivered[other.ID]))
				require.Zero(t, capacityStoredBytes(t, f))
				require.NoError(t, f.q.Push(ctx, original, other))
			})
		}
	}
}

func TestCapacityConcurrentGrowthReservesBytesAtomicallyAcrossClients(t *testing.T) {
	for _, operation := range []string{"Retry", "DeadLetter"} {
		t.Run(operation, func(t *testing.T) {
			original := &queue.Message{ID: "item-0", QueueName: "jobs-0", Payload: []byte("original")}
			grown := copyDelivery(original)
			grown.Payload = bytes.Repeat([]byte("x"), 4096)
			baseSize, grownSize := capacityEncodedSize(t, original), capacityEncodedSize(t, grown)
			options := []valkeyqueue.Option{valkeyqueue.WithMaxRetainedMessages(2), valkeyqueue.WithMaxRetainedBytes(int64(baseSize + grownSize))}
			f := newTestQueue(t, options...)
			f.freezeTime()
			peer := f.peer(t, f.prefix, options...)
			clients := []*valkeyqueue.Queue{f.q, peer}
			ctx := context.Background()
			deliveries := make([]*queue.Message, 2)
			before := make([]string, 2)
			for i, q := range clients {
				msg := copyDelivery(original)
				msg.ID, msg.QueueName = fmt.Sprintf("item-%d", i), fmt.Sprintf("jobs-%d", i)
				require.NoError(t, q.Push(ctx, msg))
				deliveries[i] = popOne(t, q, msg.QueueName)
				before[i] = capacityBlob(t, f, msg.ID)
				deliveries[i].Payload = bytes.Clone(grown.Payload)
			}
			start := make(chan struct{})
			type result struct {
				index int
				err   error
			}
			results := make(chan result, 2)
			for i, q := range clients {
				go func(i int, q *valkeyqueue.Queue) {
					<-start
					settle := q.Retry
					if operation == "DeadLetter" {
						settle = q.DeadLetter
					}
					results <- result{i, settle(ctx, deliveries[i])}
				}(i, q)
			}
			close(start)
			winner := -1
			for range clients {
				select {
				case result := <-results:
					if result.err == nil {
						require.Equal(t, -1, winner, "only one growth can fit the shared byte budget")
						winner = result.index
					} else {
						requireBatchFailures(t, result.err, []*queue.Message{deliveries[result.index]}, map[int]error{0: valkeyqueue.ErrCapacityExceeded})
					}
				case <-time.After(5 * time.Second):
					t.Fatal("concurrent snapshot updates did not finish")
				}
			}
			require.NotEqual(t, -1, winner)
			loser := 1 - winner
			require.Equal(t, int64(baseSize+grownSize), capacityStoredBytes(t, f))
			require.Equal(t, before[loser], capacityBlob(t, f, fmt.Sprintf("item-%d", loser)))
			require.NoError(t, f.q.ExtendVisibility(ctx, time.Minute, deliveries[loser]))
			require.NoError(t, f.q.Ack(ctx, deliveries[loser]))
			name := fmt.Sprintf("jobs-%d", winner)
			if operation == "DeadLetter" {
				n, err := peer.Redrive(ctx, name, 1)
				require.NoError(t, err)
				require.Equal(t, 1, n)
			}
			delivery := popOne(t, peer, name)
			require.Equal(t, grown.Payload, delivery.Payload)
			require.NoError(t, f.q.Ack(ctx, delivery))
			require.Zero(t, capacityStoredBytes(t, f))
		})
	}
}

func TestCapacityPruneObservesContextAndReturnsBackendFailureWithoutDeletion(t *testing.T) {
	f := newTestQueue(t, valkeyqueue.WithDeadLetterRetention(time.Second))
	f.freezeTime()
	ctx := context.Background()
	require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "expired", QueueName: "jobs"}))
	require.NoError(t, f.q.DeadLetter(ctx, popOne(t, f.q, "jobs")))
	key, blob := capacityMessageKey(t, f), capacityBlob(t, f, "expired")
	f.advanceTime(time.Second)
	marker := "injected pruning backend failure"
	f.server.SetError("ERR " + marker)
	t.Cleanup(func() { f.server.SetError("") })
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	n, err := f.q.PruneDeadLetters(canceled)
	require.Zero(t, n)
	require.ErrorIs(t, err, context.Canceled)
	n, err = f.q.PruneDeadLetters(ctx)
	require.Zero(t, n)
	require.ErrorContains(t, err, marker)
	var batch *queue.BatchError
	require.False(t, errors.As(err, &batch))
	require.Equal(t, blob, f.server.HGet(key, "expired"))
	f.server.SetError("")
	n, err = f.q.PruneDeadLetters(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestCapacityByteAccountingTracksGrowthShrinkAckRedriveAndPrune(t *testing.T) {
	original := &queue.Message{ID: "item-a", QueueName: "jobs", Payload: bytes.Repeat([]byte("x"), 128)}
	grown := copyDelivery(original)
	grown.Payload = bytes.Repeat([]byte("y"), 1024)
	baseSize, grownSize := capacityEncodedSize(t, original), capacityEncodedSize(t, grown)
	f := newTestQueue(t, valkeyqueue.WithMaxRetainedMessages(2), valkeyqueue.WithMaxRetainedBytes(int64(2*grownSize-1)), valkeyqueue.WithDeadLetterRetention(time.Second))
	f.freezeTime()
	ctx := context.Background()
	other := copyDelivery(original)
	other.ID = "item-b"
	require.NoError(t, f.q.Push(ctx, original, other))
	index := indexByID(t, popExactly(t, f.q, "jobs", 2))
	index["item-a"].Payload = bytes.Clone(grown.Payload)
	require.NoError(t, f.q.Retry(ctx, index["item-a"]))
	require.Equal(t, int64(baseSize+grownSize), capacityStoredBytes(t, f))
	index["item-b"].Payload = bytes.Clone(grown.Payload)
	requireBatchFailures(t, f.q.DeadLetter(ctx, index["item-b"]), []*queue.Message{index["item-b"]}, map[int]error{0: valkeyqueue.ErrCapacityExceeded})
	first := popOne(t, f.q, "jobs")
	first.Payload = bytes.Clone(original.Payload)
	require.NoError(t, f.q.Retry(ctx, first))
	require.Equal(t, int64(2*baseSize), capacityStoredBytes(t, f))
	require.NoError(t, f.q.DeadLetter(ctx, index["item-b"]))
	require.Equal(t, int64(baseSize+grownSize), capacityStoredBytes(t, f))
	first = popOne(t, f.q, "jobs")
	first.Payload = make([]byte, 2*valkeyqueue.DefaultMaxMessageBytes)
	require.NoError(t, f.q.Ack(ctx, first))
	require.Equal(t, int64(grownSize), capacityStoredBytes(t, f))
	before := capacityBlob(t, f, "item-b")
	n, err := f.q.Redrive(ctx, "jobs", 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, before, capacityBlob(t, f, "item-b"), "Redrive preserves the blob and its accounted size")
	require.Equal(t, int64(grownSize), capacityStoredBytes(t, f))
	second := popOne(t, f.q, "jobs")
	second.Payload = bytes.Clone(original.Payload)
	require.NoError(t, f.q.DeadLetter(ctx, second))
	require.Equal(t, int64(baseSize), capacityStoredBytes(t, f))
	f.advanceTime(time.Second)
	n, err = f.q.PruneDeadLetters(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Zero(t, capacityStoredBytes(t, f))
	require.NoError(t, f.q.Push(ctx, original, other))
	require.Equal(t, int64(2*baseSize), capacityStoredBytes(t, f))
}

func TestCapacityRetentionStartsAtDeadLetterAndReadsDoNotRenewIt(t *testing.T) {
	f := newTestQueue(t, valkeyqueue.WithDeadLetterRetention(5*time.Second))
	f.freezeTime()
	ctx := context.Background()
	require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "deadline", QueueName: "jobs", VisibilityTimeout: 2 * time.Hour}))
	delivery := popOne(t, f.q, "jobs")
	f.advanceTime(time.Hour)
	require.NoError(t, f.q.DeadLetter(ctx, delivery))
	peer := f.peer(t, f.prefix, valkeyqueue.WithDeadLetterRetention(5*time.Second))
	wrong := f.peer(t, f.prefix, valkeyqueue.WithDeadLetterRetention(time.Hour))
	f.advanceTime(4 * time.Second)
	requireStats(t, peer, queue.QueueStats{Name: "jobs", Dead: 1})
	_, err := wrong.Stats(ctx, "jobs")
	require.ErrorIs(t, err, valkeyqueue.ErrConfigurationMismatch)
	n, err := peer.PruneDeadLetters(ctx)
	require.NoError(t, err)
	require.Zero(t, n)
	f.advanceTime(time.Second)
	n, err = peer.PruneDeadLetters(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n, "Stats, pruning, and conflicting clients must not extend an existing deadline")
}

func TestCapacityPushRecoversExpiredIDsAndCapacityAcrossWholePrefix(t *testing.T) {
	f := newTestQueue(t, valkeyqueue.WithMaxRetainedMessages(2), valkeyqueue.WithDeadLetterRetention(time.Second))
	f.freezeTime()
	ctx := context.Background()
	id := "id:with:separators\xff\x00"
	name := "old:queue/{opaque}\xfe\x00"
	require.NoError(t, f.q.Push(ctx, &queue.Message{ID: id, QueueName: name}, &queue.Message{ID: "other-expired", QueueName: "other"}))
	require.NoError(t, f.q.DeadLetter(ctx, popOne(t, f.q, name), popOne(t, f.q, "other")))
	f.advanceTime(time.Second)
	inputs := []*queue.Message{nil, {ID: id, QueueName: "new", Payload: []byte("replacement")}, {ID: "new-id", QueueName: "new"}}
	requireBatchFailures(t, f.q.Push(ctx, inputs...), inputs, map[int]error{0: queue.ErrInvalidArgument})
	requireStats(t, f.q, queue.QueueStats{Name: name})
	requireStats(t, f.q, queue.QueueStats{Name: "other"})
	requireStats(t, f.q, queue.QueueStats{Name: "new", Ready: 2})
	index := indexByID(t, popExactly(t, f.q, "new", 2))
	require.Equal(t, []byte("replacement"), index[id].Payload)
	require.NoError(t, f.q.Ack(ctx, index[id], index["new-id"]))
}

func TestCapacityPruneRemovesAllMetadataInBoundedChunksAndReleasesBothBudgets(t *testing.T) {
	const count = 260
	name := "queue:/{opaque}\xfe\x00"
	prototype := &queue.Message{ID: "expired:000\xff\x00", QueueName: name, Payload: bytes.Repeat([]byte("x"), 32)}
	size := capacityEncodedSize(t, prototype)
	f := newTestQueue(t, valkeyqueue.WithMaxRetainedMessages(count), valkeyqueue.WithMaxRetainedBytes(int64(count*size)), valkeyqueue.WithDeadLetterRetention(time.Second))
	f.freezeTime()
	ctx := context.Background()
	inputs := make([]*queue.Message, count)
	ids := make([]string, count)
	for i := range inputs {
		inputs[i] = copyDelivery(prototype)
		inputs[i].ID = fmt.Sprintf("expired:%03d\xff\x00", i)
		ids[i] = inputs[i].ID
	}
	require.NoError(t, f.q.Push(ctx, inputs...))
	require.NoError(t, f.q.DeadLetter(ctx, popExactly(t, f.q, name, count)...))
	require.Equal(t, int64(count*size), capacityStoredBytes(t, f))
	f.advanceTime(time.Second)
	n, err := f.q.PruneDeadLetters(ctx)
	require.NoError(t, err)
	require.Equal(t, count, n, "PruneDeadLetters must drain more than one 128-item script chunk")
	requireCapacityIDsAbsent(t, f, ids)
	require.Zero(t, capacityStoredBytes(t, f))
	n, err = f.q.PruneDeadLetters(ctx)
	require.NoError(t, err)
	require.Zero(t, n)
	names, err := f.q.Queues(ctx)
	require.NoError(t, err)
	require.Empty(t, names)
	require.NoError(t, f.q.Push(ctx, inputs...), "pruning must release IDs, message count, and every accounted byte")
	require.Equal(t, int64(count*size), capacityStoredBytes(t, f))
}

func TestCapacityPushPrunesMoreThanOneChunkBeforeReservingNewBatch(t *testing.T) {
	const count = 129
	f := newTestQueue(t, valkeyqueue.WithMaxRetainedMessages(count), valkeyqueue.WithDeadLetterRetention(time.Second))
	f.freezeTime()
	ctx := context.Background()
	inputs := capacityMessages(count, "expired", "source")
	require.NoError(t, f.q.Push(ctx, inputs...))
	require.NoError(t, f.q.DeadLetter(ctx, popExactly(t, f.q, "source", count)...))
	f.advanceTime(time.Second)
	replacement := capacityMessages(count, "replacement", "destination")
	require.NoError(t, f.q.Push(ctx, replacement...))
	requireStats(t, f.q, queue.QueueStats{Name: "source"})
	requireStats(t, f.q, queue.QueueStats{Name: "destination", Ready: count})
	n, err := f.q.PruneDeadLetters(ctx)
	require.NoError(t, err)
	require.Zero(t, n, "Push must have pruned the whole prefix, not just enough for its first item")
}

func TestCapacityStatsPrunesOnlyNamedQueueAndExcludesAllExpiredDeadLetters(t *testing.T) {
	const expired, live = 260, 3
	f := newTestQueue(t, valkeyqueue.WithDeadLetterRetention(3*time.Second))
	f.freezeTime()
	ctx := context.Background()
	require.NoError(t, f.q.Push(ctx, capacityMessages(expired, "expired", "jobs")...))
	require.NoError(t, f.q.DeadLetter(ctx, popExactly(t, f.q, "jobs", expired)...))
	require.NoError(t, f.q.Push(ctx, capacityMessages(2, "other", "other")...))
	require.NoError(t, f.q.DeadLetter(ctx, popExactly(t, f.q, "other", 2)...))
	f.advanceTime(time.Second)
	require.NoError(t, f.q.Push(ctx, capacityMessages(live, "live", "jobs")...))
	require.NoError(t, f.q.DeadLetter(ctx, popExactly(t, f.q, "jobs", live)...))
	f.advanceTime(2 * time.Second)
	requireStats(t, f.q, queue.QueueStats{Name: "jobs", Dead: live})
	require.Len(t, capacityStoredIDs(t, f), expired+live+2-128, "Stats must physically prune a bounded chunk, yet report only live dead letters")
	require.NotEmpty(t, capacityBlob(t, f, "other-000"), "named Stats must not prune another queue")
	n, err := f.q.PruneDeadLetters(ctx)
	require.NoError(t, err)
	require.Equal(t, expired+2-128, n)
	require.Len(t, capacityStoredIDs(t, f), live)
}

func TestCapacityDiscoveryPrunesExpiredOnlyQueuesAcrossWholePrefix(t *testing.T) {
	const count = 129
	f := newTestQueue(t, valkeyqueue.WithDeadLetterRetention(time.Second))
	f.freezeTime()
	ctx := context.Background()
	name := "expired:queue\xff\x00"
	inputs := capacityMessages(count, "expired", name)
	require.NoError(t, f.q.Push(ctx, inputs...))
	require.NoError(t, f.q.DeadLetter(ctx, popExactly(t, f.q, name, count)...))
	require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "active", QueueName: "active"}))
	f.advanceTime(time.Second)
	names, err := f.q.Queues(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"active"}, names)
	require.Equal(t, []string{"active"}, capacityStoredIDs(t, f))
	n, err := f.q.PruneDeadLetters(ctx)
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestCapacityRedriveSkipsExpiredLettersBeyondOneChunkAndFindsLiveOnes(t *testing.T) {
	const expired, live = 260, 3
	f := newTestQueue(t, valkeyqueue.WithDeadLetterRetention(3*time.Second))
	f.freezeTime()
	ctx := context.Background()
	require.NoError(t, f.q.Push(ctx, capacityMessages(expired, "expired", "jobs")...))
	require.NoError(t, f.q.DeadLetter(ctx, popExactly(t, f.q, "jobs", expired)...))
	f.advanceTime(time.Second)
	require.NoError(t, f.q.Push(ctx, capacityMessages(live, "live", "jobs")...))
	require.NoError(t, f.q.DeadLetter(ctx, popExactly(t, f.q, "jobs", live)...))
	f.advanceTime(2 * time.Second)
	n, err := f.q.Redrive(ctx, "jobs", 1)
	require.NoError(t, err)
	require.Equal(t, 1, n, "expired head entries must not hide later live dead letters")
	first := popOne(t, f.q, "jobs")
	require.True(t, strings.HasPrefix(first.ID, "live-"))
	require.Equal(t, 1, first.Attempt)
	require.NoError(t, f.q.Ack(ctx, first))
	n, err = f.q.Redrive(ctx, "jobs", expired+live)
	require.NoError(t, err)
	require.Equal(t, live-1, n)
	remaining := popExactly(t, f.q, "jobs", live-1)
	for _, msg := range remaining {
		require.True(t, strings.HasPrefix(msg.ID, "live-"), "expired letters must never become active again")
	}
	requirePopEmpty(t, f.q, "jobs")
	f.advanceTime(time.Second)
	_, err = f.q.PruneDeadLetters(ctx)
	require.NoError(t, err)
	require.NoError(t, f.q.Ack(ctx, remaining...), "old dead-letter deadlines must not affect redriven in-flight work")
	require.Empty(t, capacityStoredIDs(t, f))
}

func TestCapacityRedriveReplayWithExpiredHeadDoesNotExceedRequestedCount(t *testing.T) {
	const expired, requested, extra = 129, 129, 7
	options := []valkeyqueue.Option{valkeyqueue.WithDeadLetterRetention(3 * time.Second)}
	f := newTestQueue(t, options...)
	f.freezeTime()
	ctx := context.Background()
	require.NoError(t, f.q.Push(ctx, capacityMessages(expired, "expired", "jobs")...))
	require.NoError(t, f.q.DeadLetter(ctx, popExactly(t, f.q, "jobs", expired)...))
	f.advanceTime(time.Second)
	require.NoError(t, f.q.Push(ctx, capacityMessages(requested+extra, "live", "jobs")...))
	require.NoError(t, f.q.DeadLetter(ctx, popExactly(t, f.q, "jobs", requested+extra)...))
	f.advanceTime(2 * time.Second)
	q := valkeyqueue.New(&replayingClient{Client: f.client}, valkeyqueue.WithKeyPrefix(f.prefix), options[0])
	n, err := q.Redrive(ctx, "jobs", requested)
	require.NoError(t, err)
	require.Equal(t, requested, n)
	requireStats(t, f.q, queue.QueueStats{Name: "jobs", Ready: requested, Dead: extra})
	for _, msg := range popExactly(t, f.q, "jobs", requested) {
		require.True(t, strings.HasPrefix(msg.ID, "live-"))
	}
}

func TestCapacityRedriveClearsExpiryAndAckIDReuseSurvivesOldDeadline(t *testing.T) {
	for _, state := range []string{"pending", "delayed", "inflight", "reused-id"} {
		t.Run(state, func(t *testing.T) {
			f := newTestQueue(t, valkeyqueue.WithDeadLetterRetention(time.Second), valkeyqueue.WithMaxRetainedMessages(1))
			f.freezeTime()
			ctx := context.Background()
			id, name := "opaque:id:\xff\x00", "opaque:queue:\xfe\x00"
			require.NoError(t, f.q.Push(ctx, &queue.Message{ID: id, QueueName: name}))
			require.NoError(t, f.q.DeadLetter(ctx, popOne(t, f.q, name)))
			n, err := f.q.Redrive(ctx, name, 1)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			var delivery *queue.Message
			if state != "pending" {
				delivery = popOne(t, f.q, name)
			}
			switch state {
			case "delayed":
				delivery.Delay = time.Hour
				require.NoError(t, f.q.Retry(ctx, delivery))
			case "reused-id":
				require.NoError(t, f.q.Ack(ctx, delivery))
				name = "replacement:queue\xfd\x00"
				require.NoError(t, f.q.Push(ctx, &queue.Message{ID: id, QueueName: name, Payload: []byte("new logical message")}))
			}
			f.advanceTime(time.Second)
			n, err = f.q.PruneDeadLetters(ctx)
			require.NoError(t, err)
			require.Zero(t, n)
			require.NotEmpty(t, capacityBlob(t, f, id))
			if state == "delayed" {
				requireStats(t, f.q, queue.QueueStats{Name: name, Delayed: 1})
				f.advanceTime(time.Hour)
			}
			if state != "inflight" {
				delivery = popOne(t, f.q, name)
			}
			if state == "reused-id" {
				require.Equal(t, []byte("new logical message"), delivery.Payload)
			}
			require.NoError(t, f.q.Ack(ctx, delivery))
		})
	}
}

func TestCapacityMaintenancePrunesInitiallyAndPeriodicallyWithoutQueueTraffic(t *testing.T) {
	for _, mode := range []string{"initial", "periodic"} {
		t.Run(mode, func(t *testing.T) {
			f := newTestQueue(t, valkeyqueue.WithDeadLetterRetention(time.Second))
			f.freezeTime()
			ctx := context.Background()
			require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "idle", QueueName: "jobs"}))
			require.NoError(t, f.q.DeadLetter(ctx, popOne(t, f.q, "jobs")))
			key := capacityMessageKey(t, f)
			interval := 10 * time.Millisecond
			if mode == "initial" {
				interval = time.Hour
				f.advanceTime(time.Second)
			}
			var calls atomic.Int64
			f.server.Server().SetPreHook(func(_ *server.Peer, command string, _ ...string) bool {
				if strings.HasPrefix(strings.ToUpper(command), "EVAL") {
					calls.Add(1)
				}
				return false
			})
			t.Cleanup(func() { f.server.Server().SetPreHook(nil) })
			owned, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- f.q.RunMaintenance(owned, interval) }()
			require.Eventually(t, func() bool { return calls.Load() > 0 }, 3*time.Second, 5*time.Millisecond)
			if mode == "periodic" {
				require.NotEmpty(t, f.server.HGet(key, "idle"))
				f.advanceTime(time.Second)
			}
			// Inspect miniredis directly: Stats, Queues, and Push would prune too.
			require.Eventually(t, func() bool { return f.server.HGet(key, "idle") == "" }, 3*time.Second, 5*time.Millisecond)
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("maintenance did not return when its owning context was canceled")
			}
			if mode == "periodic" {
				require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "after-stop", QueueName: "jobs"}))
				require.NoError(t, f.q.DeadLetter(ctx, popOne(t, f.q, "jobs")))
				f.advanceTime(time.Second)
				time.Sleep(50 * time.Millisecond)
				require.NotEmpty(t, f.server.HGet(key, "after-stop"), "canceled maintenance must not leave a hidden pruning loop")
			}
		})
	}
}

func TestCapacityMaintenanceValidatesIntervalAndReturnsContextErrors(t *testing.T) {
	f := newTestQueue(t)
	f.server.SetError("ERR maintenance must not contact backend")
	t.Cleanup(func() { f.server.SetError("") })
	for _, interval := range []time.Duration{0, -time.Millisecond} {
		require.ErrorIs(t, f.q.RunMaintenance(context.Background(), interval), queue.ErrInvalidArgument)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, f.q.RunMaintenance(canceled, 10*time.Millisecond), context.Canceled)
	require.Empty(t, f.server.Keys())
	f.server.SetError("")
	deadline, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	require.ErrorIs(t, f.q.RunMaintenance(deadline, 10*time.Millisecond), context.DeadlineExceeded)
}

func TestCapacityMaintenanceReturnsDirectInitialAndPeriodicBackendFailures(t *testing.T) {
	for _, stage := range []string{"initial", "periodic"} {
		t.Run(stage, func(t *testing.T) {
			f := newTestQueue(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			marker := "injected maintenance backend failure"
			if stage == "initial" {
				f.server.SetError("ERR " + marker)
			}
			t.Cleanup(func() { f.server.SetError("") })
			done := make(chan error, 1)
			go func() { done <- f.q.RunMaintenance(ctx, 10*time.Millisecond) }()
			if stage == "periodic" {
				require.Eventually(t, func() bool { return len(f.server.Keys()) > 0 }, time.Second, 5*time.Millisecond, "initial prune should persist the prefix policy")
				select {
				case err := <-done:
					t.Fatalf("maintenance stopped before backend failure: %v", err)
				default:
				}
				f.server.SetError("ERR " + marker)
			}
			select {
			case err := <-done:
				require.ErrorContains(t, err, marker)
				require.NotErrorIs(t, err, context.DeadlineExceeded)
				var batch *queue.BatchError
				require.False(t, errors.As(err, &batch), "maintenance errors are direct, not per-item BatchErrors")
			case <-time.After(2 * time.Second):
				t.Fatal("maintenance hid a backend failure or kept retrying")
			}
		})
	}
}

func TestCapacityMaintenanceCancellationInterruptsActiveBackendWait(t *testing.T) {
	f := newTestQueue(t)
	started, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.server.Server().SetPreHook(func(_ *server.Peer, command string, _ ...string) bool {
		if strings.HasPrefix(strings.ToUpper(command), "EVAL") {
			once.Do(func() { close(started) })
			<-resume
		}
		return false
	})
	defer func() {
		close(resume)
		f.server.Server().SetPreHook(nil)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.q.RunMaintenance(ctx, 10*time.Millisecond) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("maintenance did not send its initial pruning script")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("maintenance waited for backend I/O after cancellation")
	}
}

func TestCapacityMissingPolicyWithRetainedMessagesRejectsLegacyStorage(t *testing.T) {
	f := newTestQueue(t)
	ctx := context.Background()
	require.NoError(t, f.q.Push(ctx, &queue.Message{ID: "legacy-id", QueueName: "jobs"}))
	delivery := popOne(t, f.q, "jobs")
	key, blob := capacityMessageKey(t, f), capacityBlob(t, f, "legacy-id")
	// Preserve only a legacy retained-message hash, with no v2 policy/accounting.
	f.server.FlushAll()
	f.server.HSet(key, "legacy-id", blob)
	calls := []struct {
		name string
		call func() error
	}{
		{"Push", func() error { return f.q.Push(ctx, &queue.Message{ID: "new", QueueName: "jobs"}) }},
		{"Pop", func() error { _, err := f.q.Pop(ctx, "jobs", 1); return err }},
		{"Ack", func() error { return f.q.Ack(ctx, delivery) }},
		{"Retry", func() error { return f.q.Retry(ctx, delivery) }},
		{"DeadLetter", func() error { return f.q.DeadLetter(ctx, delivery) }},
		{"Stats", func() error { _, err := f.q.Stats(ctx, "jobs"); return err }},
		{"Queues", func() error { _, err := f.q.Queues(ctx); return err }},
		{"Redrive", func() error { _, err := f.q.Redrive(ctx, "jobs", 1); return err }},
		{"PruneDeadLetters", func() error { _, err := f.q.PruneDeadLetters(ctx); return err }},
	}
	for _, call := range calls {
		t.Run(call.name, func(t *testing.T) {
			requireCapacityOperationError(t, call.call(), valkeyqueue.ErrIncompatibleStorage)
			require.Equal(t, blob, f.server.HGet(key, "legacy-id"))
			require.Equal(t, []string{key}, f.server.Keys(), "legacy detection must not silently initialize/reset accounting")
		})
	}
}

func capacityMessages(count int, idPrefix, name string) []*queue.Message {
	messages := make([]*queue.Message, count)
	for i := range messages {
		messages[i] = &queue.Message{ID: fmt.Sprintf("%s-%03d", idPrefix, i), QueueName: name}
	}
	return messages
}

func capacityMessageKey(t *testing.T, f *testQueue) string {
	t.Helper()
	var matches []string
	for _, key := range f.server.Keys() {
		if strings.HasSuffix(key, ":messages") {
			matches = append(matches, key)
		}
	}
	require.Len(t, matches, 1, "this helper requires exactly one prefix with retained blobs")
	return matches[0]
}

func capacityBlob(t *testing.T, f *testQueue, id string) string {
	t.Helper()
	blob := f.server.HGet(capacityMessageKey(t, f), id)
	require.NotEmpty(t, blob, "missing stored JSON blob for %q", id)
	return blob
}

func capacityEncodedSize(t *testing.T, msg *queue.Message) int {
	t.Helper()
	f := newTestQueue(t)
	input := copyDelivery(msg)
	require.NoError(t, f.q.Push(context.Background(), input))
	return len(capacityBlob(t, f, input.ID))
}

func capacityStoredIDs(t *testing.T, f *testQueue) []string {
	t.Helper()
	var ids []string
	for _, key := range f.server.Keys() {
		if strings.HasSuffix(key, ":messages") {
			fields, err := f.server.HKeys(key)
			require.NoError(t, err)
			ids = append(ids, fields...)
		}
	}
	return ids
}

func capacityStoredBytes(t *testing.T, f *testQueue) int64 {
	t.Helper()
	var total int64
	for _, key := range f.server.Keys() {
		if strings.HasSuffix(key, ":messages") {
			fields, err := f.server.HKeys(key)
			require.NoError(t, err)
			for _, id := range fields {
				total += int64(len(f.server.HGet(key, id)))
			}
		}
	}
	return total
}

func requireCapacityOperationError(t *testing.T, err, expected error) {
	t.Helper()
	require.Error(t, err)
	var batch *queue.BatchError
	if errors.As(err, &batch) {
		require.NotEmpty(t, batch.Failed)
		for _, item := range batch.Failed {
			require.ErrorIs(t, item.Err, expected)
		}
		return
	}
	require.ErrorIs(t, err, expected)
}

func requireCapacityIDsAbsent(t *testing.T, f *testQueue, ids []string) {
	t.Helper()
	for _, key := range f.server.Keys() {
		var members []string
		var err error
		switch f.server.Type(key) {
		case "hash":
			members, err = f.server.HKeys(key)
		case "zset":
			members, err = f.server.ZMembers(key)
		case "set":
			members, err = f.server.Members(key)
		default:
			continue
		}
		require.NoError(t, err)
		for _, id := range ids {
			for _, member := range members {
				require.False(t, member == id || strings.HasSuffix(member, ":"+id), "pruned ID %q still appears in %q as %q", id, key, member)
			}
		}
	}
}
