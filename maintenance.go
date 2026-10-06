package valkeyqueue

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-hypercube/go-hypercube/queue"
)

const (
	DefaultDeadLetterRetention = 7 * 24 * time.Hour
	DefaultMaxRetainedMessages = 100_000
	DefaultMaxMessageBytes     = 1 << 20
	DefaultMaxRetainedBytes    = 64 << 20
	DefaultMaintenanceInterval = time.Minute
)

var (
	// ErrCapacityExceeded means a new or enlarged snapshot would exceed the
	// namespace's retained-message count or serialized-byte budget.
	ErrCapacityExceeded = errors.New("queue: retained message capacity exceeded")
	// ErrMessageTooLarge means the entire serialized message exceeds its limit.
	ErrMessageTooLarge = errors.New("queue: message too large")
	// ErrConfigurationMismatch means another client initialized this prefix
	// with different retention or capacity settings.
	ErrConfigurationMismatch = errors.New("queue: configuration mismatch")
	// ErrIncompatibleStorage rejects retained messages from an older layout
	// instead of silently losing their expiry or byte-accounting information.
	ErrIncompatibleStorage = errors.New("queue: incompatible storage layout")
)

// WithDeadLetterRetention sets how long terminal messages remain available for
// inspection/redrive, measured from successful DeadLetter using server time.
// Non-positive values are ignored; active messages never expire by this policy.
func WithDeadLetterRetention(retention time.Duration) Option {
	return func(q *Queue) {
		if retention > 0 {
			q.deadLetterRetention = retention
		}
	}
}

// WithMaxRetainedMessages bounds all ready, delayed, in-flight, and dead
// messages across a prefix. Non-positive values are ignored.
func WithMaxRetainedMessages(count int) Option {
	return func(q *Queue) {
		if count > 0 {
			q.maxRetainedMessages = count
		}
	}
}

// WithMaxMessageBytes bounds the entire encoded JSON blob, including metadata
// and base64 expansion, on Push, Retry, and DeadLetter. Non-positive values are
// ignored. Ack can always discard caller-modified data without serializing it.
func WithMaxMessageBytes(size int) Option {
	return func(q *Queue) {
		if size > 0 {
			q.maxMessageBytes = size
		}
	}
}

// WithMaxRetainedBytes bounds the sum of retained encoded message blobs in a
// prefix. It does not include server/index overhead; configure maxmemory with
// noeviction as well. Non-positive values are ignored.
func WithMaxRetainedBytes(size int64) Option {
	return func(q *Queue) {
		if size > 0 {
			q.maxRetainedBytes = size
		}
	}
}

// PruneDeadLetters permanently removes terminal messages whose retention
// expired at the start of this sweep, across the entire prefix. Scripts delete
// at most 128 messages at a time, with all indexes, ID reservations, counts, and
// byte accounting updated atomically. The result is the known completed count
// if a later step fails. Active messages are never pruned.
func (q *Queue) PruneDeadLetters(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	cutoff := "0"
	completed := 0
	for {
		result, err := q.run(ctx, expiredQueuesScript, q.keys(""), cutoff, scriptBatchSize)
		if err != nil {
			return completed, err
		}
		fields, ok := result.([]any)
		if !ok || len(fields) != 2 {
			return completed, fmt.Errorf("queue: malformed expiry discovery response")
		}
		timeValue, timeOK := fields[0].(string)
		names, namesOK := fields[1].([]any)
		if !timeOK || !namesOK {
			return completed, fmt.Errorf("queue: malformed expiry discovery fields")
		}
		cutoff = timeValue
		if len(names) == 0 {
			return completed, nil
		}
		groups := make(map[string][]string, len(names))
		var order []string
		for _, value := range names {
			pair, ok := value.([]any)
			if !ok || len(pair) != 2 {
				return completed, fmt.Errorf("queue: malformed expiry entry")
			}
			encoded, nameOK := pair[0].(string)
			id, idOK := pair[1].(string)
			if !nameOK || !idOK {
				return completed, fmt.Errorf("queue: malformed expiry entry fields")
			}
			if _, exists := groups[encoded]; !exists {
				order = append(order, encoded)
			}
			groups[encoded] = append(groups[encoded], id)
		}
		for _, encoded := range order {
			name, err := base64.RawURLEncoding.DecodeString(encoded)
			if err != nil {
				return completed, fmt.Errorf("queue: decode expiry queue name: %w", err)
			}
			args := []any{cutoff, string(name), encoded}
			for _, id := range groups[encoded] {
				args = append(args, id)
			}
			result, err := q.run(ctx, pruneScript, q.keys(string(name)), args...)
			if err != nil {
				return completed, err
			}
			n, ok := result.(int64)
			if !ok {
				return completed, fmt.Errorf("queue: malformed prune response")
			}
			completed += int(n)
		}
	}
}

// RunMaintenance prunes immediately, then periodically until ctx ends or a
// backend operation fails. It runs in the caller's goroutine: New never starts
// hidden workers. Run this under an application-owned context to reclaim dead
// letters in idle queues; stop it before closing the client.
func (q *Queue) RunMaintenance(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return queue.ErrInvalidArgument
	}
	if _, err := q.PruneDeadLetters(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := q.PruneDeadLetters(ctx); err != nil {
				return err
			}
		}
	}
}

// Preflight before encoding avoids copying enormous caller-owned fields into
// temporary base64 buffers. The server still decides error precedence: a
// duplicate ID or stale lease wins over an oversized new snapshot.
func (q *Queue) snapshot(msg *queue.Message) (data string, tooLarge bool, err error) {
	size := 0
	for _, length := range []int{len(msg.ID), len(msg.QueueName), len(msg.Namespace), len(msg.JobName), len(msg.Payload), len(msg.Extra)} {
		if length > q.maxMessageBytes-size {
			return "", true, nil
		}
		size += length
	}
	data, err = encodeMessage(msg)
	if err != nil {
		return "", false, err
	}
	if len(data) > q.maxMessageBytes {
		return "", true, nil
	}
	return data, false, nil
}

func (q *Queue) policyArgs(args []any) []any {
	result := make([]any, 0, len(args)+4)
	result = append(result, args...)
	return append(result, durationMillis(q.deadLetterRetention), q.maxRetainedMessages, q.maxMessageBytes, q.maxRetainedBytes)
}

func backendError(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "hypercube queue: configuration mismatch") {
		return fmt.Errorf("%w: %w", ErrConfigurationMismatch, err)
	}
	if strings.Contains(err.Error(), "hypercube queue: incompatible storage") {
		return fmt.Errorf("%w: %w", ErrIncompatibleStorage, err)
	}
	return err
}

func oversizedFlag(tooLarge bool) int {
	if tooLarge {
		return 1
	}
	return 0
}

func itemResult(result any, missing error) error {
	switch result {
	case int64(0):
		return missing
	case int64(1):
		return nil
	case int64(2):
		return fmt.Errorf("%w: retained message count limit", ErrCapacityExceeded)
	case int64(3):
		return ErrMessageTooLarge
	case int64(4):
		return fmt.Errorf("%w: retained serialized byte limit", ErrCapacityExceeded)
	default:
		return fmt.Errorf("queue: unexpected mutation response %v", result)
	}
}
