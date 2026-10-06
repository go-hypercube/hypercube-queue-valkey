// Package valkeyqueue implements the Hypercube queue contract using Valkey.
// State transitions are atomic Lua scripts; durability depends on the Valkey
// server's persistence, replication, and eviction configuration.
package valkeyqueue

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/go-hypercube/go-hypercube/queue"
	"github.com/valkey-io/valkey-go"
)

const (
	DefaultKeyPrefix         = "hypercube:queue"
	DefaultPollingTimeout    = time.Second
	DefaultPollingInterval   = 25 * time.Millisecond
	DefaultVisibilityTimeout = 30 * time.Second

	// Bound the work of a single Lua execution so large requests do not block
	// the Valkey server for an unbounded number of messages.
	scriptBatchSize = 128
)

// Option configures a Queue at construction time.
type Option func(*Queue)

// WithKeyPrefix isolates queue data and message IDs. Clients using the same
// prefix and Valkey database share a queue, including its delivery leases.
func WithKeyPrefix(prefix string) Option {
	return func(q *Queue) { q.prefix = prefix }
}

// WithPollingTimeout sets the maximum empty Pop wait. Non-positive values
// check for visible messages once and return queue.ErrEmpty when none exist.
func WithPollingTimeout(timeout time.Duration) Option {
	return func(q *Queue) { q.pollingTimeout = timeout }
}

// WithPollingInterval sets the interval between empty Pop checks. Non-positive
// values are ignored. The final wait is shortened to the polling deadline.
func WithPollingInterval(interval time.Duration) Option {
	return func(q *Queue) {
		if interval > 0 {
			q.pollingInterval = interval
		}
	}
}

// WithDefaultVisibilityTimeout sets the lease used for a message with zero
// VisibilityTimeout. Non-positive option values are ignored.
func WithDefaultVisibilityTimeout(timeout time.Duration) Option {
	return func(q *Queue) {
		if timeout > 0 {
			q.defaultVisibilityTimeout = timeout
		}
	}
}

// Queue is safe for concurrent use. Construct it with New. The caller owns
// the Valkey client and is responsible for closing it after workers stop.
type Queue struct {
	client                   valkey.Client
	prefix                   string
	keyBase                  string
	pollingTimeout           time.Duration
	pollingInterval          time.Duration
	defaultVisibilityTimeout time.Duration
	deadLetterRetention      time.Duration
	maxRetainedMessages      int
	maxMessageBytes          int
	maxRetainedBytes         int64
}

// ValkeyQueue is an alias for callers that prefer an explicit driver type name.
type ValkeyQueue = Queue

type deliveryToken struct {
	scope string
	value string
}

// Keep driver state outside the JSON blob. Lua never decodes/re-encodes JSON,
// which avoids loss of precision in Go durations and preserves opaque bytes.
type messageData struct {
	ID                []byte
	QueueName         []byte
	Namespace         []byte
	JobName           []byte
	Payload           []byte
	Extra             []byte
	VisibilityTimeout time.Duration
}

var (
	_ queue.Queue              = (*Queue)(nil)
	_ queue.VisibilityExtender = (*Queue)(nil)
	_ queue.RedriveProvider    = (*Queue)(nil)
	_ queue.StatsProvider      = (*Queue)(nil)
)

// New wraps a valkey-go client without contacting Valkey or taking ownership of
// it. Single-node, Sentinel, and Cluster clients are supported. Every key in
// one isolation prefix shares a Cluster hash slot for atomic global ID checks.
func New(client valkey.Client, options ...Option) *Queue {
	q := &Queue{
		client:                   client,
		prefix:                   DefaultKeyPrefix,
		pollingTimeout:           DefaultPollingTimeout,
		pollingInterval:          DefaultPollingInterval,
		defaultVisibilityTimeout: DefaultVisibilityTimeout,
		deadLetterRetention:      DefaultDeadLetterRetention,
		maxRetainedMessages:      DefaultMaxRetainedMessages,
		maxMessageBytes:          DefaultMaxMessageBytes,
		maxRetainedBytes:         DefaultMaxRetainedBytes,
	}
	for _, option := range options {
		if option != nil {
			option(q)
		}
	}
	// Place the controlled hash tag first: braces in a user prefix cannot
	// accidentally select another Cluster slot or defeat namespace isolation.
	q.keyBase = fmt.Sprintf("{%x}:%s:", sha256.Sum256([]byte(q.prefix)), q.prefix)
	return q
}

// Push snapshots messages, ignoring caller-supplied Attempt and DriverData.
func (q *Queue) Push(ctx context.Context, msgs ...*queue.Message) error {
	maintained := false
	return q.batch(ctx, msgs, func(msg *queue.Message) error {
		if !maintained {
			maintained = true
			if _, err := q.PruneDeadLetters(ctx); err != nil {
				return err
			}
		}
		if msg.ID == "" {
			msg.ID = rand.Text()
		}
		data, tooLarge, err := q.snapshot(msg)
		if err != nil {
			return err
		}
		name := msg.QueueName
		if tooLarge {
			// Still check the global ID for duplicate precedence, but do not
			// encode a potentially enormous queue name into unused index keys.
			name = ""
		}
		result, err := q.run(ctx, pushScript, q.keys(name),
			msg.ID, data, durationMillis(msg.VisibilityTimeout), int64(msg.Delay),
			durationMillis(msg.Delay), name, oversizedFlag(tooLarge))
		if err != nil {
			return err
		}
		return itemResult(result, queue.ErrAlreadyExists)
	})
}

// Pop polls until it can atomically lease at least one visible message. A
// single result contains at most 128 messages, even when maxMessages is larger.
func (q *Queue) Pop(ctx context.Context, queueName string, maxMessages int) ([]*queue.Message, error) {
	if maxMessages <= 0 {
		return nil, queue.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limit := min(maxMessages, scriptBatchSize)
	deadline := time.Now().Add(q.pollingTimeout)
	keys := q.keys(queueName)
	for {
		result, err := q.run(ctx, popScript, keys, limit,
			durationMillis(q.defaultVisibilityTimeout), rand.Text())
		if err != nil {
			return nil, err
		}
		rows, ok := result.([]any)
		if !ok {
			return nil, fmt.Errorf("valkey queue: unexpected Pop response %T", result)
		}
		if len(rows) > 0 {
			messages := make([]*queue.Message, 0, len(rows))
			for _, row := range rows {
				msg, err := q.decodeDelivery(row)
				if err != nil {
					return nil, err
				}
				messages = append(messages, msg)
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return messages, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		wait := min(q.pollingInterval, time.Until(deadline))
		if wait <= 0 {
			return nil, queue.ErrEmpty
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// Ack atomically removes each current delivery and its retained logical ID.
func (q *Queue) Ack(ctx context.Context, msgs ...*queue.Message) error {
	return q.settle(ctx, "ack", msgs)
}

// Retry snapshots updated message data and schedules each current delivery
// using its new Delay. No attempt limit or automatic dead-letter policy exists.
func (q *Queue) Retry(ctx context.Context, msgs ...*queue.Message) error {
	return q.settle(ctx, "retry", msgs)
}

// DeadLetter snapshots updated message data into retained dead-letter storage.
func (q *Queue) DeadLetter(ctx context.Context, msgs ...*queue.Message) error {
	return q.settle(ctx, "dead", msgs)
}

func (q *Queue) settle(ctx context.Context, operation string, msgs []*queue.Message) error {
	return q.batch(ctx, msgs, func(msg *queue.Message) error {
		token, ok := q.token(msg)
		if !ok {
			return queue.ErrNotFound
		}
		var data string
		var tooLarge bool
		script := ackScript
		if operation != "ack" {
			script = settleScript
			var err error
			data, tooLarge, err = q.snapshot(msg)
			if err != nil {
				return err
			}
		}
		encoded := base64.RawURLEncoding.EncodeToString([]byte(msg.QueueName))
		result, err := q.run(ctx, script, q.keys(msg.QueueName),
			msg.ID, token, operation, msg.QueueName, data,
			int64(msg.Delay), durationMillis(msg.Delay), encoded, oversizedFlag(tooLarge))
		if err != nil {
			return err
		}
		return itemResult(result, queue.ErrNotFound)
	})
}

// ExtendVisibility replaces the deadline with Valkey time plus extension.
func (q *Queue) ExtendVisibility(ctx context.Context, extension time.Duration, msg *queue.Message) error {
	if extension <= 0 || msg == nil {
		return queue.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	token, ok := q.token(msg)
	if !ok {
		return queue.ErrNotFound
	}
	result, err := q.run(ctx, extendScript, q.keys(msg.QueueName),
		msg.ID, token, durationMillis(extension))
	if err != nil {
		return err
	}
	if result == int64(0) {
		return queue.ErrNotFound
	}
	return nil
}

// Redrive atomically moves dead letters to immediately eligible pending state
// in bounded chunks. It preserves data and resets Attempt, Delay, and leases.
func (q *Queue) Redrive(ctx context.Context, queueName string, count int) (int, error) {
	if count <= 0 {
		return 0, queue.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	keys := q.keys(queueName)
	completed := 0
	for completed < count {
		limit := min(count-completed, scriptBatchSize)
		// Replaying a command after a lost network response must not transfer
		// another chunk and accidentally exceed the caller's count.
		requestKeys := append(keys, q.keyBase+"redrive:"+rand.Text())
		encoded := base64.RawURLEncoding.EncodeToString([]byte(queueName))
		result, err := q.run(ctx, redriveScript, requestKeys, limit, (5 * time.Minute).Milliseconds(), encoded, queueName)
		if err != nil {
			return completed, err
		}
		n, ok := result.(int64)
		if !ok {
			return completed, fmt.Errorf("valkey queue: unexpected Redrive response %T", result)
		}
		completed += int(n)
		if n < int64(limit) {
			break
		}
	}
	return completed, nil
}

// Queues lists queue names with retained pending, in-flight, or dead messages.
func (q *Queue) Queues(ctx context.Context) ([]string, error) {
	if _, err := q.PruneDeadLetters(ctx); err != nil {
		return nil, err
	}
	result, err := q.run(ctx, queuesScript, q.keys(""))
	if err != nil {
		return nil, err
	}
	values, ok := result.([]any)
	if !ok {
		return nil, fmt.Errorf("valkey queue: unexpected Queues response %T", result)
	}
	names := make([]string, 0, len(values))
	for _, value := range values {
		name, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("valkey queue: unexpected queue name %T", value)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// Stats returns an atomic snapshot; expired visibility leases count as Ready.
func (q *Queue) Stats(ctx context.Context, queueName string) (queue.QueueStats, error) {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(queueName))
	result, err := q.run(ctx, statsScript, q.keys(queueName), queueName, encoded)
	if err != nil {
		return queue.QueueStats{}, err
	}
	values, ok := result.([]any)
	if !ok || len(values) != 4 {
		return queue.QueueStats{}, fmt.Errorf("valkey queue: unexpected Stats response %T", result)
	}
	var counts [4]int64
	for i, value := range values {
		count, ok := value.(int64)
		if !ok {
			return queue.QueueStats{}, fmt.Errorf("valkey queue: unexpected Stats count %T", value)
		}
		counts[i] = count
	}
	return queue.QueueStats{
		Name: queueName, Ready: counts[0], Delayed: counts[1],
		InFlight: counts[2], Dead: counts[3],
	}, nil
}

func (q *Queue) batch(ctx context.Context, msgs []*queue.Message, apply func(*queue.Message) error) error {
	if len(msgs) == 0 {
		return nil
	}
	var failed []queue.ItemError
	for index, msg := range msgs {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		if msg == nil {
			err = queue.ErrInvalidArgument
		} else {
			err = apply(msg)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		// A client can observe its I/O deadline before the context timer
		// publishes ctx.Err(). Cancellation is still an operation-wide error,
		// not an item failure hidden inside a non-unwrapping BatchError.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if err != nil {
			failed = append(failed, queue.ItemError{Index: index, Msg: msg, Err: err})
		}
	}
	if len(failed) > 0 {
		return &queue.BatchError{Failed: failed}
	}
	return nil
}

func (q *Queue) token(msg *queue.Message) (string, bool) {
	token, ok := msg.DriverData.(deliveryToken)
	return token.value, ok && token.scope == q.keyBase && token.value != ""
}

func (q *Queue) keys(queueName string) []string {
	name := base64.RawURLEncoding.EncodeToString([]byte(queueName))
	return []string{
		q.keyBase + "messages", q.keyBase + "attempts",
		q.keyBase + "visibility", q.keyBase + "delays",
		q.keyBase + "tokens", q.keyBase + "queues",
		q.keyBase + "q:" + name + ":pending",
		q.keyBase + "q:" + name + ":inflight",
		q.keyBase + "q:" + name + ":dead",
		q.keyBase + "dead-expiry", q.keyBase + "retained-bytes", q.keyBase + "policy",
	}
}

func encodeMessage(msg *queue.Message) (string, error) {
	data, err := json.Marshal(messageData{
		ID: []byte(msg.ID), QueueName: []byte(msg.QueueName), Namespace: []byte(msg.Namespace),
		JobName: []byte(msg.JobName), Payload: msg.Payload, Extra: msg.Extra,
		VisibilityTimeout: msg.VisibilityTimeout,
	})
	return string(data), err
}

func (q *Queue) decodeDelivery(row any) (*queue.Message, error) {
	fields, ok := row.([]any)
	if !ok || len(fields) != 3 {
		return nil, fmt.Errorf("valkey queue: unexpected delivery response %T", row)
	}
	data, dataOK := fields[0].(string)
	attempt, attemptOK := fields[1].(int64)
	token, tokenOK := fields[2].(string)
	if !dataOK || !attemptOK || !tokenOK {
		return nil, fmt.Errorf("valkey queue: malformed delivery response")
	}
	var stored messageData
	if err := json.Unmarshal([]byte(data), &stored); err != nil {
		return nil, fmt.Errorf("valkey queue: decode message: %w", err)
	}
	return &queue.Message{
		ID: string(stored.ID), QueueName: string(stored.QueueName), Namespace: string(stored.Namespace),
		JobName: string(stored.JobName), Payload: stored.Payload, Extra: stored.Extra,
		VisibilityTimeout: stored.VisibilityTimeout, Attempt: int(attempt),
		DriverData: deliveryToken{scope: q.keyBase, value: token},
	}, nil
}

// Valkey scores use millisecond server time. Round positive durations upward
// without overflowing time.Duration, including for sub-millisecond leases.
func durationMillis(duration time.Duration) int64 {
	millis := int64(duration / time.Millisecond)
	if duration%time.Millisecond != 0 {
		if duration > 0 {
			millis++
		} else {
			millis--
		}
	}
	return millis
}

// The native client observes cancellation while waiting for I/O. Scripts stay
// mutating/non-retryable so uncertain writes are not automatically replayed.
func (q *Queue) run(ctx context.Context, script *valkey.Lua, keys []string, args ...any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if q.client == nil {
		return nil, fmt.Errorf("%w: nil Valkey client", queue.ErrInvalidArgument)
	}
	args = q.policyArgs(args)
	argv := make([]string, len(args))
	for i, arg := range args {
		if value, ok := arg.(string); ok {
			argv[i] = value
		} else {
			argv[i] = fmt.Sprint(arg)
		}
	}
	value, err := script.Exec(ctx, q.client, keys, argv).ToAny()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	return value, backendError(err)
}
