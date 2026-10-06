# Hypercube Valkey Queue

A Valkey implementation of the [go-hypercube queue contract](https://pkg.go.dev/github.com/go-hypercube/go-hypercube/queue), backed by the native [`github.com/valkey-io/valkey-go`](https://github.com/valkey-io/valkey-go) client. It follows the same state and storage semantics as the Hypercube Redis queue driver, without a go-redis dependency.

Implements the core `queue.Queue` interface and all optional capabilities:

- `queue.VisibilityExtender`
- `queue.RedriveProvider`
- `queue.StatsProvider`

## Features

- Atomic Lua-backed enqueue, delivery leases, settlement, visibility extension, and redrive
- Concurrent workers across processes sharing a Valkey database and key prefix
- Delayed push and retry, with per-message/default visibility timeouts
- At-least-once redelivery after worker crashes or expired leases
- Fresh delivery tokens and rejection of stale settlement, including before redelivery
- Independent snapshots of messages, payload, and metadata
- Duplicate ID detection across all queues and retained states in a prefix
- Batch operations with partial failure reporting and original input indices
- Explicit dead-letter storage, programmatic redrive, queue discovery, and statistics
- Native context cancellation, RESP2/RESP3 support, and script-cache fallback
- No application retry limits or automatic dead-lettering

## Installation

The standalone module path follows the other Hypercube drivers:

```sh
go get github.com/go-hypercube/hypercube-queue-valkey
```

## Usage

```go
package main

import (
    "context"
    "errors"
    "log"
    "time"

    "github.com/go-hypercube/go-hypercube/queue"
    valkeyqueue "github.com/go-hypercube/hypercube-queue-valkey"
    "github.com/valkey-io/valkey-go"
)

func main() {
    ctx := context.Background()
    client, err := valkey.NewClient(valkey.ClientOption{
        InitAddress:      []string{"localhost:6379"},
        ConnWriteTimeout: 3 * time.Second,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    q := valkeyqueue.New(client,
        valkeyqueue.WithKeyPrefix("myapp:queue"),
        valkeyqueue.WithPollingTimeout(2*time.Second),
        valkeyqueue.WithPollingInterval(25*time.Millisecond),
        valkeyqueue.WithDefaultVisibilityTimeout(30*time.Second),
    )
    msg := &queue.Message{
        QueueName: "emails",
        Namespace: "app",
        JobName:   "send-welcome-email",
        Payload:   []byte(`{"user_id":42}`),
    }
    if err := q.Push(ctx, msg); err != nil {
        log.Fatal(err)
    }

    messages, err := q.Pop(ctx, "emails", 10)
    if errors.Is(err, queue.ErrEmpty) {
        return
    }
    if err != nil {
        log.Fatal(err)
    }
    for _, delivery := range messages {
        // Process delivery.Payload; handlers must be idempotent.
        if err := q.Ack(ctx, delivery); err != nil {
            log.Fatal(err)
        }
    }
}
```

`New` wraps an existing `valkey.Client` without contacting a server or taking ownership of it. The caller is responsible for closing the client after workers stop. The client supports standalone, Sentinel, and Cluster deployments; configure those through `valkey.ClientOption`. Unlike the queue constructor, `valkey.NewClient` establishes connections.

The driver uses `valkey.NewLuaScript` and native context-aware execution. Scripts are **not marked read-only or retryable**, including statistics and discovery, so requests stay on the primary and uncertain writes are not automatically replayed. No client-side caching API is used, and the driver does not create a separate connection pool or background workers.

## Configuration

| Option | Default | Behavior |
| --- | --- | --- |
| `WithKeyPrefix` | `hypercube:queue` | Storage isolation namespace. Clients using the same prefix and database share IDs, messages, and delivery leases. |
| `WithPollingTimeout` | `1s` | Maximum empty `Pop` wait. Non-positive values check once and return `queue.ErrEmpty` if nothing is visible. |
| `WithPollingInterval` | `25ms` | Interval between empty `Pop` checks. Non-positive values are ignored; waits are capped by the remaining polling timeout. |
| `WithDefaultVisibilityTimeout` | `30s` | Lease used when a message's `VisibilityTimeout` is zero. Non-positive option values are ignored. |
| `WithDeadLetterRetention` | `7 days` | Terminal retention measured from successful dead-lettering using server time. |
| `WithMaxRetainedMessages` | `100,000` | Maximum ready, delayed, in-flight, and dead messages combined across the prefix. |
| `WithMaxMessageBytes` | `1 MiB` | Maximum entire serialized JSON snapshot, including routing metadata and base64 expansion. |
| `WithMaxRetainedBytes` | `64 MiB` | Maximum sum of retained serialized message blobs across the prefix, excluding server/index overhead. |

Use consistent default visibility settings across workers. Explicit nonzero per-message visibility timeouts override the worker's default.

Delays and visibility deadlines use **server time**, not worker clocks. Positive durations are rounded upward to milliseconds, including sub-millisecond leases. Original `VisibilityTimeout` values round-trip without losing nanosecond precision. Non-positive message delays are immediately eligible. A negative per-message visibility timeout creates an already-expired delivery, matching the memory driver.

`Pop` uses a polling loop rather than a blocking list or subscription. Empty responses do not end the wait early: it keeps polling until a message becomes visible, the context ends, or the polling timeout expires. Discovery latency is normally bounded by the polling interval plus server latency. There is no FIFO guarantee.

The Redis and Valkey queue drivers use the same default key prefix and storage layout. Use distinct prefixes for independent workloads; switching client libraries does not create an isolated queue namespace on the same backend. `DriverData` is private to each driver and must not be passed between driver implementations.

## Capacity, retention, and maintenance

Safeguards are finite by default. Non-positive retention/capacity option values are ignored; they cannot disable the limits. The first backend operation atomically persists a versioned policy for the prefix. Every producer, consumer, and maintenance instance sharing that prefix must use the same four retention/capacity settings; incompatible settings return `ErrConfigurationMismatch`. Polling and default visibility settings are not part of the persisted policy.

Capacity is reserved atomically across processes and queues. A new push that would exceed the count or serialized-byte budget fails with an item-level `ErrCapacityExceeded`; an oversized snapshot fails with `ErrMessageTooLarge`. Retry and dead-letter updates also check message size and any increase in stored bytes. A failed update leaves that delivery and its previous stored snapshot intact. Shrinking a snapshot, acknowledging it, or pruning a dead letter releases the corresponding budget. Ack does not serialize caller-modified payload/metadata, so oversized annotations do not prevent discarding the delivery.

**Active messages have no TTL.** Ready, delayed, and in-flight work is never silently discarded by retention or capacity limits. Backpressure belongs to producers; application expiry decisions belong to workers/managers calling Ack or DeadLetter.

Dead letters expire at their stored absolute deadline. Expired messages cannot be redriven and are excluded from dead statistics. `PruneDeadLetters(ctx)` physically removes them across the prefix, atomically clearing snapshots, all message fields/indexes, ID reservations, queue counts, and byte accounting. It returns the known completed count with an error if a later chunk fails. Scripts handle at most 128 IDs at a time, rechecking current dead state to protect redriven or reused IDs.

Push prunes once before processing valid items in a batch, recovering expired IDs and capacity across queues. Queues prunes before discovery; Stats and Redrive opportunistically prune up to 128 expired messages in the named queue. **Run periodic maintenance to reclaim memory in idle queues**—logical expiry alone is not a native key TTL:

```go
maintenanceCtx, stopMaintenance := context.WithCancel(ctx)
maintenanceDone := make(chan error, 1)
go func() {
    maintenanceDone <- q.RunMaintenance(maintenanceCtx, valkeyqueue.DefaultMaintenanceInterval)
}()

// Run producers/workers here, and monitor maintenanceDone for backend failures.
// At shutdown, stop and join maintenance before closing the Valkey client:
stopMaintenance()
if err := <-maintenanceDone; err != nil && !errors.Is(err, context.Canceled) {
    log.Printf("queue maintenance stopped: %v", err)
}
```

`RunMaintenance(ctx, interval)` prunes immediately, then periodically in the caller's goroutine until cancellation or a backend failure. The default suggested interval is one minute; interval must be positive. It does not silently retry failures or start hidden goroutines in New. One loop per prefix is sufficient; multiple instances can prune safely.

These budgets are **not a literal Valkey RAM limit**: indexes, ID fields, delivery tokens, script/client overhead, and temporary nonzero redrive receipts use additional memory. Configure server `maxmemory` with `noeviction` and leave headroom. Ack, pruning, expiry discovery, and statistics/discovery scripts carry compatible Redis 7 `allow-oom` metadata so freeing/read paths are not denied merely because the server's memory limit has been reached.

This is storage layout **version 2**. Earlier retained queues without policy/accounting metadata return `ErrIncompatibleStorage`; use a fresh prefix rather than resetting counters over existing work. Retention/capacity policy stays fixed for an initialized prefix, including after it is drained. Use a new prefix when intentionally changing that policy.

## Lifecycle and ownership

```text
Push             -> pending
Pop              -> pending -> in-flight
Ack              -> in-flight -> removed
Retry            -> in-flight -> pending
DeadLetter       -> in-flight -> dead
ExtendVisibility -> in-flight -> in-flight
Redrive          -> unexpired dead -> pending
PruneDeadLetters -> expired dead -> removed
```

- `Push` assigns a collision-resistant ID if needed, snapshots message data, ignores caller-supplied `DriverData`, and starts the stored attempt count at zero.
- IDs stay reserved while ready, delayed, in-flight, or retained dead letters. An acknowledged or pruned ID can be reused.
- Every `Pop` returns independently allocated data, increments `Attempt`, resets `Delay`, and attaches a new opaque delivery token.
- An expired delivery is stale immediately. Settlement and visibility extension reject it with `queue.ErrNotFound`, even if no worker has redelivered it yet.
- `Retry` snapshots current `Payload`, `Extra`, and `Delay`. Set a new delay explicitly; leaving it zero retries immediately.
- `DeadLetter` preserves the latest caller-modifiable state, retaining the logical ID and attempt count until retention expires. Ordinary workers do not receive dead letters unless explicitly redriven before expiry.
- `ExtendVisibility` replaces the deadline with server time plus the positive extension; it does not add to the previous deadline.
- `Redrive` preserves ID, routing fields, payload, metadata, and visibility timeout. It resets attempts and delay and clears the old lease. The next delivery has `Attempt == 1`.

Preserve protected fields and `DriverData` unchanged between delivery and settlement. After successful `Ack`, `Retry`, or `DeadLetter`, relinquish that delivery and all memory reachable from it, as required by the portable contract.

## Batches and cancellation

`Push`, `Ack`, `Retry`, and `DeadLetter` process items independently, including batches spanning multiple queues. A failed item does not prevent other valid items from completing. Each item uses an atomic script; the whole batch is not one transaction.

```go
if err := q.Retry(ctx, messages...); err != nil {
    var batch *queue.BatchError
    if errors.As(err, &batch) {
        for _, item := range batch.Failed {
            log.Printf("item %d: %v", item.Index, item.Err)
        }
    }
}
```

Item errors include `queue.ErrInvalidArgument` for nil messages, `queue.ErrAlreadyExists` for duplicate pushed IDs, and `queue.ErrNotFound` for stale deliveries. Infrastructure failures are retained as item errors too. Empty batches return nil without contacting the backend, even when their context is already canceled.

The driver observes contexts during polling and active backend I/O using the native client's cancellation support. Configure finite `ConnWriteTimeout` and dial timeouts for resource cleanup and unresponsive-server detection. A submitted command can still complete after cancellation: a context error is an **unknown outcome**, not evidence of rollback.

`Pop` returns at most 128 messages per call to bound Lua execution time; returning fewer than `maxMessages` is allowed by the contract. `Redrive` processes larger requests in chunks of up to 128 and returns the known completed count if a later chunk fails. Temporary five-minute request receipts protect a chunk from duplicate execution. They are not message TTLs or retry-policy limits; any custom retry mechanism must keep its retry budget shorter than this window.

## Statistics

```go
names, err := q.Queues(ctx)
if err != nil {
    return err
}
for _, name := range names {
    stats, err := q.Stats(ctx, name)
    if err != nil {
        return err
    }
    log.Printf("%s: ready=%d delayed=%d in_flight=%d dead=%d",
        stats.Name, stats.Ready, stats.Delayed, stats.InFlight, stats.Dead)
}
```

Snapshots are read atomically. Expired leases count as `Ready`, not `InFlight`. Unknown or empty queues return `queue.QueueStats{Name: queueName}`. Discovery includes queues holding only unexpired dead letters; queues disappear after their last retained message is acknowledged or pruned. Statistics are for observability, not correctness decisions.

## Storage and deployment requirements

Keys use this layout:

```text
{sha256(prefix)}:prefix:messages             # ID -> serialized message data
{sha256(prefix)}:prefix:attempts             # ID -> delivery count
{sha256(prefix)}:prefix:visibility           # ID -> configured lease duration
{sha256(prefix)}:prefix:delays               # ID -> current Delay in nanoseconds
{sha256(prefix)}:prefix:tokens               # ID -> current delivery token
{sha256(prefix)}:prefix:queues               # queue name -> retained message count
{sha256(prefix)}:prefix:policy               # schema version and immutable safeguard settings
{sha256(prefix)}:prefix:retained-bytes       # sum of serialized message blob lengths
{sha256(prefix)}:prefix:dead-expiry          # expiry -> base64(queue name):ID
{sha256(prefix)}:prefix:q:base64(name):pending
{sha256(prefix)}:prefix:q:base64(name):inflight
{sha256(prefix)}:prefix:q:base64(name):dead    # sorted by absolute expiry deadline
{sha256(prefix)}:prefix:redrive:request-id    # temporary redrive receipt
```

The hash tag is the full hexadecimal SHA-256 digest of the prefix; queue names use raw URL-safe base64. Placing the controlled tag before the user prefix prevents embedded braces from changing Cluster routing. All keys are explicitly declared to Lua.

Message blobs are JSON. Byte slices and opaque string fields are base64-encoded to preserve arbitrary bytes, including invalid UTF-8. Attempts, current delay, scheduling, and delivery state live separately. `DriverData` is never serialized. The storage format is driver-owned, not a stable public API; do not modify these keys manually.

Operational requirements:

- Use a Valkey primary with Lua enabled. The Lua API intentionally uses the compatible `redis.call` function. Read-only replicas cannot serve queue transitions.
- Configure suitable **AOF/RDB persistence, replication, backups, and failover**. Without persistence, messages can be lost on restart; asynchronous replication can lose acknowledged writes during failover. The driver does not strengthen server durability with `WAIT` or `WAITAOF`.
- Use **`maxmemory-policy noeviction`** or a dedicated instance without applicable eviction. Do not expire, evict, flush, or externally mutate queue keys: removing only some related keys can lose messages or corrupt indexes.
- Every prefix occupies **one Cluster hash slot**, deliberately, for atomic global ID checks and transitions. Use separate prefixes to distribute independent workloads; message ID uniqueness is scoped to each prefix.
- Active messages have no TTL or application attempt limit. Dead letters expire after the configured retention. Plan capacity, handle producer backpressure, and run maintenance for idle queues.
- Delivery is at-least-once, not exactly-once. Handlers must be idempotent.

## Testing

All tests use isolated [`miniredis`](https://github.com/alicebob/miniredis) test doubles and the native Valkey client. There is no external-server or environment-variable connection path, and no Valkey/Redis installation or Docker daemon is needed.

```sh
make test
make race
make lint
# Or:
go test -race -count=1 ./...
go vet ./...
```

Tests cover the portable queue contract, binary-safe ownership, leases, dead-letter/redrive preservation, concurrency, RESP2/RESP3, polling/cancellation, capacity reservation, exact serialized-size boundaries, byte accounting, retention deadlines, stale expiry/ID reuse, schema/policy mismatches, and idle maintenance.

Miniredis does not understand Redis 7 Lua headers. Test-only client adapters strip that metadata while executing the unchanged script bodies. Real-server Lua flag handling, memory-pressure behavior, and Valkey/Cluster integration are not validated by this suite.
