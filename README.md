# Redis Clone in Go

It speaks the RESP protocol, so the standard `redis-cli` and any Redis client library can talk to it. The project is also a vehicle for learning Go concurrency properly: every feature below was chosen to exercise a real concurrency problem (shared state, background work, cancellation, and cross-connection signalling).

## Quick start

```bash
# run the server (listens on 0.0.0.0:6379)
go run ./app

# in another terminal
redis-cli PING
redis-cli SET greeting hello PX 5000
redis-cli GET greeting
```

Run with the race detector during development:

```bash
go run -race ./app
```

## Supported commands

| Command | Syntax | Reply |
|---|---|---|
| `PING` | `PING` | `+PONG` |
| `ECHO` | `ECHO message` | bulk string |
| `SET` | `SET key value [EX seconds \| PX milliseconds]` | `+OK` |
| `GET` | `GET key` | bulk string, or null (`$-1`) if missing or expired |
| `RPUSH` | `RPUSH key value [value ...]` | list length after the push |
| `LPUSH` | `LPUSH key value [value ...]` | list length after the push |
| `LRANGE` | `LRANGE key start stop` | array (supports negative indices) |
| `LPOP` | `LPOP key [count]` | bulk string, or array when `count` is given |
| `BLPOP` | `BLPOP key [key ...] timeout` | `[key, value]` array, or null array (`*-1`) on timeout |
| `TYPE` | `TYPE key` | `+string`, `+list`, or `+none` |
| `XADD` | `XADD key streamID fields` | `streamID` or error message |

### Semantics worth knowing

- **Expiry.** Keys set without `EX`/`PX` never expire. Expired keys are invisible to every read path (`GET`, `TYPE`, etc.) immediately, even before they are physically removed.
- **Lists.** `LPUSH k a b c` produces `c b a` (each value is pushed onto the head in turn). A list that becomes empty is deleted, just like in Redis.
- **`LRANGE` indices** are inclusive on both ends. Negative indices count from the tail (`-1` is the last element), and out-of-range values are clamped rather than rejected. `LRANGE k 0 -1` returns the whole list.
- **`LPOP` reply shape** depends on whether `count` was supplied: `LPOP k` returns a bulk string, while `LPOP k 1` returns a one-element array.
- **`BLPOP`** blocks until one of the keys has data. Keys are checked in order, so the first key acts as a priority queue. Blocked clients are served in FIFO order. The timeout is a float in seconds, and `0` means wait forever.

## Architecture

```
app/
├── main.go      # listener, accept loop, signal handling, graceful shutdown
├── store.go     # the keyspace: strings, lists, expiry, blocking waiters
└── resp/        # RESP protocol reader
```

### Connection model

Each client connection is served by its own goroutine (`go handleClient(...)`). Handlers are written as straightforward blocking code; the Go runtime multiplexes them over OS threads, so a slow client never blocks the others.

### The store

All data lives in a single `store` guarded by one mutex:

```go
type store struct {
	mu      sync.RWMutex
	data    map[string]entry
	waiters map[string][]chan popped
}
```

Every read-modify-write operation (`RPUSH`, `LPOP`, lazy expiry in `GET`, etc.) holds the lock for the **entire** operation. This avoids check-then-act bugs, where the state changes between reading a value and acting on it. The race detector cannot catch those, because each individual access is locked.

Returned slices (for example from `LRANGE`) are copied while the lock is still held, so callers never share a backing array with live data.

### Expiry: lazy plus active

Redis-style expiry uses two mechanisms, and both are needed:

1. **Lazy expiry.** Every read checks the key's deadline and treats an expired key as missing. This guarantees correctness.
2. **Background sweeper.** A goroutine wakes on a `time.Ticker` and deletes expired keys. This reclaims memory for keys that are never read again, which lazy expiry alone would leak forever.

Expiry deadlines are stored as absolute `time.Time` values; the zero value means "no TTL".

### Blocking pops (`BLPOP`)

This is the most concurrency-heavy part of the server. A blocked client needs to be woken by a **different** connection's push.

- When `BLPOP` finds every key empty, it creates a buffered channel (capacity 1) and registers it in `waiters` for each key. The emptiness check and the registration happen **under the same lock acquisition**; otherwise a push could slip in between them and the client would block forever on a non-empty list.
- `RPUSH`/`LPUSH` check for waiters first and hand values **directly** to the longest-waiting client, one value per waiter. Remaining values are appended to the list. The reply still reports the full list length, matching Redis.
- The waiting client `select`s on three things: its channel, a timeout timer (a nil channel when the timeout is `0`, so it never fires), and the connection's `context`.
- **Timeout race.** On timeout or cancellation, the client re-takes the lock and removes itself from the waiter queues. If it is no longer registered, a pusher already claimed it and a value is sitting in its buffered channel, so it takes the value instead of returning null. Without this check, elements could be silently lost.

### Graceful shutdown

On `SIGINT`/`SIGTERM`:

1. A root `context` is cancelled.
2. The listener is closed, which unblocks `Accept`.
3. The sweeper exits via its `ctx.Done()` case.
4. Each connection has a small watcher goroutine that calls `SetReadDeadline(time.Now())` when the context is cancelled, forcing a blocked `Read` to return (since `net.Conn` does not respect contexts on its own).
5. Blocked `BLPOP` calls return through their `ctx.Done()` case.
6. `main` waits on a `sync.WaitGroup` covering the sweeper and every connection before exiting.

## Testing

```bash
go test -race ./...
```

The key concurrency test for `BLPOP` checks a conservation invariant under heavy contention: many goroutines blocking with short timeouts while many others push. It asserts that **values received + values left in the list = values pushed**, so nothing is lost or delivered twice. Run it repeatedly to explore different interleavings:

```bash
go test -race -run TestNothingLost -count=50 ./app
```

### Manual testing with `redis-cli`

```bash
# terminal A: blocks
redis-cli BLPOP tasks 0

# terminal B: wakes terminal A
redis-cli RPUSH tasks hello
```

Other useful checks:

- `BLPOP empty 2` returns `(nil)` after two seconds.
- Several clients blocked on one key, then `RPUSH tasks a b`: exactly two wake, in the order they blocked.
- Press Ctrl+C on the server while a client is blocked: the server should exit cleanly instead of hanging.

## Known limitations

These are deliberate simplifications, not bugs:

- **The sweeper scans the whole keyspace under the write lock.** Fine for small datasets, but it would stall all clients with millions of keys. Real Redis samples about 20 random keys per cycle instead.
- **`LPUSH` is O(n) per element** because lists are Go slices. Real Redis uses a quicklist, which makes head insertion O(1).
- **A single global lock** serializes all writes. Sharding the keyspace would improve write throughput.
- **No persistence and no replication yet.**
- **Shutdown waits without a timeout.** A wedged handler would stall shutdown indefinitely; production servers bound this (for example 30 seconds).

## Roadmap

- Streams (`XADD`, `XRANGE`, `XREAD`)
- Transactions (`MULTI` / `EXEC`)
- Replication and the `WAIT` command
- RDB persistence