# redis-go

A Redis server written from scratch in Go — no Redis libraries, no third-party dependencies, standard library only.

It speaks the RESP wire protocol, so `redis-cli` and any off-the-shelf Redis client can talk to it unmodified. Beyond the keyspace it implements the parts of Redis that are actually hard: blocking reads, transactions with optimistic locking, pub/sub fan-out, password auth, leader/follower replication, and both persistence formats (RDB load, AOF append + replay).

The project doubles as a deliberate exercise in Go concurrency: every feature below was picked because it forces a real concurrency problem — shared mutable state, background work, cancellation, cross-connection signalling, and check-then-act races that the race detector cannot see.

---

## Highlights

| Area | What's implemented |
|---|---|
| **Data types** | strings, lists, hashes of bits (bitmaps), sorted sets, streams, geospatial indexes |
| **Blocking** | `BLPOP` across multiple keys, `XREAD BLOCK`, both cancellable and timeout-aware |
| **Transactions** | `MULTI` / `EXEC` / `DISCARD` with `WATCH` optimistic locking |
| **Pub/Sub** | `SUBSCRIBE` / `UNSUBSCRIBE` / `PUBLISH` with subscriber-mode command gating |
| **Auth** | `AUTH`, SHA-256 password hashes, constant-time comparison, `ACL` introspection |
| **Replication** | full leader/follower handshake, command propagation, `REPLCONF ACK` offsets, `WAIT` |
| **Persistence** | RDB file loading on boot, AOF append-on-write with Redis 7-style multi-part layout and replay |
| **Expiry** | lazy expiry on every read path plus a background sweeper goroutine |
| **Protocol** | hand-written RESP reader: multibulk, inline commands, pipelining, hard size limits |
| **Lifecycle** | signal-driven graceful shutdown that unblocks every blocked reader and waits for every goroutine |

---

## Quick start

```bash
# build
go build -o redis-go ./app

# run (listens on 0.0.0.0:6379)
./redis-go

# or straight from source
go run ./app
```

```bash
# in another terminal
redis-cli PING
redis-cli SET greeting hello PX 5000
redis-cli GET greeting
redis-cli RPUSH tasks build test ship
redis-cli LRANGE tasks 0 -1
```

Develop with the race detector on — several subsystems hand values between goroutines:

```bash
go run -race ./app
```

### Persistence

```bash
# load a dump on boot
./redis-go --dir /var/lib/redis-go --dbfilename dump.rdb

# durable writes: every write command is appended to the AOF and replayed on restart
./redis-go --dir /var/lib/redis-go --appendonly yes --appendfsync always
```

### Replication

```bash
# terminal A — leader
./redis-go --port 6379

# terminal B — follower
./redis-go --port 6380 --replicaof "localhost 6379"

# terminal C
redis-cli -p 6379 SET city tashkent
redis-cli -p 6380 GET city      # -> "tashkent"
redis-cli -p 6379 WAIT 1 500    # -> (integer) 1
redis-cli -p 6380 INFO replication
```

---

## Configuration

All configuration is via flags (`app/config.go`); values are also readable at runtime through `CONFIG GET`.

| Flag | Default | Meaning |
|---|---|---|
| `--port` | `6379` | TCP port to listen on |
| `--replicaof` | *(empty)* | `"<host> <port>"` — start as a follower of that leader |
| `--dir` | `/app` | base directory for all persistence files |
| `--dbfilename` | *(empty)* | RDB file to load on boot; empty disables RDB loading |
| `--appendonly` | `no` | `yes` enables the append-only file |
| `--appenddirname` | `appendonlydir` | AOF directory, created under `--dir` |
| `--appendfilename` | `appendonly.aof` | AOF base name; the manifest and incr file derive from it |
| `--appendfsync` | `everysec` | `always`, `everysec`, or `no` |

Invalid values are rejected at startup rather than silently ignored — a malformed `--replicaof` or an unknown `--appendfsync` aborts the boot with a clear error.

---

## Command reference

### Connection & server

| Command | Syntax | Reply |
|---|---|---|
| `PING` | `PING [message]` | `+PONG`, or a push-shaped reply in subscriber mode |
| `ECHO` | `ECHO message` | bulk string |
| `AUTH` | `AUTH password` / `AUTH username password` | `+OK` or `-WRONGPASS` |
| `ACL` | `ACL WHOAMI` / `ACL GETUSER` / `ACL SETUSER <user> >password` | varies |
| `INFO` | `INFO` | `# Replication` section: `role`, `master_replid`, `master_repl_offset` |
| `CONFIG` | `CONFIG GET <param>` | 2-element array; supports `dir`, `dbfilename`, `appendonly`, `appendfilename`, `appendfsync`, `appenddirname` |
| `KEYS` | `KEYS pattern` | array of key names |

### Strings & bitmaps

| Command | Syntax | Reply |
|---|---|---|
| `SET` | `SET key value [PX milliseconds]` | `+OK` |
| `GET` | `GET key` | bulk string, or null (`$-1`) when missing or expired |
| `INCR` | `INCR key` | integer after the increment |
| `STRLEN` | `STRLEN key` | string length in bytes |
| `TYPE` | `TYPE key` | `+string`, `+stream`, `+zset`, or `+none` |
| `SETBIT` | `SETBIT key offset 0\|1` | the **previous** bit value |
| `GETBIT` | `GETBIT key offset` | `0` or `1`; reads past the end return `0` without growing the value |
| `BITCOUNT` | `BITCOUNT key [start stop]` | number of set bits in the byte range |
| `BITOP` | `BITOP AND\|OR destkey key1 key2` | length in bytes of the stored result |

### Lists

| Command | Syntax | Reply |
|---|---|---|
| `RPUSH` | `RPUSH key value [value ...]` | list length after the push |
| `LPUSH` | `LPUSH key value [value ...]` | list length after the push |
| `LLEN` | `LLEN key` | list length, `0` when missing |
| `LRANGE` | `LRANGE key start stop` | array; inclusive bounds, negative indices, clamped |
| `LPOP` | `LPOP key [count]` | bulk string, or an array when `count` is given |
| `BLPOP` | `BLPOP key [key ...] timeout` | `[key, value]`, or a null array (`*-1`) on timeout |

### Sorted sets

| Command | Syntax | Reply |
|---|---|---|
| `ZADD` | `ZADD key score member` | `1` when added, `0` when an existing member was rescored |
| `ZRANK` | `ZRANK key member` | rank, or null when the member or key is missing |
| `ZRANGE` | `ZRANGE key start stop` | members in score order; negative indices supported |
| `ZCARD` | `ZCARD key` | cardinality |
| `ZSCORE` | `ZSCORE key member` | score as a bulk string, or null |
| `ZREM` | `ZREM key member` | `1` when removed, else `0` |

### Geospatial

Built on top of the sorted set: a position is stored as its 52-bit interleaved geohash score.

| Command | Syntax | Reply |
|---|---|---|
| `GEOADD` | `GEOADD key longitude latitude member` | `1` when added, `0` when updated |
| `GEOPOS` | `GEOPOS key member [member ...]` | one `[lon, lat]` pair per member; null array for unknown members |
| `GEODIST` | `GEODIST key member1 member2` | distance in meters as a bulk string |
| `GEOSEARCH` | `GEOSEARCH key FROMLONLAT lon lat BYRADIUS radius <unit>` | members inside the radius |

### Streams

| Command | Syntax | Reply |
|---|---|---|
| `XADD` | `XADD key <id>\|<ms>-*\|* field value [field value ...]` | the generated entry ID |
| `XRANGE` | `XRANGE key start end` | entries; `-` and `+` mean "from the beginning" / "to the end" |
| `XREAD` | `XREAD [BLOCK ms] STREAMS key [key ...] id [id ...]` | per-stream entries; `$` means "only entries added from now on" |

### Transactions

| Command | Syntax | Reply |
|---|---|---|
| `MULTI` | `MULTI` | `+OK`; subsequent commands reply `+QUEUED` |
| `EXEC` | `EXEC` | array of replies, or a null array (`*-1`) when a watched key changed |
| `DISCARD` | `DISCARD` | `+OK`, queue dropped |
| `WATCH` | `WATCH key [key ...]` | `+OK` |
| `UNWATCH` | `UNWATCH` | `+OK` |

### Pub/Sub

| Command | Syntax | Reply |
|---|---|---|
| `SUBSCRIBE` | `SUBSCRIBE channel [channel ...]` | one `["subscribe", channel, count]` push per channel |
| `UNSUBSCRIBE` | `UNSUBSCRIBE channel [channel ...]` | one `["unsubscribe", channel, count]` push per channel |
| `PUBLISH` | `PUBLISH channel message` | number of subscribers the message reached |

### Replication

| Command | Syntax | Reply |
|---|---|---|
| `PSYNC` | `PSYNC <replid> <offset>` | `+FULLRESYNC <replid> 0` followed by an RDB payload; the connection then becomes a replica link |
| `REPLCONF` | `REPLCONF listening-port <port>` / `capa <c>` / `ACK <offset>` / `GETACK *` | `+OK`, or nothing for an `ACK` |
| `WAIT` | `WAIT numreplicas timeout_ms` | number of replicas acknowledged up to the current offset |

### Semantics worth knowing

- **Expiry.** Keys written without `PX` never expire. An expired key is invisible to every read path immediately, before it is physically deleted.
- **Lists.** `LPUSH k a b c` yields `c b a`: each value is pushed onto the head in turn. A list that drains to empty is removed from the keyspace, as in real Redis.
- **`LRANGE`** bounds are inclusive. `-1` is the last element, out-of-range values are clamped rather than rejected, and `LRANGE k 0 -1` returns everything.
- **`LPOP` reply shape** depends on the `count` argument: `LPOP k` is a bulk string, `LPOP k 1` is a one-element array.
- **`BLPOP`** checks keys in the order given, so the first key behaves as a priority queue. Blocked clients are served FIFO. The timeout is a float in seconds; `0` waits forever.
- **Stream IDs** are `<ms>-<seq>`. `*` generates both halves from the clock, `<ms>-*` generates only the sequence, and an explicit ID must be strictly greater than the stream's last ID (`0-0` is never valid).
- **Subscriber mode.** Once a connection holds at least one subscription, only the subscribe/unsubscribe family, `PING`, and `QUIT` are accepted; anything else gets an error, matching Redis.
- **`WRONGTYPE`** is enforced on the typed paths: a string key cannot be used as a stream, a list cannot be used as a sorted set, and so on.

---

## Architecture

```
app/
├── main.go            # boot: flags, RDB load, AOF open + replay, listener, signals, shutdown
├── config.go          # flag parsing and validation
├── server.go          # server singleton: replication identity, replica set, pub/sub, ACL
├── client.go          # per-connection loop: auth gate, tx queueing, subscriber mode, replica links
├── handleCommands.go  # the command dispatcher: one command in, one RESP reply out
├── store.go           # the keyspace: strings, lists, zsets, bitmaps, streams, geo, expiry, waiters
├── replication.go     # leader-side propagation and ACK tracking; follower-side handshake and apply loop
├── aof.go             # append-only file: append with fsync policy, replay on boot
├── pubsub.go          # channel registry and subscriber fan-out
├── resp_write.go      # RESP encoders for every reply shape
├── rdb/rdb.go         # RDB file reader (length encodings, aux fields, expiries, string values)
└── resp/reader.go     # RESP reader: multibulk, inline commands, pipelining, size limits
```

### Connection model

Every connection gets its own goroutine (`handleClient`). Handlers are plain blocking code; the Go runtime multiplexes them across OS threads, so one slow or blocked client never stalls the others.

Per-connection state — the queued transaction, the watched-key list, the subscription set, the authenticated user — lives on `handleClient`'s stack and is therefore unshared by construction. Only truly global state (`store`, `pubsub`, `replicaSet`, `acl`) is behind a mutex.

The split between `client.go` and `handleCommands.go` is deliberate: commands that mutate *connection* state (`MULTI`, `SUBSCRIBE`, `AUTH`, `PSYNC`) must stay in the connection loop, while commands that only touch the keyspace go through `handleCommands`. That separation is what lets the same dispatcher be reused by three different callers — a live client, the AOF replayer, and the replication apply loop — each handing it a different `io.Writer`.

### The keyspace

All data lives in one `store` behind a single `sync.RWMutex`:

```go
type store struct {
	mu       sync.RWMutex
	data     map[string]entry
	waiters  map[string][]chan popped   // BLPOP queues, per key
	notify   chan struct{}              // broadcast channel for XREAD BLOCK
	watchers map[string][]*transaction  // WATCH registrations, per key
}
```

Every read-modify-write operation holds the lock for the **whole** operation, not per-access. This is what prevents check-then-act bugs, where state changes between reading a value and acting on it — a class of bug the race detector cannot report, because each individual access is correctly locked.

Slices handed back to callers (`LRANGE`, `XRANGE`, `XREAD`) are copied while the lock is still held, so a reply never shares a backing array with live data.

### Expiry: lazy plus active

Two mechanisms, both necessary:

1. **Lazy expiry** — every read compares the entry's deadline against now and treats an expired key as missing. This is what guarantees correctness.
2. **Background sweeper** — a goroutine on a `time.Ticker` (100 ms) deletes expired entries. This reclaims memory for keys nobody reads again, which lazy expiry alone would leak forever.

Deadlines are absolute `time.Time` values; the zero value means "no TTL". The sweeper also marks watchers dirty when it deletes a key, so an expiry correctly aborts a concurrent `EXEC`.

### Blocking reads

`BLPOP` is the most concurrency-heavy path in the server: a blocked client has to be woken by a *different* connection's push.

- When every requested key is empty, the client creates a buffered channel (capacity 1) and registers it under each key in `waiters`. The emptiness check and the registration happen **in the same lock acquisition** — otherwise a push could slip in between and the client would block forever on a non-empty list.
- `RPUSH` checks for waiters *first* and hands values **directly** to the longest-waiting client, one value per waiter; leftovers are appended to the list. The reply still reports the full list length, matching Redis.
- The waiting client `select`s on three things: its channel, a timeout timer (left nil when the timeout is `0`, so it never fires), and the connection's context.
- **The timeout race.** On timeout or cancellation the client re-takes the lock and removes itself from the waiter queues. If it is no longer registered, a pusher already claimed it and a value is sitting in its buffered channel — so it takes that value instead of returning null. Without this check, pushed elements would be silently lost.

`XREAD BLOCK` solves a different shape of the same problem. There is no single consumer to hand an entry to, so instead of a per-waiter channel the store keeps one `notify` channel that `XADD` closes and replaces. Closing broadcasts to every blocked reader at once; each then re-checks its own cursor under the lock and either returns or waits on the fresh channel. The resolved cursor is captured *before* blocking, so `$` means "entries added after this call started", not "after it woke up".

### Transactions and `WATCH`

`MULTI` flips a per-connection flag; every later command except the transaction-control verbs is appended to a queue and answered `+QUEUED`. `EXEC` drains the queue through the same dispatcher a live client uses.

`WATCH` registers the connection's `transaction` under each watched key. Every mutating store path calls `markWatchersLocked`, which flips the `dirty` flag on each watcher **while holding the write lock** — so the window between "key changed" and "transaction knows" does not exist. `EXEC` checks that flag, and a dirty transaction replies with a null array and executes nothing, which is exactly Redis's optimistic-locking contract.

### Pub/Sub

`pubsub` holds `map[channel]map[*subscriber]struct{}`. The interesting part is `deliver`: it copies the subscriber set under the registry lock, **releases that lock**, and only then writes to sockets. Holding the registry lock across a `conn.Write` would let one slow subscriber block every `SUBSCRIBE` and `PUBLISH` in the server. Each `subscriber` carries its own mutex, so concurrent publishers cannot interleave bytes inside a single connection's output.

### Authentication and ACL

The server starts with one user, `default`, carrying the `nopass` flag — which is why an unconfigured server answers everyone, like real Redis.

- Passwords are stored as hex-encoded SHA-256 digests, never as plaintext.
- Verification uses `crypto/subtle.ConstantTimeCompare`, so a wrong password takes the same time to reject regardless of how many leading bytes matched.
- `ACL SETUSER default >hunter2` adds a password and strips `nopass`. From that moment, a fresh connection starts unauthenticated and every command except `AUTH` is refused with `-NOAUTH Authentication required.`
- `AUTH` accepts both the one-argument (`AUTH password`, implying `default`) and two-argument (`AUTH user password`) forms.

```bash
redis-cli ACL SETUSER default '>hunter2'
redis-cli GET foo                      # -> NOAUTH Authentication required.
redis-cli -a hunter2 GET foo           # -> works
```

### Replication

**On the leader.** A `PSYNC` turns an ordinary client connection into a replica link: the server replies `+FULLRESYNC <replid> 0`, streams an RDB payload, and registers the connection in the `replicaSet`. From then on, every command in the write set (`SET`, `INCR`, `RPUSH`, `LPUSH`, `LPOP`, `BLPOP`, `XADD`, `ZADD`) is re-encoded and fanned out to all replica connections, with the leader's byte offset advanced by exactly what it wrote.

`WAIT numreplicas timeout` is the acknowledgement protocol:

1. Snapshot the current offset as the target.
2. Broadcast `REPLCONF GETACK *` to every replica.
3. Count replicas whose last-known ACK offset reached the target; return as soon as `numreplicas` is satisfied.
4. Otherwise block on a broadcast channel that `ack()` closes and replaces on every incoming `REPLCONF ACK`, re-counting on each wake, and return the partial count when the timeout fires.

That close-and-replace pattern is the same broadcast idiom used for `XREAD BLOCK`: one channel, N waiters, no per-waiter bookkeeping.

**On the follower.** `startReplication` runs the handshake in order — `PING`, `REPLCONF listening-port <port>`, `REPLCONF capa psync2`, `PSYNC ? -1` — reads the `+FULLRESYNC` line, consumes exactly the advertised number of RDB bytes, and then reuses the *same* buffered reader for the command stream. Reusing the reader matters: the leader may have pipelined commands into the same TCP segment as the RDB payload, and a fresh reader would drop whatever was already buffered.

The apply loop then feeds each command to `handleCommands` with `io.Discard` as the writer — replicated writes must mutate state without emitting replies — while tracking a byte offset so it can answer `REPLCONF GETACK` with its true position.

### Persistence

**RDB (load only).** `app/rdb` is a hand-written reader for the real on-disk format: the `REDIS` magic and version header, the 6/14/32-bit length encodings, the special integer encodings (int8/int16/int32), auxiliary metadata fields, resize hints, and both second- and millisecond-precision expiries. Keys arrive through a `SetFunc` callback, which keeps the parser free of any dependency on the keyspace. A missing file is not an error — the server simply starts empty.

**AOF (append and replay).** With `--appendonly yes`, the server lays out a Redis 7-style multi-part AOF under `<dir>/<appenddirname>/`: a manifest file naming the current incremental file, and the incr file itself holding commands in plain RESP. On boot, the manifest is read (and created if absent), the incr file is opened in append mode, and every command in it is replayed through the dispatcher — again with `io.Discard`, so a replay rebuilds state without writing replies to anyone. After that, every write command is appended as it executes, with `--appendfsync always` forcing an `fsync` per command.

Both formats reuse infrastructure built for other purposes: the AOF replayer reuses the RESP reader written for clients, and the AOF writer reuses the command encoder written for replication.

### The RESP reader

`app/resp` is a protocol reader, not a parser bolted onto a connection:

- Full multibulk support, plus **inline commands** so a raw `telnet`/`nc` session can drive the server by typing.
- **Pipelining** falls out of the design: commands are read one at a time off a buffered reader, so a client can push many requests into one write without the server noticing.
- Hard limits — 64 KiB lines, 512 MiB bulk strings, 1 Mi array elements — are enforced *before* allocating, so a hostile length prefix cannot make the server allocate on command.
- Malformed input yields a typed `*ProtocolError`, which the connection loop turns into `-ERR Protocol error: ...` before closing, exactly as Redis does.
- A truncated frame is reported as `io.ErrUnexpectedEOF`, cleanly distinguishing "client disconnected between commands" from "client disconnected mid-command".

### Geospatial encoding

`GEOADD` converts a `(longitude, latitude)` pair into a single `float64` score, so the entire geo type rides on the sorted set with no new storage:

1. Normalize each coordinate into `[0, 2^26)` over Redis's actual latitude bounds (±85.05112878, the Mercator limit).
2. Interleave the two 26-bit integers into one 52-bit Morton code via the classic bit-spreading trick — `spread` shifts and masks in five steps instead of looping over 26 bits.
3. `decode` reverses it: de-interleave, recover each grid cell, and return the cell's center point — which is why `GEOPOS` returns coordinates very close to, but not bit-identical to, what was stored. Real Redis behaves the same way.

Distances use the haversine formula with Redis's own earth radius constant (6372797.560856 m), so `GEODIST` output matches real Redis to within floating-point noise.

### Graceful shutdown

On `SIGINT`/`SIGTERM`:

1. The root context is cancelled.
2. The listener is closed, unblocking `Accept`; the accept loop distinguishes "we are shutting down" from a transient accept error by checking `ctx.Done()`.
3. The sweeper exits through its `ctx.Done()` case.
4. Each connection has a small watcher goroutine that calls `SetReadDeadline(time.Now())` when the context is cancelled, forcing a blocked `Read` to return — `net.Conn` does not honour contexts on its own.
5. Blocked `BLPOP` and `XREAD BLOCK` calls return through their `ctx.Done()` cases.
6. `main` waits on a `sync.WaitGroup` covering the sweeper, the replication client, and every live connection, then prints the final goroutine count as a leak check.

---

## Testing

```bash
go test -race ./...
```

The suite covers the store's data operations (`SET`/`GET` with and without TTLs, list push/pop/range edge cases, stream ID validation and `WRONGTYPE` enforcement) and the protocol reader (well-formed frames, pipelined commands, torn input, and malformed-length rejection).

`TestStoreConcurrent` hammers the keyspace with ten goroutines interleaving reads and writes on both shared and private keys; under `-race` it is the fast check that the locking discipline holds. Run it repeatedly to explore more interleavings:

```bash
go test -race -run TestStoreConcurrent -count=50 ./app
```

### Manual checks with `redis-cli`

Blocking hand-off across connections:

```bash
# terminal A: blocks
redis-cli BLPOP tasks 0

# terminal B: wakes terminal A
redis-cli RPUSH tasks hello
```

Blocking stream reads:

```bash
# terminal A: waits for entries added from now on
redis-cli XREAD BLOCK 0 STREAMS events '$'

# terminal B
redis-cli XADD events '*' temperature 21
```

Optimistic locking:

```bash
redis-cli -x MULTI   # or, interactively:
#   WATCH balance
#   MULTI
#   INCR balance
#   (change balance from another client)
#   EXEC          -> (nil), the transaction aborted
```

Pub/sub fan-out:

```bash
redis-cli SUBSCRIBE news          # terminal A
redis-cli PUBLISH news hello      # terminal B -> (integer) 1
```

Other useful probes:

- `BLPOP empty 2` returns `(nil)` after two seconds.
- Several clients blocked on one key, then `RPUSH tasks a b`: exactly two wake, in the order they blocked.
- Press Ctrl+C on the server while a client is blocked: it should exit cleanly instead of hanging, and report a small goroutine count.

---

## Known limitations

Deliberate simplifications, listed so nobody has to discover them the hard way:

**Commands and options**
- `SET` accepts only `PX`; `EX`, `NX`, `XX`, `GET`, and `KEEPTTL` are not implemented.
- `KEYS` ignores its pattern argument and returns every key.
- `ZADD` takes exactly one score/member pair, and `ZRANGE` has no `WITHSCORES`.
- `BITOP` supports only `AND` and `OR`, and exactly two source keys.
- `GEOSEARCH` supports only `FROMLONLAT ... BYRADIUS` and treats the radius as meters whatever unit is given.
- `PSUBSCRIBE`/`PUNSUBSCRIBE` are allowed through subscriber mode but have no implementation yet.
- No `HELLO`, so RESP3 is unavailable; `SELECT` and multiple databases are not supported.
- `LPOP key count` with a `count` larger than the list will panic rather than clamping.
- The list push paths never stamp a type tag on the entry, so `TYPE` on a list answers an empty simple string instead of `+list`.

**Replication**
- `PSYNC` always serves a hardcoded *empty* RDB, so a full resync does not transfer the leader's existing keys — only writes from that point forward replicate.
- `INFO` reports `master_repl_offset:0` unconditionally.
- Only the eight commands in the write set propagate; `SETBIT`, `ZREM`, `GEOADD`, and friends do not. Commands run inside `MULTI`/`EXEC` are not propagated or AOF-appended either.
- Expiry is evaluated independently on each node rather than being driven by the leader, so a replica can expire a key on its own clock.

**Persistence**
- RDB is load-only — there is no `SAVE`/`BGSAVE`, and only the string type and non-LZF encodings are understood.
- `--appendfsync everysec` and `no` currently leave flushing to the OS; there is no background fsync goroutine, so only `always` is genuinely durable.
- There is no AOF rewrite and no base file, so the incr file grows without bound.

**Concurrency and performance**
- A handful of read paths (`LLEN`, `TYPE`, `ZCARD`, `ZSCORE`, `ZREM`, `KEYS`) touch the map without holding the lock and are racy under concurrent writes.
- The sweeper scans the entire keyspace under the write lock. Fine for small datasets; with millions of keys it would stall every client. Real Redis samples ~20 random keys per cycle instead.
- `LPUSH` is O(n) per element because lists are Go slices; real Redis uses a quicklist for O(1) head insertion.
- One global lock serializes all writes. Sharding the keyspace would raise write throughput.
- Shutdown waits without a deadline, so a wedged handler would stall it indefinitely; production servers bound this.

## Roadmap

- `EX`/`NX`/`XX` and the rest of the `SET` option set
- Real `KEYS` glob matching, plus `SCAN`
- Hashes (`HSET`/`HGET`/`HGETALL`) and sets (`SADD`/`SMEMBERS`)
- RDB *writing* (`SAVE`/`BGSAVE`) and a real snapshot in `PSYNC`'s full resync
- AOF rewrite and a background `everysec` fsync
- Pattern subscriptions (`PSUBSCRIBE`) and `RESET`
- Partial resynchronization (`PSYNC <replid> <offset>` with a backlog)
- RESP3 via `HELLO`, and client-side keyspace notifications
