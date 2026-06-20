# kv_cache

A Redis-compatible in-memory key-value cache written in Go, with a low-level event loop modeled after Redis. Uses **kqueue** on macOS, **epoll** on Linux, and goroutine-per-connection on other platforms.

## Requirements

- Go 1.22+
- No external runtime dependencies (only `golang.org/x/sys` for syscall access)

## Build & Run

```bash
# Download dependencies
go mod tidy

# Run with defaults (binds to 0.0.0.0:6380)
go run .

# With options
go run . -addr 127.0.0.1:6380 -maxkeys 10000 -maxconns 1000

# Build binary
go build -o kv_cache .
./kv_cache -addr 0.0.0.0:6380
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `0.0.0.0:6380` | TCP address to listen on |
| `-maxkeys` | `0` | Max keys in store (0 = unlimited). Evicts LRU key when full. |
| `-maxconns` | `0` | Max concurrent connections (0 = unlimited) |

## Connecting

Any Redis client works — the server speaks RESP2.

```bash
# redis-cli
redis-cli -p 6380 PING
redis-cli -p 6380 SET hello world EX 60
redis-cli -p 6380 GET hello

# netcat (inline commands, useful without redis-cli)
printf "PING\r\n" | nc -w1 127.0.0.1 6380
printf "SET foo bar\r\nGET foo\r\n" | nc -w1 127.0.0.1 6380
```

## Supported Commands

### String
| Command | Syntax |
|---------|--------|
| `SET` | `SET key value [EX sec] [PX ms] [NX] [XX] [GET] [KEEPTTL]` |
| `GET` | `GET key` |
| `GETDEL` | `GETDEL key` |
| `GETSET` | `GETSET key value` |
| `GETEX` | `GETEX key [EX sec \| PX ms \| EXAT ts \| PXAT ts \| PERSIST]` |
| `MSET` | `MSET key value [key value ...]` |
| `MSETNX` | `MSETNX key value [key value ...]` |
| `MGET` | `MGET key [key ...]` |
| `APPEND` | `APPEND key value` |
| `STRLEN` | `STRLEN key` |
| `INCR` | `INCR key` |
| `INCRBY` | `INCRBY key increment` |
| `INCRBYFLOAT` | `INCRBYFLOAT key increment` |
| `DECR` | `DECR key` |
| `DECRBY` | `DECRBY key decrement` |
| `SETNX` | `SETNX key value` |
| `SETEX` | `SETEX key seconds value` |
| `PSETEX` | `PSETEX key milliseconds value` |

### Key
| Command | Syntax |
|---------|--------|
| `DEL` | `DEL key [key ...]` |
| `UNLINK` | `UNLINK key [key ...]` |
| `EXISTS` | `EXISTS key [key ...]` |
| `EXPIRE` | `EXPIRE key seconds` |
| `PEXPIRE` | `PEXPIRE key milliseconds` |
| `EXPIREAT` | `EXPIREAT key unix-time-seconds` |
| `PEXPIREAT` | `PEXPIREAT key unix-time-milliseconds` |
| `TTL` | `TTL key` |
| `PTTL` | `PTTL key` |
| `PERSIST` | `PERSIST key` |
| `KEYS` | `KEYS pattern` (glob: `*`, `?`, `[abc]`) |
| `RANDOMKEY` | `RANDOMKEY` |
| `RENAME` | `RENAME oldkey newkey` |
| `RENAMENX` | `RENAMENX oldkey newkey` |
| `TYPE` | `TYPE key` |
| `OBJECT` | `OBJECT ENCODING\|REFCOUNT\|IDLETIME\|HELP key` |

### Server
| Command | Syntax |
|---------|--------|
| `DBSIZE` | `DBSIZE` |
| `FLUSHALL` | `FLUSHALL [ASYNC\|SYNC]` |
| `FLUSHDB` | `FLUSHDB [ASYNC\|SYNC]` |
| `SELECT` | `SELECT index` (only DB 0) |
| `INFO` | `INFO [section]` |
| `CONFIG GET` | `CONFIG GET parameter` |
| `CONFIG SET` | `CONFIG SET parameter value` |
| `CLIENT LIST\|ID\|GETNAME\|SETNAME\|INFO` | |
| `SAVE` / `BGSAVE` / `BGREWRITEAOF` | |
| `LASTSAVE` | |
| `SLOWLOG GET\|LEN\|RESET` | |
| `LATENCY RESET\|HISTORY\|LATEST` | |
| `DEBUG SLEEP\|SET-ACTIVE-EXPIRE` | |

### Connection
| Command | Syntax |
|---------|--------|
| `PING` | `PING [message]` |
| `ECHO` | `ECHO message` |
| `QUIT` | `QUIT` |
| `COMMAND` | `COMMAND` |

## Architecture

```
kv_cache/
├── main.go                  # Entry point, flag parsing, graceful shutdown
├── resp/resp.go             # RESP2 protocol parser and serializer
├── store/
│   ├── store.go             # Thread-safe KV store (sync.RWMutex + hash map)
│   ├── lru.go               # O(1) LRU eviction (doubly-linked list + map)
│   └── ttl.go               # Background expiry sweep (every 100ms)
├── server/
│   ├── conn.go              # Shared Config struct
│   ├── server_darwin.go     # kqueue event loop (macOS)
│   ├── server_linux.go      # epoll edge-triggered event loop (Linux)
│   └── server_other.go      # Goroutine-per-connection fallback
└── commands/commands.go     # Command dispatcher (50+ commands)
```

### Event Loop

**macOS — kqueue** (`server_darwin.go`)
- Single-threaded reactor loop via `unix.Kevent`
- Non-blocking TCP sockets (`O_NONBLOCK`, `TCP_NODELAY`)
- `EVFILT_READ` for incoming data, `EVFILT_WRITE` for backpressure
- Wake pipe for clean shutdown

**Linux — epoll** (`server_linux.go`)
- Edge-triggered (`EPOLLET`) epoll loop via `unix.EpollWait`
- `Accept4` with `SOCK_NONBLOCK` for zero-syscall setup
- `eventfd` for shutdown wakeup
- Switches to `EPOLLOUT` when write buffer is non-empty

**Other — goroutine-per-connection** (`server_other.go`)
- Standard `net.Listen` + one goroutine per accepted connection
- Blocking reads via `bufio.Reader`

### Partial Read Safety

The event loop accumulates raw bytes in a per-connection buffer (`rdBuf []byte`). On each READ event, available bytes are appended to the buffer. RESP parsing then runs over a `bytes.NewReader` wrapper — consumed bytes are calculated as:

```
consumed = total - bytes.Reader.Len() - bufio.Reader.Buffered()
```

This ensures partial commands (split across TCP segments) are held until the full command arrives, with no data loss.

### Store Internals

- **Thread safety**: `sync.RWMutex` — read lock for reads, write lock for mutations
- **LRU eviction**: `container/list` doubly-linked list + `map[string]*list.Element` for O(1) access, touch, and eviction
- **TTL**: each entry stores an `expiresAt time.Time`; checked lazily on read and swept actively every 100ms by a background goroutine
- **Incr/Decr**: values are stored as strings; integer operations parse and re-serialize as `strconv.ParseInt`/`FormatInt`

## Quick Example

```bash
$ go run . -addr 127.0.0.1:6380 -maxkeys 1000
2026/06/15 10:00:00 kv-cache listening on 127.0.0.1:6380 (kqueue event loop)
```

```bash
# In another terminal
$ redis-cli -p 6380
127.0.0.1:6380> SET user:1 alice EX 300
OK
127.0.0.1:6380> GET user:1
"alice"
127.0.0.1:6380> TTL user:1
(integer) 299
127.0.0.1:6380> INCR visits
(integer) 1
127.0.0.1:6380> INCRBY visits 9
(integer) 10
127.0.0.1:6380> MSET k1 v1 k2 v2 k3 v3
OK
127.0.0.1:6380> KEYS *
1) "k1"
2) "k2"
3) "k3"
4) "user:1"
5) "visits"
127.0.0.1:6380> DBSIZE
(integer) 5
127.0.0.1:6380> INFO server
# Server
redis_version:7.0.0
tcp_port:6380
uptime_in_seconds:12
...
```

## Graceful Shutdown

Send `SIGINT` or `SIGTERM` to stop the server cleanly:

```bash
kill -SIGTERM <pid>
# or Ctrl+C
```
