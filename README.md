# TQCache

TQCache is a high-performance, persistent session storage server. It provides a
Memcached-compatible interface with disk-based persistence, making it ideal for
session storage that survives restarts.

Blog post: https://www.tqdev.com/2026-tqcache-memcache-redis-alternative

## Features

- **Session Storage**: Ideal for PHP's native `memcached` session handler
- **Persistent Storage**: Data stored on disk, survives server restarts
- **Efficient Storage**: About 25-33% waste space on disk on average
- **Faster than Redis**: More than 50% faster than Redis in typical cases
- **Memcached Compatible**: Supports all Memcached commands, text and binary
- **TTL Enforcement**: Maximum TTL defaults to 24 hours (set to 0 to disable)
- **No Eviction**: No LRU eviction support; Use max-ttl to limit diskspace usage

## Requirements

- Go 1.21 or later

## Installation

```bash
go install github.com/mevdschee/tqcache/cmd/tqcache@latest
```

Or build from source:

```bash
git clone https://github.com/mevdschee/tqcache.git
cd tqcache
go build -o tqcache ./cmd/tqcache
```

## Usage

```bash
tqcache [options]
```

### Command-Line Flags

| Flag             | Default    | Description                                                       |
| ---------------- | ---------- | ----------------------------------------------------------------- |
| `-config`        |            | Path to [config file](cmd/tqcache/tqcache.conf) (overrides flags) |
| `-listen`        | `:11211`   | Address to listen on (`[address]:port`)                           |
| `-data-dir`      | `data`     | Directory for persistent data files                               |
| `-shards`        | `16`       | Number of shards for parallel processing                          |
| `-default-ttl`   | `0`        | Default TTL for keys (`0` = no expiry)                            |
| `-max-ttl`       | `24h`      | Maximum TTL cap for any key (`0` = unlimited)                     |
| `-sync-mode`     | `periodic` | Sync mode: `none`, `periodic`, `always`                           |
| `-sync-interval` | `1s`       | Interval between fsync calls (when periodic)                      |

**Fixed limits:** Max key size is 1KB. Max value size is 64MB.

## PHP Configuration

Configure PHP to use TQCache as the session handler:

```ini
session.save_handler = memcached
session.save_path = "localhost:11211"
```

NB: Set "max-ttl = 24h" to prevent disk space exhaustion.

## Performance

**TQCache vs Redis** (Periodic Sync Mode - Default Configuration)

Benchmarks were run on a local development environment (Linux, Loopback).
100,000 keys were used for the benchmark, each with a size of 10KB.

### Benchmark Results

![Performance Benchmark](benchmarks/getset/getset_benchmark_periodic.png)

### Performance Highlights (16 shards)

- **Write (SET)**: +60% faster than Redis (~96k RPS vs ~60k RPS) via socket,
  or 3x faster (~180k RPS) using the package directly.
- **Read (GET)**: +84% faster than Redis (~184k RPS vs ~100k RPS) via socket,
  or 5x faster (~498k RPS) using the package directly.
- **Memory**: Uses ~20x less memory than Redis (~70-83MB vs ~1372MB),
  relies on OS disk cache.
- **CPU**: Uses ~4 cores (shards/4) compared to Redis's single-threaded ~1 core.

### Summary Table

| Backend                  | SET (RPS) | GET (RPS) | Memory (MB) | CPU Usage  |
| :----------------------- | :-------- | :-------- | :---------- | :--------- |
| **TQCache** (socket)     | ~96k      | ~184k     | ~70MB       | ~4 cores   |
| **TQCache** (package)    | ~180k     | ~498k     | ~83MB       | ~3.5 cores |
| **Redis**                | ~60k      | ~100k     | ~1372MB     | ~1 core    |

NB: Package mode calls TQCache directly without network overhead.

## Testing

```bash
go test ./pkg/tqcache/...
```

## Architecture

TQCache stores session data on disk in a fixed-size record formats and holds
several memory data structures to speed up access. It assumes SSD performance
with good random I/O and enough free memory to let the OS keep the disk blocks
in the cache. It does not use append-only files.

See [PROJECT_BRIEF.md](PROJECT_BRIEF.md) for detailed architecture.
