# TQCache Limitations

This document describes the known limitations, test exclusions, and differences from Memcached behavior in TQCache.

## Test Suite Results

**Current Status:** 38,122 passed / 14 failed / 9 skipped (38,145 total tests, 99.96% pass rate)

| Test File | Pass | Fail | Skip | Total | Status |
|-----------|------|------|------|-------|--------|
| cas.t | 38 | 0 | 4 | 42 | PASS |
| incrdecr.t | 23 | 0 | 0 | 23 | PASS |
| noreply.t | 9 | 0 | 0 | 9 | PASS |
| touch.t | 4 | 0 | 0 | 4 | PASS |
| getset.t | 37989 | 2 | 1 | 37992 | Partial |
| expirations.t | 36 | 5 | 0 | 41 | Partial |
| flush-all.t | 18 | 4 | 4 | 26 | Partial |
| flags.t | 5 | 3 | 0 | 8 | Partial |

---

## Disabled Tests

### cas.t - check_args Tests (4 tests disabled)

The `check_args` function tests are disabled because they spawn new server connections for each validation test, which doesn't work with the test harness configuration.

```perl
# Disabled for TQCache: check_args
# check_args "cas bad blah 0 0 0\r\n\r\n", "bad flags";
# check_args "cas bad 0 blah 0 0\r\n\r\n", "bad exp";
# check_args "cas bad 0 0 blah 0\r\n\r\n", "bad cas";
# check_args "cas bad 0 0 0 blah\r\n\r\n", "bad size";
```

### getset.t - Long Line Without Newline Test (1 subtest disabled)

The test for connection close on oversized commands without a terminating newline is disabled. This edge case tests Memcached's internal buffer limit behavior which requires byte-by-byte timeout handling.

```perl
# Disabled for TQCache: subtest
# subtest 'close if no get found in 2k' => sub { ... }
```

---

## Known Failures

### 1. flags.t - Flags Not Stored (3 failures)

**Affected Tests:** 4, 6, 8

**Description:** TQCache does not store the flags field. GET always returns `0` for flags regardless of what was set.

```
set foo 123 0 6\r\nfooval\r\n  → STORED
get foo\r\n                   → VALUE foo 0 6\r\n (flags=0, not 123)
```

**Reason:** Intentional design decision to reduce memory overhead. The flags field is validated but not persisted.

---

### 2. expirations.t - Time Simulation Not Supported (5 failures)

**Affected Tests:** 3, 8, 16, 17, 36

**Description:** These tests use `mem_move_time($sock, N)` to simulate time advancement for TTL testing. TQCache does not support debug time manipulation commands.

**Failing Scenarios:**
- Test 3: Set with `exptime=3`, advance 3 seconds, expect expired
- Test 8: Set with future Unix timestamp, advance time, expect expired
- Test 16-17: Add after expiration via time advancement
- Test 36: GAT with time advancement

**Reason:** TQCache uses real-time expiration. The `mem_move_time` helper likely sends a debug command that TQCache doesn't implement.

---

### 3. flush-all.t - Delayed Flush Not Implemented (4 failures)

**Affected Tests:** 14, 18, 20, 22

**Description:** The `flush_all <delay>` command with a delay argument is not implemented.

```
flush_all 2\r\n  → Immediate flush occurs, not delayed
```

**Reason:** Delayed flush functionality not yet implemented. `flush_all` always executes immediately.

---

### 4. getset.t - Key Retention After Size Rejection (2 failures)

**Affected Tests:** 536, 539 (keys `foo_1049600`, `foo_1051648`)

**Description:** When a SET with a value exceeding 1MB is rejected with `SERVER_ERROR object too large for cache`, the existing key value is retained instead of being deleted.

```
set foo_1049600 0 0 3\r\nMOO\r\n        → STORED
set foo_1049600 0 0 1049600\r\n<data>   → SERVER_ERROR object too large for cache
get foo_1049600\r\n                     → VALUE foo_1049600 0 3\r\nMOO (still exists)
```

**Expected:** Key should not exist after a failed oversized SET.

**Reason:** Current implementation rejects the oversized value but doesn't delete the existing key.

---

## Protocol Differences

### 1. Binary Protocol Flags

Similar to text protocol, binary protocol does not persist the flags field.

### 2. Maximum Value Size

TQCache enforces the same 1MB maximum value size as Memcached. Values exceeding this limit return:
```
SERVER_ERROR object too large for cache
```

### 3. Maximum Key Size

Maximum key size is 250 bytes, matching Memcached.

---

## Unsupported Commands

The following Memcached commands are not implemented:

| Command | Description |
|---------|-------------|
| `stats slabs` | Slab allocator statistics |
| `stats items` | Item statistics per slab |
| `stats sizes` | Size distribution |
| `stats cachedump` | Cache dump |
| `watch` | Log watching |
| `lru_crawler` | LRU crawler commands |
| `debug` | Debug commands |

