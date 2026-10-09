# FlashKV (C++)

A small, multi-threaded, Redis-compatible in-memory key/value server written in C++ from scratch using raw POSIX sockets. It speaks the **RESP** protocol, so it works with `redis-cli` and `redis-benchmark`.

---

## Table of Contents

1. [Features](#features)
2. [Build & Run](#build--run)
3. [Architecture Overview](#architecture-overview)
4. [Code Walkthrough](#code-walkthrough)
   - [Data Model](#1-data-model)
   - [Time Handling](#2-time-handling-now_ms)
   - [RESP Parser](#3-resp-parser-respparser)
   - [Client Handler](#4-client-handler-handle_client)
   - [Command Reference](#5-commands)
   - [Server Startup (`main`)](#6-server-startup-main)
5. [Threading & Concurrency](#threading--concurrency-model)
6. [Key Expiry (TTL)](#key-expiry-ttl)
7. [Example Session](#example-session)
8. [Known Limitations](#known-limitations)
9. [Ideas for Future Work](#ideas-for-future-work)

---

## Features

| Feature | Details |
|---|---|
| Protocol | RESP arrays of bulk strings **and** inline commands (used by `redis-benchmark`) |
| Commands | `PING`, `ECHO`, `SET` (with `PX`), `GET`, `CONFIG` (stubbed) |
| Expiry | Millisecond TTL via `SET key value PX <ms>`, lazily enforced on `GET` |
| Concurrency | One detached thread per client, shared store protected by a mutex |
| Pipelining | Multiple commands in one TCP read are all parsed and executed |
| Case-insensitive | `get`, `GET`, `Get` all work |
| Port | `6379` (default Redis port), bound on `0.0.0.0` |

---

## Build & Run

```bash
# Compile (C++11 or newer, pthreads required)
g++ -std=c++17 -O2 -pthread main.cpp -o mini-redis

# Run
./mini-redis
```

Talk to it using the real Redis tools:

```bash
redis-cli -p 6379 PING
redis-benchmark -p 6379 -t set,get -n 100000 -q
```

---

## Architecture Overview

```mermaid
flowchart LR
    subgraph Clients
        C1["redis-cli"]
        C2["redis-benchmark"]
        C3["Any RESP client"]
    end

    subgraph Server["mini-redis process"]
        direction TB
        L["Listener socket<br/>0.0.0.0:6379<br/>main thread"]
        T1["Thread 1<br/>handle_client"]
        T2["Thread 2<br/>handle_client"]
        T3["Thread N<br/>handle_client"]
        P1["RespParser 1"]
        P2["RespParser 2"]
        P3["RespParser N"]
        M{{"store_mutex"}}
        S[("store<br/>unordered_map of string to Value")]
    end

    C1 -- TCP --> L
    C2 -- TCP --> L
    C3 -- TCP --> L

    L -- "accept and spawn" --> T1
    L -- "accept and spawn" --> T2
    L -- "accept and spawn" --> T3

    T1 --- P1
    T2 --- P2
    T3 --- P3

    T1 --> M
    T2 --> M
    T3 --> M
    M --> S
```

**In one sentence:** the main thread only accepts connections; every client gets its own thread with its own private parser; all threads share one global hash map guarded by a single mutex.

---

## Code Walkthrough

### 1. Data Model

```cpp
struct Value {
    string data;
    long long expiry;   // absolute time in ms, or -1 = never expires
};

unordered_map<string, Value> store;
mutex store_mutex;
```

```mermaid
classDiagram
    class Value {
        +string data
        +long long expiry
    }
    class store {
        <<global unordered_map>>
        +string key
        +Value value
    }
    class store_mutex {
        <<global mutex>>
        +lock()
        +unlock()
    }
    store "1" o-- "*" Value : holds
    store_mutex ..> store : guards
```

- `data` is the stored string.
- `expiry` is an **absolute** deadline in milliseconds (`now_ms() + ttl`), not a countdown. `-1` is a sentinel meaning "no expiry".
- `store` and `store_mutex` are globals, shared by every client thread.

---

### 2. Time Handling (`now_ms`)

```cpp
long long now_ms() {
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}
```

It uses `std::chrono::steady_clock` (a **monotonic** clock) instead of `system_clock`. That means key expiry is not affected if someone changes the system date/time, or if NTP adjusts the clock.

---

### 3. RESP Parser (`RespParser`)

TCP is a **stream**, not a message protocol. One `recv()` can contain half a command, exactly one command, or several commands. `RespParser` solves this by buffering bytes and only returning a command when a complete frame is available.

```cpp
class RespParser {
    string buffer;                               // bytes received but not yet consumed
    void append(const char* data, int len);      // push new bytes in
    bool parse(vector<string>& result);          // try to extract ONE full command
};
```

#### What RESP looks like on the wire

`SET name alice` is sent by `redis-cli` as:

```
*3\r\n          <- array of 3 elements
$3\r\n          <- bulk string, 3 bytes
SET\r\n
$4\r\n
name\r\n
$5\r\n
alice\r\n
```

#### Parsing flow

```mermaid
flowchart TD
    A(["parse() called"]) --> B{"buffer empty?"}
    B -- yes --> RF(["return false<br/>need more data"])
    B -- no --> C{"first byte is star?"}

    C -- "no: inline command" --> D{"found CRLF?"}
    D -- no --> RF
    D -- yes --> E["Cut one line from buffer<br/>and erase it"]
    E --> F["Split line on spaces into tokens"]
    F --> RT(["return true"])

    C -- "yes: RESP array" --> G{"found CRLF after header?"}
    G -- no --> RF
    G -- yes --> H["num_elements = stoi of header"]
    H --> I["idx = start of first element"]
    I --> J{"more elements<br/>to read?"}

    J -- yes --> K{"idx past buffer end?"}
    K -- yes --> RF
    K -- no --> L{"buffer at idx is dollar sign?"}
    L -- no --> RF
    L -- yes --> M{"found CRLF after length?"}
    M -- no --> RF
    M -- yes --> N["str_len = stoi of length"]
    N --> O{"all str_len bytes + CRLF<br/>already in buffer?"}
    O -- no --> RF
    O -- yes --> P["Extract argument<br/>push into result<br/>advance idx"]
    P --> J

    J -- "no: all elements read" --> Q["Erase consumed bytes<br/>buffer.erase(0, idx)"]
    Q --> RT
```

Key design points:

- **Incomplete frames are never consumed.** If anything is missing, `parse` returns `false` *without* touching the buffer. Next time more bytes arrive, parsing restarts from the beginning of the same frame.
- **Complete frames are erased** from the buffer, so any bytes that follow (the next pipelined command) remain ready for the next `parse()` call.
- **Inline protocol** (e.g. plain `PING\r\n`) is handled first. `redis-benchmark` and `telnet`/`nc` sessions use it.
- **Length-prefixed** reading means values can safely contain spaces, `\r\n`, or any binary bytes.

---

### 4. Client Handler (`handle_client`)

Every accepted connection runs this function in its own thread.

```mermaid
flowchart TD
    S(["Thread starts with client_fd"]) --> R["recv up to 1024 bytes"]
    R --> Q{"bytes_received <= 0?"}
    Q -- "yes: closed or error" --> X["close(client_fd)"]
    X --> E(["Thread exits"])
    Q -- no --> AP["parser.append(buf, n)"]
    AP --> PL{"parser.parse(cmd)<br/>returns true?"}
    PL -- no --> R
    PL -- yes --> EM{"cmd empty?"}
    EM -- yes --> PL
    EM -- no --> UP["Uppercase cmd[0]"]
    UP --> D["Dispatch to command handler"]
    D --> SN["send() RESP reply"]
    SN --> PL
```

The **inner `while (parser.parse(cmd))` loop** is what gives you pipelining: after one `recv()`, it keeps executing commands until the parser says the buffer holds no more complete frames, and only then goes back to `recv()`.

---

### 5. Commands

#### Dispatch overview

```mermaid
flowchart LR
    CMD["command name<br/>uppercased"] --> SW{"which one?"}
    SW -- PING --> PING["+PONG"]
    SW -- ECHO --> ECHO["bulk string of arg"]
    SW -- SET --> SET["store key, reply +OK"]
    SW -- GET --> GET["lookup, check TTL"]
    SW -- CONFIG --> CFG["hardcoded reply"]
    SW -- other --> ERR["-ERR unknown command"]
```

#### Command table

| Command | Syntax | Reply | Notes |
|---|---|---|---|
| `PING` | `PING` | `+PONG` | Simple string reply |
| `ECHO` | `ECHO msg` | `$len\r\nmsg` | Bulk string. Error if no argument |
| `SET` | `SET key value [PX ms]` | `+OK` | Needs at least 3 tokens. `PX` sets a TTL in ms |
| `GET` | `GET key` | `$len\r\nvalue` or `$-1` | `$-1` is the RESP "null bulk string" (key missing or expired) |
| `CONFIG` | `CONFIG ...` | Array with `maxmemory` / `0` | Stub so `redis-benchmark` can finish its startup handshake |
| anything else | | `-ERR unknown command` | |

Error replies use the RESP error prefix `-ERR ...`.

#### SET in detail

```mermaid
flowchart TD
    A(["SET received"]) --> B{"cmd.size() < 3?"}
    B -- yes --> ERR["send -ERR wrong number of arguments"]
    B -- no --> C["key = cmd[1]<br/>value = cmd[2]<br/>expiry = -1"]
    C --> D{"cmd.size() >= 5?"}
    D -- no --> G
    D -- yes --> E["opt = uppercase cmd[3]"]
    E --> F{"opt == PX?"}
    F -- yes --> F2["expiry = now_ms() + stoll(cmd[4])"]
    F -- no --> G
    F2 --> G["lock store_mutex"]
    G --> H["store[key] = value, expiry<br/>overwrites any old value"]
    H --> I["unlock"]
    I --> J["send +OK"]
```

#### GET in detail

```mermaid
flowchart TD
    A(["GET received"]) --> B{"cmd.size() < 2?"}
    B -- yes --> ERR["send -ERR wrong number of arguments"]
    B -- no --> C["lock store_mutex"]
    C --> D{"key exists in store?"}
    D -- no --> NF["found = false"]
    D -- yes --> E{"expiry != -1<br/>AND now_ms() > expiry?"}
    E -- yes --> F["Lazy delete:<br/>store.erase(it)"]
    F --> NF
    E -- no --> G["value = data<br/>found = true"]
    NF --> U["unlock"]
    G --> U
    U --> H{"found?"}
    H -- yes --> I["send $len, value"]
    H -- no --> J["send $-1 null reply"]
```

Note that the **response is sent after the lock is released**. Network I/O can be slow, and holding the mutex during `send()` would block every other client.

---

### 6. Server Startup (`main`)

```mermaid
sequenceDiagram
    autonumber
    participant OS as Operating System
    participant M as main thread

    M->>OS: socket(AF_INET, SOCK_STREAM)
    OS-->>M: server_fd
    M->>OS: setsockopt(SO_REUSEADDR)
    Note right of M: allows instant restart on the same port
    M->>OS: bind(0.0.0.0:6379)
    M->>OS: listen(backlog = 5)
    loop forever
        M->>OS: accept()
        OS-->>M: client_fd
        M->>M: spawn detached thread running handle_client(client_fd)
    end
```

Details:

- `AF_INET` + `SOCK_STREAM` = IPv4 TCP.
- `SO_REUSEADDR` avoids the "Address already in use" error when restarting quickly.
- `INADDR_ANY` means the server listens on all network interfaces.
- `std::unitbuf` makes `cout`/`cerr` flush after every write, so logs appear instantly.
- `.detach()` lets each client thread run independently; the main thread never joins them.

---

## Threading & Concurrency Model

```mermaid
sequenceDiagram
    autonumber
    participant A as Client A thread
    participant B as Client B thread
    participant L as store_mutex
    participant S as store

    A->>L: lock
    activate L
    A->>S: write key1
    A->>L: unlock
    deactivate L
    Note over B: B was waiting for the lock
    B->>L: lock
    activate L
    B->>S: read key1
    B->>L: unlock
    deactivate L
    A->>A: send reply (outside lock)
    B->>B: send reply (outside lock)
```

- **Per-client state is private.** Each thread has its own `buf` and its own `RespParser`, so no locking is needed for parsing.
- **Only `store` is shared.** Every access, read or write, goes through `lock_guard<mutex>`, which unlocks automatically when the scope ends (RAII), even on early exit.
- **Critical sections are tiny.** Only the map operation sits inside the lock; formatting and `send()` happen outside.

---

## Key Expiry (TTL)

This server uses **lazy expiration**: nothing runs in the background. A key's expiry is only checked at the moment it is read.

```mermaid
stateDiagram-v2
    [*] --> Live: SET key value PX 5000
    Live --> Live: GET before deadline returns value
    Live --> Expired: now_ms passes expiry
    Expired --> Deleted: next GET sees expired key and erases it
    Deleted --> [*]: GET returns null
    Live --> Live: SET again overwrites value and TTL
```

Timeline example:

```mermaid
gantt
    title SET foo bar PX 3000
    dateFormat X
    axisFormat %ss
    section Key lifetime
    Readable          :active, 0, 3
    Logically expired :crit, 3, 6
    section Client
    GET returns bar   :milestone, 1, 0
    GET returns null  :milestone, 4, 0
```

> A key that is never read again after expiring stays in memory forever (see [limitations](#known-limitations)).

---

## Example Session

```text
$ redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> ECHO "hello world"
"hello world"
127.0.0.1:6379> SET name alice
OK
127.0.0.1:6379> GET name
"alice"
127.0.0.1:6379> SET session abc123 PX 2000
OK
127.0.0.1:6379> GET session
"abc123"
   ... wait 2+ seconds ...
127.0.0.1:6379> GET session
(nil)
127.0.0.1:6379> FLUSHALL
(error) ERR unknown command
```

Full request/response lifecycle for one command:

```mermaid
sequenceDiagram
    autonumber
    participant C as redis-cli
    participant H as handle_client thread
    participant P as RespParser
    participant S as store

    C->>H: *3 / $3 SET / $4 name / $5 alice
    H->>P: append(bytes)
    H->>P: parse(cmd)
    P-->>H: true, cmd = SET, name, alice
    H->>S: lock, store[name] = alice, unlock
    H-->>C: +OK
    H->>P: parse(cmd)
    P-->>H: false, buffer empty
    H->>H: back to recv()
```

---

## Known Limitations

These are things I would be upfront about if someone reviews the project:

| Area | Limitation |
|---|---|
| **Commands** | Only 5 commands. No `DEL`, `EXISTS`, `INCR`, `EXPIRE`, `TTL`, lists, hashes, etc. |
| **SET options** | Only `PX` is understood. `EX`, `NX`, `XX`, `KEEPTTL` are silently ignored (the key is still set, without TTL) |
| **CONFIG** | Returns a fixed reply regardless of arguments. It only exists to satisfy `redis-benchmark` |
| **Expiry** | Lazy only. Expired keys that are never read again are never freed (memory leak for TTL-heavy workloads) |
| **Parser robustness** | `stoi`/`stoll` throw on non-numeric input and are not wrapped in try/catch, so a malformed request can crash the whole process. A frame that has `*` but no `$` returns `false` forever and stalls that connection |
| **Scalability** | One OS thread per client does not scale to thousands of connections. One global mutex serializes all store access |
| **Networking** | `send()` return values are not checked (partial writes ignored). `accept()` errors are not checked. `listen` backlog is only 5. Port 6379 is hardcoded |
| **Replies** | Each reply is sent with its own `send()` call, so pipelined commands cause many small syscalls instead of one batched write |
| **Binary safety** | `buf` is 1024 bytes per `recv`. Large values work since they accumulate in the parser buffer, but there is no max-size limit (a client can make the buffer grow unbounded) |
| **Persistence** | Purely in-memory. Everything is lost on restart |
| **Security** | No authentication, no TLS, listens on all interfaces |
| **Shutdown** | `close(server_fd)` after the infinite loop is unreachable; there is no signal handling or graceful shutdown |

---

## Ideas for Future Work

```mermaid
mindmap
  root((Mini Redis))
    More commands
      DEL
      EXISTS
      INCR and DECR
      EXPIRE and TTL
      SET with EX NX XX
      KEYS
    Performance
      epoll event loop
      Thread pool
      Sharded locks
      Batched reply writes
    Expiry
      Background sweeper thread
      Min-heap of deadlines
    Robustness
      try/catch around parsing
      Max request size
      Check send and accept return values
      Graceful shutdown on SIGINT
    Features
      Persistence via AOF or RDB
      Pub/Sub
      Configurable port via argv
      AUTH
```

---

## Project Layout

```text
.
├── main.cpp      # entire server: store, RESP parser, client handler, main()
└── README.md
```
