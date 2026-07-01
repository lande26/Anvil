# Anvil — Building a Zero-Dependency Job Queue by Embedding a Database into the Queue Itself

> **This is Part 3 of a series where we build a distributed system from scratch.**
> - **Part 1** — [Valkyr: Building a Redis Clone in Go](#) — we built the storage engine.
> - **Part 2** — [ForgeQueue: A Fault-Tolerant Job Queue in Go](#) — we built the job queue on top of it.
> - **Part 3** — You're reading it. We're combining them.

---

## Where we left off

After finishing Valkyr and ForgeQueue, the system looked like this:

```
  [API Server]  →  [Redis / Valkyr]  ←→  [Worker Pool]
                                    ←→  [Reaper]
```

Three processes. One of them is a database that the other two talk to over a network socket.

This works. ForgeQueue works in production. But if you stare at this long enough, one question keeps coming up:

**Why is the database a separate process at all?**

---

## What "separate process" actually costs you

To understand why this is an interesting question, you need to understand what happens when two programs talk to each other over a socket. This concept has a formal name — **RPC**, short for **Remote Procedure Call**.

The idea sounds fancy but it's simple. When your worker wants to dequeue a job, it doesn't just call a Go function. It:

1. Serializes the command (`LMOVE pending processing RIGHT LEFT`) into bytes.
2. Sends those bytes across a network socket to the Redis process.
3. **Waits.**
4. Redis reads the bytes, parses the command, executes it, serializes the response.
5. Sends the response back across the socket.
6. Your worker reads the response bytes and continues.

That round trip — send, wait, receive — is called a **network hop**. Even on localhost, this takes around **50–100 microseconds** per call. Doesn't sound like much. But ForgeQueue's dequeue path makes several of them in sequence: acquire lock, load metadata, update heartbeat. Every job has an RPC tax of several hundred microseconds before the worker even starts running the job.

More importantly, because the two processes are separate and communicate over a network, you can never have true atomicity for free. You have to fake it — that's what all those Lua scripts in ForgeQueue are doing. You're asking Redis to run a tiny program inside its own process because you can't hold a mutex across a network socket.

---

## The embedded alternative

What if instead of a separate process, the database lived inside the same process as the queue?

```
  [API Server]  →  [Anvil: Queue + Storage Engine in one binary]
```

No network socket. No serialization. No RPC. When a worker calls `queue.Dequeue()`, it's calling a Go function that directly manipulates a Go struct that lives in the same heap. The "database" is just memory. Atomicity is just a mutex.

This is the same design decision SQLite made in 1999 — and it's why SQLite is embedded in billions of devices, from browsers to phones to aircraft systems. You give up multi-process access and get simplicity, performance, and a single deployable artifact in return.

Anvil is that idea applied to job queues. One binary. One process. No external dependencies.

---

## What Anvil is built from

Anvil is composed of two things you've already seen built:

### 1. The storage layer — stripped Valkyr

We take Valkyr's in-memory store and extract exactly the three data types Anvil needs:
- **Strings** — for idempotency keys (deduplication).
- **Hashes** — for job metadata (ID, type, status, retry count, heartbeat timestamp).
- **Lists** — for the three queues (`pending`, `processing`, `dead`).

Everything else in Valkyr — pub/sub, sorted sets, TTL expiry, RDB snapshots, the TCP server, the entire RESP protocol parser — is dropped. We're not building a general-purpose database. We're building the smallest possible storage layer that the queue needs and no more.

### 2. The queue layer — ForgeQueue without the wire

We take ForgeQueue's orchestration logic — idempotent enqueue, atomic dequeue, complete/fail with retry logic, the heartbeat-driven Reaper — but instead of Redis clients and Lua scripts, all of it is just Go function calls on a Go struct.

The Lua script that ForgeQueue used to atomically dequeue a job becomes:

```go
mu.Lock()
job := store.Lists.LMove("pending", "processing")
store.Hashes.HSet(job.ID, "status", "processing")
aofCh <- aofEntry{verb: "DEQUEUE", jobID: job.ID}
mu.Unlock()
```

The mutex does what the Lua script was doing. The channel sends the write to disk asynchronously so we don't hold the lock while waiting on I/O.

---

## The AOF — persistence without a database server

Without a separate process running, we need a way to survive crashes. Anvil's answer is the same one Valkyr used: the **Append-Only File (AOF)**. Every state change is logged to disk. On restart, we replay the log to reconstruct exactly where we were.

The key difference is that Anvil's AOF is purpose-built for the queue. ForgeQueue had to translate queue concepts into raw Redis commands to get them logged. Anvil's AOF speaks queue natively:

```
ENQUEUE  abc-123  {"type":"email","to":"user@example.com"}
DEQUEUE  abc-123
COMPLETE abc-123
```

Six verbs total. This means the log is compact, fast to replay at startup, and completely decoupled from how the data is stored internally — if we ever change the internals (swap a list for a heap), existing AOF files still replay correctly.

The AOF also needs **compaction**. A completed job leaves three log entries behind that are semantically useless. Periodically, Anvil rewrites the log to contain only the commands needed to reconstruct the current live state. This is done via a two-step handshake: the background write loop flushes and pauses, a compacted log is written to a temp file and atomically renamed over the old file, and then the write loop resumes on the new file. No entries are ever lost or blocked during the swap.

Additionally, Anvil supports strict durability guarantees. Under the `SyncAlways` policy, when a worker logs an entry, it blocks on a `done` channel until the background write loop explicitly confirms that the fsync has completed on disk.

---

## The tradeoffs

Every architecture decision is a tradeoff. Here's an honest accounting.

### What you gain

**Performance.** No network hops, no serialization overhead. A `Dequeue` call is a mutex acquisition and two in-memory map lookups. Roughly 10x latency reduction per operation compared to ForgeQueue.

**Simplicity.** One binary to deploy. No Redis to provision, patch, back up, or monitor separately.

**True atomicity.** Mutexes are not a workaround — they're the primitive. No Lua scripts, no edge cases where the script path and the Go path disagree on state.

### What you give up

**Multi-process access.** Because the state lives in one process's heap, only that process can read or write it. You cannot run two Anvil instances sharing a job queue. This is the exact tradeoff SQLite makes.

**Horizontal scaling of the queue itself.** In ForgeQueue, you could run workers on multiple machines, all coordinating through Redis. In Anvil, all workers are goroutines inside the same process. The upper bound is one machine's concurrency.

**Operational familiarity.** Redis is known, documented, and has tooling. Anvil's embedded store has none of that — you inspect it by hitting the API or reading the AOF.

---

## How the pieces connect at runtime

```
                        ┌──────────────────────────────────────────┐
  curl POST /jobs  ───► │  HTTP API                                │
                        │  anvil.Enqueue(job)                      │
                        │       │                                  │
                        │       ▼                                  │
                        │  Queue.Enqueue()                         │
                        │   ├─ store.Strings.SetNX(idemKey)        │
                        │   ├─ store.Hashes.HSet(jobID, ...)       │
                        │   ├─ store.Lists.RPush("pending")        │
                        │   └─ aofCh ← ENQUEUE entry               │
                        │       │                                  │
                        │       ▼                                  │
                        │  Workers (N goroutines)                  │
                        │   ├─ queue.Dequeue() ← blocks here       │
                        │   ├─ handler(ctx, payload)               │
                        │   └─ queue.Complete() or .Fail()         │
                        │                                          │
                        │  Reaper (background goroutine)           │
                        │   └─ sweeps processing list,             │
                        │      checks HeartbeatAt,                 │
                        │      calls queue.Requeue()               │
                        │                                          │
                        │  AOF writeLoop (single goroutine)        │
                        │   └─ drains aofCh → disk → fsync         │
                        └──────────────────────────────────────────┘
                                        │
                                   anvil.aof
                                  (on-disk log)
```

Everything in the diagram is one binary. The AOF is the only artifact that survives a restart. On startup, Anvil replays it and reconstructs the entire in-memory state before accepting its first request.

---

## The SDK decision

Anvil ships as an **importable Go library**, not just a server binary. You can run it as a standalone server, or embed it directly into your application:

```go
import "github.com/lande26/anvil"

func main() {
    a, _ := anvil.New(anvil.Config{
        DataDir:     "./data",
        Concurrency: 10,
        HTTPAddr:    ":8080",
    })

    a.RegisterHandler("send-email", func(ctx context.Context, payload json.RawMessage) error {
        // your logic here
    })

    a.Run(context.Background())
}
```

Your application is the queue. The queue is your application. No separate deployment, no version skew, no HTTP calls between your app and the queue. This is the pattern `asynq` uses and why it's popular for Go services that don't need horizontal queue scaling.

---

## Where this series lands

| | Valkyr | ForgeQueue | Anvil |
|---|---|---|---|
| **What it is** | Redis clone | Job queue on Redis | Job queue + embedded store |
| **State stored in** | Process memory | External Redis | Process memory |
| **Atomicity via** | Internal mutex | Lua scripts | Internal mutex |
| **Persistence** | AOF (raw RESP) | Redis AOF | AOF (queue verbs) |
| **Horizontal scale** | Yes (TCP server) | Yes (shared Redis) | No (single process) |
| **Deploy** | One binary | 3 containers + Redis | One binary |
| **Network hops per job** | — | 4–6 per lifecycle | 0 |

We built the database. We built the queue that used it. Now we're building the thing where the database and the queue are the same thing — and seeing exactly what that costs and what it buys.

---

*Source code:*
- *[Valkyr](https://github.com/lande26/valkyr) — the Redis-compatible storage engine*
- *[ForgeQueue](https://github.com/lande26/ForgeQueue) — the fault-tolerant distributed job queue*
- *[Anvil](https://github.com/lande26/Anvil) — the embedded system*
