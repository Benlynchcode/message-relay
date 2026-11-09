# Agent Instructions: Java TCP Message Relay

Working instructions given to the AI assistant (Claude Code) while building this project.
They record how the work was split, and the spec and design decisions the code follows.

## How we worked
- The developer owns every design decision. The assistant explains options, proposes designs and code, and reviews; it asks before making any decision not covered below.
- The goals carry equal weight: **a correct, readable submission**, and **the developer understanding all of it**. Knowledge gaps found along the way are logged in `AI_NOTES.md`.
- The assistant started by giving skeletons to fill in; the developer wrote part of the protocol layer that way, then asked the assistant to generate the rest of the code, followed by a walkthrough and review questions.
- Prefer **simple, readable, idiomatic Java** over clever code. Every line must be defensible out loud.
- Keep `AI_NOTES.md` up to date (step log, "Gaps found"). It is the source for the AI-usage section of APPROACH.md, which must say honestly how AI was used and how the work was verified.
- Time box ~2 hours as a guide. Finish the core first; record shortcuts and incomplete work honestly.
- Before anything is pushed: no secrets, no proprietary code, and the README commands verified from a clean clone.

## The task (summary)
Build a small client/server messaging relay over **raw TCP** with a **custom protocol**. Two or more clients connect to one server; clients never talk directly. The server handles registration, mailboxes, delivery, ACKs and redelivery.

Required behaviour:
1. A client registers with a unique ID. Many clients can be registered at once.
2. A client sends a uniquely identified message to another client.
3. The server confirms accept/reject of every send.
4. The recipient receives the message and explicitly ACKs it.
5. If the recipient is offline, messages are retained (within bounded limits).
6. The recipient reconnects with the same ID and receives its queued messages.
7. A message delivered but not ACKed before disconnect is redelivered after reconnect.

Must also:
- Remove a message only after the **correct recipient** ACKs it (at-least-once delivery).
- Bound everything: mailboxes, frame size, active connections, buffers. Report limits with clear error codes.
- Define behaviour for ordering, duplicate sends, and repeated or stale ACKs.
- Ensure a slow, malformed, or disconnected client never blocks others.
- Keep state correct under concurrency, and shut down predictably.

## Hard constraints
- **Java 21** (for virtual threads), built with **Maven** via the committed wrapper (`./mvnw`).
- Networking: **blocking `java.net`** (`ServerSocket`, `Socket`). No Netty, no NIO selector, no web framework.
- Dependencies (approved by Ben, versions pinned in `pom.xml`): **Jackson** (`jackson-databind`) for JSON, **JUnit 5** for tests, `maven-shade-plugin` for the fat jar. Nothing else without approval. The exercise allows serialization and testing libraries.
- **No messaging libraries, brokers or queues** (Kafka, RabbitMQ, NATS, ZeroMQ, Redis, etc.). All relay logic is ours. `java.util.concurrent` collections are fine; they are data structures, not brokers.
- Out of scope: auth, TLS, UI, multiple replicas, durable storage. In-memory state is fine (document it).

## Architecture
```
Client A ──TCP──► ClientConnection A (reader + writer virtual threads) ──► Registry ──► Mailbox(alice)
Client B ──TCP──► ClientConnection B (reader + writer virtual threads) ──►          ──► Mailbox(bob)
```
- **Connections are temporary; mailboxes are durable** (live for the server's lifetime). Never tie message state to a socket.
- The `mailbox` package must **not import `java.net` or `java.io` sockets**. It's pure logic and unit-testable.

## Repo layout
```
message-relay/
├── README.md, APPROACH.md, AI_NOTES.md
├── pom.xml
├── mvnw, mvnw.cmd, .mvn/wrapper/       # Maven wrapper: a fresh clone builds without Maven installed
├── Dockerfile                          # bonus: multi-stage, eclipse-temurin:21
├── .github/workflows/ci.yml            # ./mvnw -B verify
└── src/
    ├── main/java/io/github/benlynchcode/relay/
    │   ├── Main.java                   # arg parsing, shutdown hook, start server
    │   ├── protocol/  FrameCodec.java, Message.java, ErrorCode.java
    │   ├── mailbox/   Mailbox.java, EnqueueResult.java
    │   └── server/    RelayServer.java, ClientConnection.java, Registry.java, ServerConfig.java
    └── test/java/io/github/benlynchcode/relay/
        ├── protocol/FrameCodecTest.java
        ├── mailbox/MailboxTest.java
        └── server/RelayServerTest.java, TestClient.java
```

## Protocol spec

### Framing
- Every frame is a **4-byte big-endian length** followed by that many bytes of **UTF-8 JSON**.
- Writer: `DataOutputStream.writeInt(len)` then the body, over a `BufferedOutputStream`; `flush()` per frame.
- Reader: `DataInputStream.readInt()`, then **check `0 < n <= maxFrame` before allocating**, then `readFully` the body.
  `readInt` is **signed**: a header of `0xFFFFFFFF` reads as `-1`, so the negative check matters.
- A clean EOF before a header means the peer closed (`EOFException`); EOF mid-frame means truncated.
- On an oversized frame or bad length: send `ERROR` if possible, then **close the connection** (the stream is no longer trustworthy).
- Bad JSON inside a correctly framed message is different: the stream is still in sync, so reply `ERROR MALFORMED` and **keep the connection open** (see the close-vs-reply rule).

### Message type
One record, with nulls omitted on the wire (`@JsonInclude(NON_NULL)`):
```java
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Message(
    String type,
    String clientId,   // "client_id" on the wire (@JsonProperty)
    String id,         // unique per sender
    String to,
    String from,
    String body,
    Boolean accepted,  // boxed so it can be absent, and false still serialises
    String code,
    Integer pending) {}
```
Unknown JSON fields: reject as `MALFORMED` (fail fast) or ignore (forward compatible). Ben decides in step 1.

### Operations
| Direction | Type | Fields | Meaning |
|---|---|---|---|
| C→S | `REGISTER` | client_id | Register or reattach identity |
| S→C | `REGISTERED` | client_id, pending | Success; number of queued messages |
| C→S | `SEND` | id, to, body | Send message to a recipient |
| S→C | `SEND_RESULT` | id, accepted, code? | Accept/reject for that send |
| S→C | `DELIVER` | id, from, body | Message for the recipient |
| C→S | `ACK` | id, from | Recipient confirms receipt |
| C→S | `PING` | | Heartbeat; resets the server's idle read timer |
| S→C | `PONG` | | Heartbeat reply |
| S→C | `ERROR` | code | Non-send-specific error |

### Error codes (`enum ErrorCode`)
`MALFORMED`, `FRAME_TOO_LARGE`, `NOT_REGISTERED`, `ALREADY_REGISTERED` (REGISTER twice on one connection), `INVALID_ID`, `UNKNOWN_RECIPIENT`, `MAILBOX_FULL`, `BODY_TOO_LARGE` (the DELIVER built from this SEND would exceed `max-frame`, because it adds `from`), `DUPLICATE`, `ID_REUSED` (same `(from,id)` with a different body), `ACK_UNKNOWN`, `SERVER_BUSY`, `SHUTTING_DOWN`.

## Semantics (decided; document in APPROACH.md)
- **Delivery:** at-least-once. A message key is `(from, id)`. It's deleted only on ACK from the mailbox owner.
- **Ordering:** FIFO per recipient. Default **max in-flight = 1** per recipient, so strict FIFO including across reconnects. On disconnect, in-flight messages go back to the **front** of pending.
- **Recipients:** a mailbox is created on a client's first REGISTER. SEND to a never-registered ID returns `UNKNOWN_RECIPIENT`. SEND to a registered-but-offline ID is queued.
- **Duplicate sends:** a bounded `(from, id)` seen-set inside the **recipient's `Mailbox`**, recorded **only on successful enqueue, under the same mailbox lock** (so a `MAILBOX_FULL` rejection can be retried). Order inside the lock: duplicate check, room check, store + record. The entry stores a hash of the body: same body is a retry (`accepted:true, code:"DUPLICATE"`, **not** enqueued again); a different body is rejected with `accepted:false, code:"ID_REUSED"`. Reusing an ID across *different* recipients isn't detected (client bug; document it).
  - **`dedupeWindow >= mailboxSize` is required and validated at startup.** A smaller window would forget IDs of messages still queued, and a retry would be stored and delivered twice. IDs older than the window aren't detected; document this.
- **Stale or repeated ACK:** if `(from,id)` isn't in the owner's in-flight set, reply `ACK_UNKNOWN`. No state change.
- **Re-register while the old connection is alive:** the **new connection wins** (`Registry.register()`); the old one is closed.
  - `Mailbox.attach(doorbell)` runs under the mailbox lock: it **requeues all in-flight messages to the front of pending**, bumps the **session ticket**, stores the new owner's doorbell, rings it, and returns the ticket. In-flight state belongs to the mailbox, not to a connection.
  - **Every owner operation must prove it's the current owner:** `nextForDelivery(ticket)` returns nothing and `detach(ticket)` is a no-op for a stale ticket. Otherwise the old writer could pull a message into in-flight and write it to a dead socket, using up the in-flight slot.
  - Rule: *anything that delivers or cleans up must present the current ticket.*
  - `ack` is not ticket-checked: a late ACK from the old socket for a message Bob really received is harmless, and an ACK for a requeued message gets `ACK_UNKNOWN` and is redelivered (at-least-once).
- **Actions before REGISTER:** `NOT_REGISTERED` error.
- **Mailbox full:** the sender gets `accepted:false, code:"MAILBOX_FULL"`. Never drop silently.
- **Input validation** (step 0, Q5): `client_id` and `to` must match `^[A-Za-z0-9_-]{1,64}$`, else `INVALID_ID`. Message `id` is capped at 128 chars. Self-send is allowed. Empty body `""` is allowed; a missing (`null`) body is `MALFORMED`.
- **Shutdown order** (Q6): set `shuttingDown` → close `ServerSocket` → send `SHUTTING_DOWN` (best-effort: queue it, let the writer flush briefly) → close client sockets → await with grace timeout, then `shutdownNow()`. Queued and unACKed messages are lost (in-memory; document it).
  - **Late-accept race:** a new connection *adds itself to the connection set, then checks `shuttingDown`*; shutdown *sets the flag, then walks the set*. Either the walk sees the connection or the connection sees the flag. Concurrent set + `AtomicBoolean`; `close()` is idempotent because both paths may close.
- **Close vs reply** (Q5): if the byte stream can't be trusted or the client is misbehaving, **close**; if the frame is fine but the request is invalid, **reply and stay open**.
  - Close: `FRAME_TOO_LARGE`, bad length/truncated frame, register timeout, idle timeout, slow client (outbound queue full), `SERVER_BUSY`, `SHUTTING_DOWN`, replaced by a newer connection.
  - Reply and stay open: `MALFORMED` (bad JSON in a good frame), `INVALID_ID`, `NOT_REGISTERED`, `ALREADY_REGISTERED`, `UNKNOWN_RECIPIENT`, `MAILBOX_FULL`, `BODY_TOO_LARGE`, `DUPLICATE`, `ID_REUSED`, `ACK_UNKNOWN`.
- **REGISTERED before DELIVER:** a connection's doorbell is ignored until `REGISTERED` has been queued; it then rings itself once. One check finds everything, so no wake-up is lost.
- **Idle clients:** clients send `PING` about every third of the idle timeout (~20s); any received frame resets the idle read timeout. TCP keepalive is not relied on (OS-level probes don't reach `read()`, slow defaults). A separate, shorter register timeout applies before `REGISTER`.

## Design-decision tags (required)
Every place in the code that implements an answer to one of the nine step-0 design questions gets a one-line tagged comment:
```java
// [Q1: Idle timeout] Heartbeat PING resets the read timer so quiet-but-alive clients aren't dropped.
```
- `Q1`–`Q9` match the question list; include the short name. Explain **why**, not what. One line.
- If an answer spans several places, tag each place. Don't tag unrelated code.
- Skeletons Claude provides include the tags; reviews check that Ben's code keeps them.
- `AI_NOTES.md` keeps a table mapping each question to its files and methods. Revise with `grep -rn "\[Q" src/`.

| Tag | Short name |
|---|---|
| Q1 | Idle timeout |
| Q2 | Re-register race |
| Q3 | Dedupe vs reject |
| Q4 | Dedupe state location |
| Q5 | Edge inputs |
| Q6 | Shutdown |
| Q7 | Slow client |
| Q8 | Stuck writer |
| Q9 | Waiting on two things |

## Concurrency model (Java mapping of the Go design)
| Go idea | Java equivalent |
|---|---|
| goroutine per connection | **virtual thread** per reader/writer (`Executors.newVirtualThreadPerTaskExecutor()` or `Thread.ofVirtual()`) |
| buffered-channel semaphore | `java.util.concurrent.Semaphore(maxConns)` with `tryAcquire()`; over the cap, send `SERVER_BUSY` and close |
| `chan Message` (bounded) | `ArrayBlockingQueue<...>(outboundBuffer)`; producers use non-blocking `offer()` |
| `select` on two channels | **No `select` in Java.** The writer takes from **one** queue. The mailbox "wake up and pull" signal is a special item in that queue, coalesced with an `AtomicBoolean` so there's at most one pending wake-up (the equivalent of a cap-1 notify channel) |
| `SetReadDeadline` | `socket.setSoTimeout(ms)`: register timeout first, then idle timeout. Throws `SocketTimeoutException` |
| `SetWriteDeadline` | **No write timeout on blocking sockets.** The writer thread is the only one that can block on a write; a watchdog (or the bounded queue overflow) closes the socket to unblock it. Discuss in step 4 |
| `sync.Mutex` | `ReentrantLock` or `synchronized`, one per mailbox; `ConcurrentHashMap` for the registry (`computeIfAbsent` is atomic). **Never hold a lock during socket I/O** |
| `defer` cleanup | `try/finally` (and try-with-resources for the socket) |
| `signal.NotifyContext` | `Runtime.getRuntime().addShutdownHook(...)` (runs on SIGINT/SIGTERM) |
| `WaitGroup` + grace timeout | `executor.shutdown()` + `awaitTermination(grace)`, then `shutdownNow()` |

- **Slow client:** non-blocking `offer()` to the outbound queue, never `put()`; if it's full, close the connection. Messages stay safe in the mailbox.
- **Stuck writer:** a write watchdog closes any connection whose current write has run longer than `--write-timeout`. `close()` is idempotent (guarded by an `AtomicBoolean`).
- **Writer wake-up:** one `BlockingQueue<Work>` holding `Reply(msg)` and a `CheckMailbox` marker; an `AtomicBoolean` coalesces markers and is cleared **before** the writer checks the mailbox (avoids a lost wake-up). The mailbox holds the current owner's doorbell and rings it on: **enqueue**, **attach** (drain offline/requeued messages), and **ack** (frees the in-flight slot).
- **Shutdown:** stop accepting (close `ServerSocket`), notify and close all connections, await the executor with a grace timeout, exit.

## Config (CLI args `--key=value`, defaults)
No flag library: a small hand-written parser in `Main`, producing a `ServerConfig` record.
| Arg | Default |
|---|---|
| `--port` | 9000 (0 = ephemeral, used by tests) |
| `--max-conns` | 100 |
| `--max-frame` | 65536 bytes |
| `--mailbox-size` | 1000 messages |
| `--outbound-buffer` | 64 |
| `--register-timeout` | 5s |
| `--idle-timeout` | 60s |
| `--write-timeout` | 5s |
| `--shutdown-grace` | 5s |
| `--dedupe-window` | 1000 IDs per sender |

The server must also be startable **in-process** from tests: `new RelayServer(config)`, `start()` binding `127.0.0.1:0`, `port()`, `close()`/`shutdown(Duration)`.

## Tests (small, focused, deterministic)
Run all of them with: `./mvnw -B test`
- JUnit 5. Every test class has a `@Timeout`, so nothing can hang the build.
- Java has **no race detector** (Go's `-race`). Concurrency confidence comes from: a design that keeps shared state behind one lock, targeted concurrent tests (many threads plus a `CountDownLatch` start gate), and `@RepeatedTest` to shake out flakiness. Know that `jcstress` exists.
- **protocol:** frame round-trip; oversized and negative length rejected before allocation; truncated frame errors.
- **mailbox (parameterised/table-style):** enqueue/next/ack; ack by wrong owner ignored; detach requeues in-flight at front; mailbox full; duplicate ID; stale ACK.
- **server integration (real TCP, `TestClient` helper with `setSoTimeout` on every read):**
  1. Happy path: register both, send, SEND_RESULT, DELIVER, ACK.
  2. Offline delivery: send to offline bob, bob reconnects and receives it.
  3. Redelivery: bob gets DELIVER, disconnects without ACK, reconnects, receives the same message again.
  4. Mailbox full returns `MAILBOX_FULL`.
  5. Malformed or oversized frame: error and close; other clients unaffected.
  6. Slow client (never reads) doesn't block delivery between other clients.
  7. FIFO preserved across a reconnect.
  8. Graceful shutdown completes within the grace period.
- No `Thread.sleep` for synchronisation. Wait on actual frames with read timeouts.

## Build order
1. `protocol`: framing, `Message`, `ErrorCode`, tests.
2. `mailbox`: logic plus table tests.
3. `server`: registry, accept loop, connection cap.
4. `ClientConnection` reader/writer, timeouts, cleanup, shutdown.
5. Integration tests plus `TestClient`.
6. `Main` plus arg parsing and shutdown hook.
7. README.md, APPROACH.md (from AI_NOTES.md).
8. Bonus if time allows: Dockerfile, CI workflow, small CLI client. **Skip persistence**; document it as a next step (append-only log, replay on startup).

## Commands (README must list exactly these, verified)
```
./mvnw -B package                 # compiles, runs tests, builds target/relay.jar
./mvnw -B test                    # tests only
java -jar target/relay.jar --port=9000          # server (default mode)
java -jar target/relay.jar client alice         # demo CLI client: send <to> <text> | ack | quit
docker build -t relay . && docker run -p 9000:9000 relay
```
Artifact: `target/relay.jar`, a runnable **fat jar** (Jackson bundled via `maven-shade-plugin`), and optionally the `relay` Docker image.

## Definition of done
- `./mvnw -B verify` passes from a clean clone, and the integration tests pass under `@RepeatedTest`.
- README commands verified from a clean clone.
- APPROACH.md covers: acceptance criteria, architecture, protocol and lifecycle (reconnect, ACKs, connection close), state and concurrency models, delivery semantics, trade-offs, known limitations (in-memory, no auth, dedupe window, single node), incomplete work, next steps, AI usage.
