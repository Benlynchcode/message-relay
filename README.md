# message-relay

A small client/server message relay over raw TCP with a custom protocol. Clients register with
a name, send messages to each other through the server, and explicitly acknowledge what they
receive. Messages for offline clients are kept, and anything delivered but not acknowledged is
delivered again after a reconnect.

Design, protocol and trade-offs are in [APPROACH.md](APPROACH.md).

## Prerequisites

- **JDK 21 or newer.** Check with `java -version`.
  The build targets Java 21, so an older JDK fails with `release version 21 not supported`.
- **Nothing else.** Maven is not needed: `./mvnw` downloads a pinned Maven (3.10.0) on first use.
- Docker is optional (only for the image).

## Build, test, run

```bash
./mvnw -B test                       # run all tests (about 2 seconds after the first build)
./mvnw -B package                    # run tests, then build the artifact
java -jar target/relay.jar           # start the server on port 9000
```

**Artifact:** `target/relay.jar`, a single runnable jar with its one runtime dependency
(Jackson) bundled. The same jar runs the server and the demo client.

## Try it: demo client

Start the server, then each client in its own terminal:

```bash
java -jar target/relay.jar                  # terminal 1: server
java -jar target/relay.jar client alice     # terminal 2
java -jar target/relay.jar client bob       # terminal 3
```

Client commands:

| Command | Does |
|---|---|
| `send <to> <text...>` | Send a message; the server answers `accepted` or `rejected (reason)` |
| `ack` | Acknowledge the last message delivered to you (it is only removed from your mailbox now) |
| `quit` | Disconnect |

To see redelivery: as bob, receive a message and `quit` **without** `ack`. Reconnect as bob:
the server reports `1 waiting` and delivers the same message again.

Client options: `--host=localhost --port=9000`. The client sends a heartbeat `PING` every 20s.
Start the server first; if nothing is listening the client prints `is the server running?`.

## Configuration

Every limit and timeout is a `--key=value` argument; unknown keys are rejected at startup.

```bash
java -jar target/relay.jar --port=9100 --mailbox-size=500 --idle-timeout=30s
```

| Option | Default | Meaning |
|---|---|---|
| `--host` | `0.0.0.0` | Interface to listen on |
| `--port` | `9000` | TCP port (`0` = any free port, used by the tests) |
| `--max-conns` | `100` | Concurrent connections; over the cap a client gets `SERVER_BUSY` and is closed |
| `--max-frame` | `65536` | Largest frame in bytes; larger is rejected before any memory is allocated |
| `--mailbox-size` | `1000` | Messages held per client (waiting + delivered-but-unacked); then `MAILBOX_FULL` |
| `--outbound-buffer` | `64` | Replies queued per connection; a client that stops reading is disconnected when it fills |
| `--dedupe-window` | `1000` | Recent message ids remembered per mailbox for duplicate detection (must be ≥ mailbox-size) |
| `--register-timeout` | `5s` | A new connection must `REGISTER` within this |
| `--idle-timeout` | `60s` | A registered client that sends nothing (not even `PING`) for this long is disconnected |
| `--write-timeout` | `5s` | A write to a client stuck longer than this closes that connection |
| `--shutdown-grace` | `5s` | How long shutdown waits for connections to finish before forcing them closed |

Durations take `ms` or `s` (`500ms`, `5s`).

## Server lifecycle

- **Start:** `java -jar target/relay.jar [options]`. Logs `listening on 0.0.0.0:9000`.
  Invalid options print the reason and exit with code 2.
- **Stop:** Ctrl-C, `kill <pid>` (SIGTERM) or `docker stop`. A shutdown hook then:
  1. stops accepting connections,
  2. sends `SHUTTING_DOWN` to every client and closes each connection after its pending writes,
  3. waits up to `--shutdown-grace`, then force-closes anything left.
- **State is in memory only:** stopping the server loses all queued and unacknowledged messages.

## Dependencies

| What | Why | Scope |
|---|---|---|
| Java 21 standard library | Sockets (`java.net`), threads (virtual threads), concurrency | — |
| Jackson `jackson-databind` 2.21.2 | JSON inside each frame | runtime, bundled in the jar |
| JUnit 5.14.4 | Tests | test only |
| Maven plugins (compiler, surefire, shade) | Build, test, bundle the jar | build only |

No messaging library or broker: registration, mailboxes, delivery, retries and acknowledgements are all in this code.

## Tests

`./mvnw -B test` runs 47 tests in three groups:

| Class | What it covers |
|---|---|
| `FrameCodecTest` | Framing: round trip, back-to-back frames, oversized / negative / truncated frames, bad JSON |
| `MailboxTest` | Mailbox logic with no sockets: order, limits, duplicates, stale acks, reconnect tickets, concurrency |
| `RelayServerTest` | A real server on a free port with real sockets: the core scenario end to end, limits, timeouts, slow client, takeover, shutdown |

Every test has a timeout, server tests use port `0` so they never collide, and nothing waits
with `sleep`: tests wait for the actual frame, with a read timeout.

## Docker (optional)

```bash
docker build -t relay .
docker run --rm -p 9000:9000 relay
```

Then run clients from the host as above. Options go after the image name, e.g.
`docker run --rm -p 9100:9100 relay --port=9100`.

`docker stop` sends SIGTERM to the JVM, so the server shuts down cleanly (same steps as Ctrl-C).

## Project layout

```
src/main/java/io/github/benlynchcode/relay/
├── Main.java            entry point: server (default) or `client <id>`
├── protocol/            wire format: FrameCodec, Message, MessageType, ErrorCode
├── mailbox/             per-client message state (no networking)
├── server/              RelayServer, ClientConnection, Registry, ServerConfig
└── client/              RelayCli, the demo client
src/test/java/...        tests for each package
```
