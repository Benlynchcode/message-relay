# AI Notes

Running log of AI suggestions, what was accepted or changed, and how it was verified.
Source material for the "AI usage" section of APPROACH.md.

## Step log

### Language switch Go → Java
- Ben switched the project to Java. Choices (Ben picked from options Claude offered):
  Java 21 (virtual threads), Maven with committed wrapper, blocking `java.net` sockets (not NIO).
- Claude proposed Jackson (JSON) and JUnit 5 as the only dependencies. **Approved by Ben** (pinned versions, shade fat jar).
- Removed `go.mod`; rewrote `AGENT_INSTRUCTIONS.md` for Java, including a Go→Java concurrency mapping table.

### Design-decision tags
- Ben asked for `// [Qn: Name]` one-line "why" comments at every spot implementing a step-0 answer, plus the map table below.
- Ben supplied mappings for Q1 (PING/PONG + register timeout), Q2 (session counter in Mailbox attach/detach), Q3/Q4 (`(from,id)` seen-set in recipient Mailbox, recorded on successful enqueue under the same lock). Claude folded these into `AGENT_INSTRUCTIONS.md` (added PING/PONG ops; dedupe moved from per-sender window to recipient mailbox; `Connection` renamed `ClientConnection`).

### Step 0: Design review
- Ben answered Q1–Q9 in writing. All nine were sound; decisions folded into `AGENT_INSTRUCTIONS.md`.
- Ben's own additions: `ID_REUSED` via body hash (Q3), ID regex and field caps (Q5), rejecting TCP keepalive as the fix (Q1), clear-flag-before-check to avoid a lost wake-up (Q9).
- Claude's review found follow-up gaps (see "Gaps found"); the open ones are to be answered before or during the step they affect.
- Follow-ups 1–6 answered by Ben the same day; all correct. Spec updated (ticket checks on delivery, attach requeues, window ≥ mailbox, close-vs-reply rule, late-accept race, doorbell events). Bad JSON in a good frame changed from close to reply.
- Status: **done**.

### Step 1: protocol
- Claude wrote skeletons only: `FrameCodec` (readFrame/writeFrame/decode/encode as two layers), `FrameTooLargeException` (an IOException, so close), `MalformedMessageException` (checked, so reply), and `FrameCodecTest` with 6 named tests that fail until Ben fills them in.
- Ben writes: `pom.xml`, `Message` record, `ErrorCode` enum, all method and test bodies.
- `pom.xml`: Ben asked Claude to write it (build config, not relay logic). Claude wrote it with "why" comments: `release` 21, pinned versions, test-scoped JUnit, Surefire, Shade with `finalName` relay, module-info filter, Multi-Release manifest entry. Versions match a known-good Java 21 build; verify on first `./mvnw -B package`.
- Ben asked Claude to generate the protocol package after Ben answered three small design questions:
  unknown JSON fields → reject as MALFORMED (strict; catches typos like "too"); `type` as a `MessageType` enum (unknown types fail at decode); keep the name `FrameCodec`.
  Claude also enabled `FAIL_ON_TRAILING_TOKENS` (junk after the JSON is MALFORMED) and made `code` an `ErrorCode` enum rather than a String.
- Verified: `./mvnw -B test` compiles; the 6 tests fail only on their `fail("TODO")` placeholders. One compile error in Claude's first version (`getOriginalMessage()` exists only on Jackson's exception type) was caught by the build and fixed.
- Toolchain gap: Homebrew's JDK 21 isn't registered with `/usr/libexec/java_home`, so `java_home -v 21` silently returns 17. Use the Homebrew path for JAVA_HOME.
- Ben asked Claude to generate `FrameCodecTest`: 10 methods / 16 cases (round trip with multi-byte UTF-8, exact wire JSON, back-to-back frames, ~2 GB header with no body, boundary at maxFrame, 0/-1/MIN_VALUE lengths, truncated body and header, oversized outgoing frame writes nothing, 5 bad-content cases → MalformedMessageException).
- The compiler rejected a test assertion `e instanceof IOException` on a MalformedMessageException: the close-vs-reply split is enforced by the type system, so the assertion was removed.
- Verified: `./mvnw -B test` → 16 run, 0 failures.
- Status: **step 1 done**.

### Step 2: mailbox
- Ben chose option B: Claude generated `mailbox/` (`Mailbox`, `Envelope`, `MessageKey`, `EnqueueResult`, `Attachment`) and `MailboxTest`, to be walked through line by line.
- Design as decided in step 0: pending `ArrayDeque` + in-flight `LinkedHashMap` keyed by `(from, id)`; max in-flight 1; ticket on attach, checked by `nextForDelivery`/`detach`; `ack` not ticket-checked; bounded `(from,id)` → SHA-256(body) seen-map, recorded only on successful store; doorbell `Runnable` rung on enqueue/attach/ack.
- Choices Claude made, to be confirmed by Ben: `ReentrantLock` not `synchronized` (virtual-thread pinning on Java 21); SHA-256 of the body rather than storing bodies (memory) or `hashCode()` (collisions would silently drop a different message as DUPLICATE); doorbell rung **after** unlocking (never call out while holding a lock); `attach` returns `Attachment(ticket, pending)` so the REGISTERED count matches the requeue exactly.
- Verified: `./mvnw -B test` → 34 run, 0 failures (18 mailbox). `MailboxTest` repeated 20× with no failure (the 16-thread duplicate-retry test is the one that could flake). Mailbox package imports no `java.net`.
- Ben answered drill 1 correctly (dup check first so a stored retry isn't reported as failed; proven by `duplicateIsReportedEvenWhenTheMailboxIsFull`) and asked to stop line-level drills in favour of architecture-level review: draw it / point to where it lives / defend the decisions.
- Status: **step 2 done**.

### Steps 3-6 + client: server, connections, Main, CLI, integration tests
- Ben asked Claude to finish the code before returning to the diagram; Claude generated everything in one pass.
- Files: `server/` (`ServerConfig`, `Ids`, `Registry`, `ClientConnection`, `RelayServer`), `Main`, `client/RelayCli`, `server/TestClient`, `server/RelayServerTest`. Also `Message` factory methods and a new `BODY_TOO_LARGE` code.
- Decisions Claude made while building (for Ben to own):
  - `Registry` uses one lock for register/unregister so "who is online" and "who holds the ticket" change together; mailbox lookups stay lock-free (`ConcurrentHashMap`).
  - Doorbell is ignored until `REGISTERED` is queued, then rung once, so DELIVER can't overtake REGISTERED.
  - Register timeout is a deadline (not per-read), so junk frames can't keep an unregistered socket alive.
  - `BODY_TOO_LARGE`: a SEND that fits a frame can produce a DELIVER that doesn't (it adds `from`); rejected at send time instead of failing forever at delivery.
  - PING requires registration (consistent "nothing before REGISTER" rule).
  - The replaced connection is closed gracefully with no error frame.
  - Write watchdog runs every write-timeout/2 on a daemon platform thread.
- Bugs caught by verification: `--host` was read with `getOrDefault` instead of removed, so it was reported as an unknown option (all 13 integration tests failed; fixed). An unused field and test helper removed.
- Verified:
  - `./mvnw -B verify` → 47 tests, 0 failures; builds `target/relay.jar`.
  - `RelayServerTest` run 30× in a row, 0 failures (includes the slow-client flood and shutdown timing tests).
  - Manual end-to-end on the real jar with the CLI: offline queueing ("2 waiting"), ACK, quit without ACK → redelivered ("1 waiting"); SIGTERM → clean exit in ~0.4s.
- Status: **code complete for the core**. Remaining: README, APPROACH.md (with the diagram), CI workflow, Dockerfile.

### APPROACH.md: architecture and protocol diagrams
- Claude wrote the first sections of APPROACH.md: architecture (Mermaid component diagram, temporary-vs-permanent table, "where each part lives" table) and protocol (frame layout, operations table, Mermaid sequence diagram including a disconnect and redelivery).
- Not verified: rendering the Mermaid locally needed a downloaded renderer, which was blocked. Check the diagrams render on GitHub (or a VS Code Mermaid preview) before relying on them.
- Remaining APPROACH.md sections: acceptance criteria, state and concurrency models, delivery semantics, trade-offs, limitations, next steps, AI usage.

### Client polish and submission docs
- Ben asked what happens if the client starts before the server: it printed a raw `ConnectException` stack trace. Claude added a catch with a one-line "is the server running?" message; verified by running the jar against an unused port (exit code 1).
- Diagrams: Ben asked for much simpler diagrams; settled on a 3-lifeline Mermaid sequence diagram (Alice, Server, Bob) with the mailbox shown as notes.
- Claude wrote README.md (prerequisites, commands, artifact, configuration, lifecycle, dependencies, tests, Docker) and the remaining APPROACH.md sections (acceptance criteria, connection lifecycle, errors, state, concurrency, delivery semantics, Q1–Q9 decisions, testing, trade-offs, limitations, next steps, AI usage). Ben to read and adjust anything not in his own words.
- Dockerfile (bonus 2): multi-stage, Maven + Temurin 21 to build, Temurin 21 JRE to run, non-root user, exec-form entrypoint. **Not built**: Docker isn't installed on Ben's machine.
- Repo hygiene: before the final push, the working notes were trimmed to project content only, the notes files were removed from the earlier unpushed commits, and the GitHub repo is to be recreated empty before pushing.

## Design-question map
Where each step-0 answer lives in the code. Every spot has a `// [Qn: Name]` comment; `grep -rn "\[Q" src/` lists them all.
All nine are implemented and tagged.

| Q | Name | Where (files · methods) | Status |
|---|---|---|---|
| Q1 | Idle timeout | `ClientConnection`: constructor (register deadline), `applyReadTimeout()`, `handleRegister()` (idle timeout), `handlePing()` · `RelayCli.HEARTBEAT_SECONDS` | done |
| Q2 | Re-register race | `Mailbox.attach()` / `nextForDelivery(ticket)` / `detach(ticket)` · `Registry` (lock, `register()` new connection wins) · `ClientConnection.runReader()` finally, `handleRegister()`, `deliverNext()` | done |
| Q3 | Dedupe vs reject | `Mailbox.enqueue()` (dup → room → store + record, one lock; body hash → `ID_REUSED`) · `Mailbox` constructor + `ServerConfig` (window ≥ size) | done |
| Q4 | Dedupe state location | `Mailbox.seen`: bounded `(from,id)` → SHA-256 map, `removeEldestEntry` eviction | done |
| Q5 | Edge inputs | `FrameCodec.decode()` + `FrameTooLargeException` · `Ids` (ID rules) · `ClientConnection.runReader()` (close vs reply) and `handleSend()` (empty vs missing body) | done |
| Q6 | Shutdown | `RelayServer.shutdown()` (steps 1-4) · `RelayServer.handleNewSocket()` (add-then-check) · `ClientConnection.shutdownNotice()` · `Main` shutdown hook | done |
| Q7 | Slow client | `ClientConnection` constructor (bounded queue) · `ClientConnection.reply()` (`offer`, close on full) | done |
| Q8 | Stuck writer | `ClientConnection.writeStartedNanos` / `write()` · `RelayServer.closeStuckWriters()` · `ClientConnection.closeNow()` (idempotent, socket close frees the write) | done |
| Q9 | Waiting on two things | `ClientConnection`: `runWriter()` (one queue), `ring()` (coalesced), `deliverNext()` (clear before check) · `Mailbox` rings in `enqueue`/`attach`/`ack` | done |

## Gaps found
| Area | Question | Resolution |
|---|---|---|
| Q2 | If `detach(staleTicket)` is ignored, who requeues the in-flight message the old connection was holding? | Resolved (Ben): `attach()` requeues all in-flight to the front of pending under the lock, then bumps the ticket. |
| Q2 | Can the old connection's writer still pull a message after the new connection attaches? | Resolved (Ben): yes, via a leftover marker. `nextForDelivery(ticket)` checks the ticket too; the mailbox stores the current owner's doorbell. Claude added: `ack` needn't be ticket-checked (late ACK is harmless). |
| Q3/Q4 | Seen-window smaller than the mailbox: what breaks? | Resolved (Ben): queued IDs get forgotten, so retries are stored twice. Enforce `dedupeWindow >= mailboxSize` at startup. |
| Q1 | `setSoTimeout` is per `read()` call, not per frame. What does a client sending 1 byte every 59s do? | Resolved (Claude): slowloris. Keeps a slot forever with a half-built frame. Mitigation: per-frame deadline or accept it at our scale and document. |
| Q1 | What is `PONG` actually for? | Resolved (Claude): client-side liveness. Lets the *client* detect a dead server or half-open socket and reconnect. |
| Q1 | Switching from register timeout to idle timeout | Resolved (Claude): call `setSoTimeout` again after `REGISTERED`; it applies to subsequent reads. |
| Q5 | Which errors close the connection and which don't? | Resolved (Ben): untrusted stream or misbehaving client → close; valid frame, invalid request → reply and stay open. Bad JSON in a good frame stays open. |
| Q6 | A socket accepted just before `ServerSocket.close()` isn't in the connection set yet. Does it get `SHUTTING_DOWN`? | Resolved (Ben): add-to-set then check flag; shutdown sets flag then walks set. One side always catches it. |
| Q6 | Step 2 then step 3 back-to-back: does `SHUTTING_DOWN` actually reach the client? | Resolved (Claude): not guaranteed. Enqueue it, let the writer flush, close after a short bound; best-effort only. |
| Q6 | What happens to unACKed messages on shutdown? (question part not answered) | Resolved: lost, in-memory only; document in APPROACH.md; persistence is a next step. |
| Q7 | Who actually fills Bob's outbound queue? | Resolved (Claude): markers are coalesced, so only Bob's own replies (SEND_RESULT/PONG) fill it, i.e. Bob sending lots without reading. |
| Q7 | A dropped `SEND_RESULT` was stored. What does the sender see? | Resolved (Claude): it retries, gets `DUPLICATE` (accepted). This is why dedupe exists; Q3 and Q7 are linked. |
| Q9 | Who rings the doorbell? | Resolved (Ben): enqueue, attach, ack. |
| Client | Who prints "is the server running?" when no server is up? | Resolved: the client itself. Nothing is listening, so the OS answers the connect with a TCP reset (RST); Java throws `ConnectException`; `RelayCli.main` catches it. The server never sees it. |
| Client | How many threads does the CLI run? | Resolved (Ben, correcting Claude's "two"): three. Main (keyboard), virtual reader (`printIncoming`), heartbeat timer. |
| Q9 | Marker `offer()` fails on a full queue after the flag was set true. | Resolved (Claude): flag stuck true means no further wake-ups, but the connection is closed on full anyway. Make sure that's the code path. |
