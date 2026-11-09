package io.github.benlynchcode.relay.server;

import io.github.benlynchcode.relay.mailbox.EnqueueResult;
import io.github.benlynchcode.relay.mailbox.Envelope;
import io.github.benlynchcode.relay.mailbox.Mailbox;
import io.github.benlynchcode.relay.protocol.ErrorCode;
import io.github.benlynchcode.relay.protocol.FrameCodec;
import io.github.benlynchcode.relay.protocol.FrameTooLargeException;
import io.github.benlynchcode.relay.protocol.MalformedMessageException;
import io.github.benlynchcode.relay.protocol.Message;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * One TCP connection: a socket, a reader thread, a writer thread, and a bounded queue between them.
 *
 *   socket ──▶ reader ──▶ dispatch (REGISTER / SEND / ACK / PING)
 *                              │ replies
 *                              ▼
 *                       [ outbound queue ] ◀── CHECK_MAILBOX (doorbell, rung by other clients)
 *                              │
 *   socket ◀── writer ◀────────┘
 *
 * Temporary by design: it holds no message state. Messages live in the Mailbox, which this
 * connection only attaches to with a ticket. When the socket dies, nothing is lost.
 *
 * Only the writer ever writes to the socket, so one slow client can only ever block its own writer.
 */
final class ClientConnection {

    private static final System.Logger LOG = System.getLogger(ClientConnection.class.getName());

    /** What the writer can be asked to do. */
    private sealed interface Work permits Reply, Signal {
    }

    private record Reply(Message message) implements Work {
    }

    private enum Signal implements Work {
        /** "Your mailbox may have something to deliver." Carries no data; the writer pulls from the mailbox. */
        CHECK_MAILBOX,
        /** Write everything queued ahead of this, then close. */
        FINISH
    }

    private final String label;
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final ServerConfig config;
    private final FrameCodec codec;
    private final Registry registry;
    private final BooleanSupplier shuttingDown;
    private final Consumer<ClientConnection> onClosed;

    private final BlockingQueue<Work> outbound;
    private final AtomicBoolean wakePending = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final long registerDeadlineNanos;

    // Set once by the reader on REGISTER and read by the writer, hence volatile.
    private volatile String clientId;
    private volatile Mailbox mailbox;
    private volatile long ticket;
    // Off until REGISTERED is queued, so a DELIVER can never overtake the REGISTERED reply.
    private volatile boolean deliveryEnabled;
    // [Q8: Stuck writer] When the current write started (0 = not writing), so the server's watchdog can spot a stuck one.
    private volatile long writeStartedNanos;

    ClientConnection(String label, Socket socket, ServerConfig config, FrameCodec codec, Registry registry,
                     BooleanSupplier shuttingDown, Consumer<ClientConnection> onClosed) throws IOException {
        this.label = label;
        this.socket = socket;
        this.config = config;
        this.codec = codec;
        this.registry = registry;
        this.shuttingDown = shuttingDown;
        this.onClosed = onClosed;
        // [Q7: Slow client] Bounded: a client that stops reading can only ever cost us this many queued replies.
        this.outbound = new ArrayBlockingQueue<>(config.outboundBuffer());
        // [Q1: Idle timeout] A separate, shorter deadline before REGISTER, so anonymous sockets can't hold a slot.
        this.registerDeadlineNanos = System.nanoTime() + config.registerTimeout().toNanos();
        // Buffered, so a frame's 4-byte header and body go out in one write rather than two.
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    String label() {
        return label;
    }

    // ================================================================== reader thread

    void runReader() {
        try {
            while (true) {
                applyReadTimeout();
                Message message;
                try {
                    message = codec.decode(codec.readFrame(in));
                } catch (MalformedMessageException e) {
                    // [Q5: Edge inputs] The frame boundary was fine, so the stream is still in sync: reply and carry on.
                    reply(Message.error(ErrorCode.MALFORMED));
                    continue;
                }
                dispatch(message);
            }
        } catch (FrameTooLargeException e) {
            // [Q5: Edge inputs] A bad length means we no longer know where the next frame starts: tell them, then close.
            LOG.log(System.Logger.Level.WARNING, "{0}: {1}", label, e.getMessage());
            reply(Message.error(ErrorCode.FRAME_TOO_LARGE));
        } catch (SocketTimeoutException e) {
            LOG.log(System.Logger.Level.INFO, "{0}: timed out ({1})", label,
                    mailbox == null ? "never registered" : "idle");
        } catch (IOException e) {
            // EOF (client left) or our own close() from another thread. Both are normal endings.
        } finally {
            if (mailbox != null) {
                // [Q2: Re-register race] Pass our ticket: if a newer connection owns the mailbox, this is ignored.
                registry.unregister(clientId, this, ticket);
            }
            finish();
        }
    }

    private void applyReadTimeout() throws IOException {
        if (mailbox != null) {
            return;   // idle timeout was set once on REGISTER
        }
        // [Q1: Idle timeout] A deadline, not a per-read timeout: sending junk can't keep an unregistered socket alive.
        long remainingMs = TimeUnit.NANOSECONDS.toMillis(registerDeadlineNanos - System.nanoTime());
        if (remainingMs <= 0) {
            throw new SocketTimeoutException("register timeout");
        }
        socket.setSoTimeout((int) remainingMs);
    }

    private void dispatch(Message message) throws IOException {
        if (shuttingDown.getAsBoolean()) {
            reply(Message.error(ErrorCode.SHUTTING_DOWN));
            return;
        }
        switch (message.type()) {
            case REGISTER -> handleRegister(message);
            case SEND -> handleSend(message);
            case ACK -> handleAck(message);
            case PING -> handlePing();
            // Server-to-client types make no sense coming from a client.
            case REGISTERED, SEND_RESULT, DELIVER, PONG, ERROR -> reply(Message.error(ErrorCode.MALFORMED));
        }
    }

    private void handleRegister(Message message) throws IOException {
        if (mailbox != null) {
            reply(Message.error(ErrorCode.ALREADY_REGISTERED));
            return;
        }
        String id = message.clientId();
        if (!Ids.isValidClientId(id)) {
            reply(Message.error(ErrorCode.INVALID_ID));
            return;
        }

        Registry.Registration registration = registry.register(id, this, this::ring);
        clientId = id;
        ticket = registration.ticket();
        mailbox = registration.mailbox();

        // [Q1: Idle timeout] From now on any frame, including PING, resets this timer; silence for this long means the client is gone.
        socket.setSoTimeout((int) config.idleTimeout().toMillis());

        LOG.log(System.Logger.Level.INFO, "{0}: registered as ''{1}'' ({2} waiting)",
                label, id, registration.pending());
        reply(Message.registered(id, registration.pending()));
        deliveryEnabled = true;
        ring();   // queued messages start flowing, strictly after REGISTERED

        if (registration.replaced() != null) {
            LOG.log(System.Logger.Level.INFO, "{0}: took over ''{1}'' from {2}",
                    label, id, registration.replaced().label());
            // [Q2: Re-register race] New connection wins: close the old one. Its in-flight messages were already requeued by attach.
            registration.replaced().finish();
        }
    }

    private void handleSend(Message message) {
        if (!requireRegistered()) {
            return;
        }
        String id = message.id();
        if (id == null || id.isBlank()) {
            reply(Message.error(ErrorCode.MALFORMED));   // no id, so there's nothing to attach a SEND_RESULT to
            return;
        }
        if (!Ids.isValidMessageId(id)) {
            reply(Message.error(ErrorCode.INVALID_ID));  // don't echo a huge id back
            return;
        }
        if (!Ids.isValidClientId(message.to())) {
            reply(Message.sendResult(id, false, ErrorCode.INVALID_ID));
            return;
        }
        // [Q5: Edge inputs] An empty body "" is a valid message; a missing one is not.
        if (message.body() == null) {
            reply(Message.sendResult(id, false, ErrorCode.MALFORMED));
            return;
        }

        Optional<Mailbox> recipient = registry.find(message.to());
        if (recipient.isEmpty()) {
            reply(Message.sendResult(id, false, ErrorCode.UNKNOWN_RECIPIENT));
            return;
        }
        // The DELIVER we would build adds "from", so a SEND that fit in a frame might not. Refuse it now,
        // rather than accept a message that could never be delivered.
        if (codec.encode(Message.deliver(id, clientId, message.body())).length > config.maxFrame()) {
            reply(Message.sendResult(id, false, ErrorCode.BODY_TOO_LARGE));
            return;
        }

        EnqueueResult result = recipient.get().enqueue(clientId, id, message.body());
        reply(switch (result) {
            case ACCEPTED -> Message.sendResult(id, true, null);
            // A retry of something we already hold: tell the sender it's safe, so they stop retrying.
            case DUPLICATE -> Message.sendResult(id, true, ErrorCode.DUPLICATE);
            case ID_REUSED -> Message.sendResult(id, false, ErrorCode.ID_REUSED);
            case MAILBOX_FULL -> Message.sendResult(id, false, ErrorCode.MAILBOX_FULL);
        });
    }

    private void handleAck(Message message) {
        if (!requireRegistered()) {
            return;
        }
        if (message.from() == null || message.id() == null) {
            reply(Message.error(ErrorCode.MALFORMED));
            return;
        }
        // Only our own mailbox is ever touched, so a client can't remove someone else's message.
        if (!mailbox.ack(message.from(), message.id())) {
            reply(Message.error(ErrorCode.ACK_UNKNOWN, message.from(), message.id()));
        }
    }

    private void handlePing() {
        if (requireRegistered()) {
            // [Q1: Idle timeout] Reading the PING already reset our timer; PONG lets the client know WE are still alive.
            reply(Message.pong());
        }
    }

    private boolean requireRegistered() {
        if (mailbox == null) {
            reply(Message.error(ErrorCode.NOT_REGISTERED));
            return false;
        }
        return true;
    }

    // ================================================================== called from any thread

    /** Queues a reply for the writer. Never blocks. */
    void reply(Message message) {
        if (closed.get()) {
            return;
        }
        // [Q7: Slow client] offer, never put: a full queue means this client isn't reading. Drop the connection, not the caller's thread.
        if (!outbound.offer(new Reply(message))) {
            closeNow("outbound queue full (slow client)");
        }
    }

    /** The mailbox's doorbell. Called by whichever thread just made something deliverable. */
    private void ring() {
        if (!deliveryEnabled || closed.get()) {
            return;
        }
        // [Q9: Waiting on two things] At most one CHECK_MAILBOX queued: it carries no data, so one check finds everything.
        if (wakePending.compareAndSet(false, true) && !outbound.offer(Signal.CHECK_MAILBOX)) {
            closeNow("outbound queue full (slow client)");
        }
    }

    /** Graceful close: the writer sends everything already queued, then closes. */
    void finish() {
        if (!closed.get() && !outbound.offer(Signal.FINISH)) {
            closeNow("outbound queue full while finishing");
        }
    }

    /** Server shutdown: say why, then finish. */
    void shutdownNotice() {
        reply(Message.error(ErrorCode.SHUTTING_DOWN));
        finish();
    }

    /** Hard close. Safe to call any number of times, from any thread. */
    void closeNow(String reason) {
        // [Q8: Stuck writer] Idempotent: the watchdog, a slow-client kick, the reader and shutdown may all race to get here.
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        LOG.log(System.Logger.Level.INFO, "{0}: closing ({1})", label, reason);
        deliveryEnabled = false;
        try {
            // [Q8: Stuck writer] Closing the socket from another thread is the only way to free a blocked write(); it also frees the reader.
            socket.close();
        } catch (IOException ignored) {
            // already gone
        }
        outbound.clear();
        outbound.offer(Signal.FINISH);   // wakes a writer parked in take()
        onClosed.accept(this);
    }

    boolean isWriteStuck(long nowNanos, long timeoutNanos) {
        long started = writeStartedNanos;
        return started != 0 && nowNanos - started > timeoutNanos;
    }

    // ================================================================== writer thread

    void runWriter() {
        try {
            while (true) {
                // [Q9: Waiting on two things] One queue holds both replies and the doorbell signal, because Java has no select.
                Work work = outbound.take();
                if (work instanceof Reply reply) {
                    write(reply.message());
                } else if (work == Signal.CHECK_MAILBOX) {
                    deliverNext();
                } else {
                    return;   // FINISH: everything queued before it has been written
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            // Socket closed or broken: nothing more to write.
        } finally {
            closeNow("writer finished");
        }
    }

    private void deliverNext() throws IOException {
        // [Q9: Waiting on two things] Clear BEFORE checking: mail arriving during the check rings again instead of being missed.
        wakePending.set(false);
        // [Q2: Re-register race] Our ticket is checked: if a newer connection owns the mailbox we get nothing.
        Optional<Envelope> next = mailbox.nextForDelivery(ticket);
        if (next.isPresent()) {
            Envelope envelope = next.get();
            write(Message.deliver(envelope.id(), envelope.from(), envelope.body()));
        }
    }

    private void write(Message message) throws IOException {
        byte[] body = codec.encode(message);
        writeStartedNanos = System.nanoTime();
        try {
            codec.writeFrame(out, body);
        } finally {
            writeStartedNanos = 0;
        }
    }
}
