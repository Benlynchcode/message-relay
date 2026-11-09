package io.github.benlynchcode.relay.mailbox;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One client's messages. Lives for the server's lifetime; connections come and go.
 *
 *   enqueue ──▶ pending ──nextForDelivery──▶ inFlight ──ack──▶ removed
 *                  ▲                            │
 *                  └──── attach / detach ───────┘  (requeued to the FRONT, so FIFO survives reconnects)
 *
 * Pure logic: no sockets, no threads of its own. The connection talks to it through a
 * ticket (proof it is the current owner) and a doorbell (a Runnable the mailbox calls
 * when there is something new to deliver).
 *
 * Thread safety: every public method takes the one lock, so each is a single atomic step.
 */
public final class Mailbox {

    /** Strict FIFO: the next message waits until the current one is acknowledged. */
    static final int MAX_IN_FLIGHT = 1;

    private final String owner;
    private final int maxSize;

    // ReentrantLock, not synchronized: on Java 21 a virtual thread that blocks inside
    // synchronized pins its carrier thread, and our connections run on virtual threads.
    private final ReentrantLock lock = new ReentrantLock();

    private final ArrayDeque<Envelope> pending = new ArrayDeque<>();
    // LinkedHashMap: O(1) lookup for ACK, and remembers order for requeueing.
    private final LinkedHashMap<MessageKey, Envelope> inFlight = new LinkedHashMap<>();

    // [Q4: Dedupe state location] Lives in the recipient's mailbox, so one lock covers check, room, store and record.
    private final LinkedHashMap<MessageKey, String> seen;

    private long currentTicket = 0;
    private Runnable doorbell;   // null while the owner is offline

    /**
     * @param dedupeWindow how many recent (from, id) pairs to remember. Must be at least
     *                     maxSize, or IDs of messages still queued could be forgotten and a
     *                     retry stored twice.
     */
    public Mailbox(String owner, int maxSize, int dedupeWindow) {
        this.owner = Objects.requireNonNull(owner);
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be > 0");
        }
        // [Q3: Dedupe vs reject] A window smaller than the mailbox forgets IDs still queued, so retries would be stored twice.
        if (dedupeWindow < maxSize) {
            throw new IllegalArgumentException(
                    "dedupeWindow (" + dedupeWindow + ") must be >= maxSize (" + maxSize + ")");
        }
        this.maxSize = maxSize;
        // Bounded: insertion-ordered map that drops its oldest entry once over the window.
        this.seen = new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<MessageKey, String> eldest) {
                return size() > dedupeWindow;
            }
        };
    }

    public String owner() {
        return owner;
    }

    /** Stores a message for this mailbox's owner. Called from the SENDER's connection. */
    public EnqueueResult enqueue(String from, String id, String body) {
        Envelope envelope = new Envelope(Objects.requireNonNull(from), Objects.requireNonNull(id),
                Objects.requireNonNull(body));
        MessageKey key = envelope.key();
        String bodyHash = sha256(body);   // computed outside the lock: it's the only slow part

        Runnable toRing;
        lock.lock();
        try {
            // [Q3: Dedupe vs reject] Duplicate check comes BEFORE the room check, so a retry of a stored message gets DUPLICATE, not MAILBOX_FULL.
            String previous = seen.get(key);
            if (previous != null) {
                return previous.equals(bodyHash) ? EnqueueResult.DUPLICATE : EnqueueResult.ID_REUSED;
            }
            if (size() >= maxSize) {
                // [Q3: Dedupe vs reject] Nothing recorded on rejection, so the sender's retry after room frees up is accepted.
                return EnqueueResult.MAILBOX_FULL;
            }
            pending.addLast(envelope);
            // [Q3: Dedupe vs reject] Recorded only once actually stored, under the same lock as the store.
            seen.put(key, bodyHash);
            toRing = doorbell;
        } finally {
            lock.unlock();
        }
        // [Q9: Waiting on two things] New mail rings the owner's doorbell; rung after unlocking so we never call out while holding the lock.
        ring(toRing);
        return EnqueueResult.ACCEPTED;
    }

    /**
     * A connection claims this mailbox. Any previous connection is now stale.
     *
     * @param doorbell called (outside the lock) whenever there may be something new to deliver
     */
    public Attachment attach(Runnable doorbell) {
        Objects.requireNonNull(doorbell);
        Attachment attachment;
        lock.lock();
        try {
            // [Q2: Re-register race] Requeue here, not in the old connection's detach: that detach will be ignored as stale.
            requeueInFlight();
            // [Q2: Re-register race] A new ticket makes every earlier connection's ticket stale.
            currentTicket++;
            this.doorbell = doorbell;
            attachment = new Attachment(currentTicket, pending.size());
        } finally {
            lock.unlock();
        }
        // [Q9: Waiting on two things] Attach rings so queued offline messages start draining.
        ring(doorbell);
        return attachment;
    }

    /**
     * Moves the next pending message to in-flight and returns it, if the caller is still the
     * owner, the owner is online, and the in-flight limit allows it.
     */
    public Optional<Envelope> nextForDelivery(long ticket) {
        lock.lock();
        try {
            // [Q2: Re-register race] A stale writer must not pull a message onto a dead socket and use up the in-flight slot.
            if (!isCurrent(ticket) || inFlight.size() >= MAX_IN_FLIGHT) {
                return Optional.empty();
            }
            Envelope next = pending.pollFirst();
            if (next == null) {
                return Optional.empty();
            }
            inFlight.put(next.key(), next);
            return Optional.of(next);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes an acknowledged message. Not ticket-checked: a late ACK from an old connection
     * for a message the owner really received is correct to honour.
     *
     * @return false if (from, id) is not in flight: stale, repeated, or never delivered
     */
    public boolean ack(String from, String id) {
        Runnable toRing;
        lock.lock();
        try {
            if (inFlight.remove(new MessageKey(from, id)) == null) {
                return false;
            }
            toRing = doorbell;
        } finally {
            lock.unlock();
        }
        // [Q9: Waiting on two things] An ACK frees the in-flight slot, so the next message can go.
        ring(toRing);
        return true;
    }

    /** The connection with this ticket has gone. A no-op if a newer connection has taken over. */
    public void detach(long ticket) {
        lock.lock();
        try {
            // [Q2: Re-register race] The old connection's late cleanup must not touch the new connection's state.
            if (!isCurrent(ticket)) {
                return;
            }
            requeueInFlight();
            doorbell = null;
        } finally {
            lock.unlock();
        }
    }

    public int pendingCount() {
        lock.lock();
        try {
            return pending.size();
        } finally {
            lock.unlock();
        }
    }

    public int inFlightCount() {
        lock.lock();
        try {
            return inFlight.size();
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ helpers (lock held)

    private boolean isCurrent(long ticket) {
        return ticket == currentTicket && doorbell != null;
    }

    /** Counts in-flight too: a client that never ACKs still holds those messages. */
    private int size() {
        return pending.size() + inFlight.size();
    }

    /** In-flight back to the FRONT of pending, keeping their order, so FIFO survives a reconnect. */
    private void requeueInFlight() {
        List<Envelope> toRequeue = new ArrayList<>(inFlight.values());
        inFlight.clear();
        // Walk backwards: addFirst in forward order would reverse them.
        for (int i = toRequeue.size() - 1; i >= 0; i--) {
            pending.addFirst(toRequeue.get(i));
        }
    }

    private static void ring(Runnable doorbell) {
        if (doorbell != null) {
            doorbell.run();
        }
    }

    // A hash, not the body itself: remembering 1000 bodies of up to 64 KB each would cost ~64 MB per mailbox.
    private static String sha256(String body) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(body.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required on every JVM", e);
        }
    }
}
