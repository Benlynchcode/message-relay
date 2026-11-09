package io.github.benlynchcode.relay.server;

import io.github.benlynchcode.relay.mailbox.Attachment;
import io.github.benlynchcode.relay.mailbox.Mailbox;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Who exists, and who is online.
 *
 * The server owns the mailboxes here, keyed by client ID, for the server's whole lifetime.
 * That is what lets a client disconnect and come back to the same messages: the mailbox
 * never belonged to the connection in the first place.
 */
final class Registry {

    /** What a successful REGISTER hands back to the connection. */
    record Registration(Mailbox mailbox, long ticket, int pending, ClientConnection replaced) {
    }

    private final int mailboxSize;
    private final int dedupeWindow;

    // Read on every SEND, so lock-free lookups.
    private final ConcurrentHashMap<String, Mailbox> mailboxes = new ConcurrentHashMap<>();

    // [Q2: Re-register race] One lock so "who is online" and "who holds the mailbox ticket" change together; registration is rare, so it's cheap.
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, ClientConnection> online = new HashMap<>();

    Registry(int mailboxSize, int dedupeWindow) {
        this.mailboxSize = mailboxSize;
        this.dedupeWindow = dedupeWindow;
    }

    /**
     * Claims an identity for a connection, creating its mailbox on first use.
     *
     * @return the registration, including any connection that held this identity before.
     *         The caller closes that one, outside this lock.
     */
    Registration register(String clientId, ClientConnection connection, Runnable doorbell) {
        lock.lock();
        try {
            Mailbox mailbox = mailboxes.computeIfAbsent(
                    clientId, id -> new Mailbox(id, mailboxSize, dedupeWindow));
            // [Q2: Re-register race] New connection wins: an old one may be a half-open socket we can't detect yet.
            ClientConnection replaced = online.put(clientId, connection);
            Attachment attachment = mailbox.attach(doorbell);
            return new Registration(mailbox, attachment.ticket(), attachment.pending(),
                    replaced == connection ? null : replaced);
        } finally {
            lock.unlock();
        }
    }

    /** A connection has gone. Harmless if a newer connection has already taken the identity. */
    void unregister(String clientId, ClientConnection connection, long ticket) {
        lock.lock();
        try {
            // Remove only if it's still us: never take the newer connection offline.
            online.remove(clientId, connection);
            Mailbox mailbox = mailboxes.get(clientId);
            if (mailbox != null) {
                mailbox.detach(ticket);   // requeues in-flight; a no-op for a stale ticket
            }
        } finally {
            lock.unlock();
        }
    }

    /** The mailbox for a client ID, if that client has ever registered. */
    Optional<Mailbox> find(String clientId) {
        return Optional.ofNullable(mailboxes.get(clientId));
    }
}
