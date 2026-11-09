package io.github.benlynchcode.relay.mailbox;

/** Outcome of {@link Mailbox#enqueue}. The server maps each to a SEND_RESULT. */
public enum EnqueueResult {
    /** Stored. */
    ACCEPTED,
    /** Same (from, id) and same body seen before: a retry. Not stored again, but the sender may treat it as success. */
    DUPLICATE,
    /** Same (from, id) but a different body: a client bug, rejected. */
    ID_REUSED,
    /** No room. Nothing stored and nothing recorded, so the sender can retry later. */
    MAILBOX_FULL
}
