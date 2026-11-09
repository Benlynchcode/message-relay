package io.github.benlynchcode.relay.mailbox;

/** One stored message. Immutable, so it can be handed between threads without locking. */
public record Envelope(String from, String id, String body) {

    /** The identity of a message: ids are only unique per sender, so the key is (from, id). */
    MessageKey key() {
        return new MessageKey(from, id);
    }
}
