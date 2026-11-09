package io.github.benlynchcode.relay.mailbox;

/**
 * Returned by {@link Mailbox#attach}.
 *
 * @param ticket  proof of ownership; every later owner operation must present it
 * @param pending messages waiting at the moment of attach (after requeueing), for the REGISTERED reply
 */
public record Attachment(long ticket, int pending) {
}
