package io.github.benlynchcode.relay.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One frame body. A single record for every type keeps the protocol in one place;
 * each type uses a subset of the fields (see the operations table in APPROACH.md).
 *
 * Null fields are left out of the JSON, so a PING is just {"type":"PING"}.
 * Boxed Boolean/Integer so "absent" (null) is different from false/0.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Message(
        MessageType type,
        @JsonProperty("client_id") String clientId,
        String id,
        String to,
        String from,
        String body,
        Boolean accepted,
        ErrorCode code,
        Integer pending) {

    // ------------------------------------------------------------------ client -> server

    public static Message register(String clientId) {
        return new Message(MessageType.REGISTER, clientId, null, null, null, null, null, null, null);
    }

    public static Message send(String id, String to, String body) {
        return new Message(MessageType.SEND, null, id, to, null, body, null, null, null);
    }

    public static Message ack(String from, String id) {
        return new Message(MessageType.ACK, null, id, null, from, null, null, null, null);
    }

    public static Message ping() {
        return new Message(MessageType.PING, null, null, null, null, null, null, null, null);
    }

    // ------------------------------------------------------------------ server -> client

    public static Message registered(String clientId, int pending) {
        return new Message(MessageType.REGISTERED, clientId, null, null, null, null, null, null, pending);
    }

    /** {@code code} is null for a plain accept, DUPLICATE for an accepted retry, or the rejection reason. */
    public static Message sendResult(String id, boolean accepted, ErrorCode code) {
        return new Message(MessageType.SEND_RESULT, null, id, null, null, null, accepted, code, null);
    }

    public static Message deliver(String id, String from, String body) {
        return new Message(MessageType.DELIVER, null, id, null, from, body, null, null, null);
    }

    public static Message pong() {
        return new Message(MessageType.PONG, null, null, null, null, null, null, null, null);
    }

    public static Message error(ErrorCode code) {
        return new Message(MessageType.ERROR, null, null, null, null, null, null, code, null);
    }

    /** An error about one specific message, e.g. ACK_UNKNOWN, so the client can tell which. */
    public static Message error(ErrorCode code, String from, String id) {
        return new Message(MessageType.ERROR, null, id, null, from, null, null, code, null);
    }
}
