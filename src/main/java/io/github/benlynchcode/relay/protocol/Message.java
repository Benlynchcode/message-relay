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
}
