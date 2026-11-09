package io.github.benlynchcode.relay.server;

import java.util.regex.Pattern;

/** Rules for identifiers coming from clients. Never trust client input. */
final class Ids {

    // [Q5: Edge inputs] Client IDs live for the server's lifetime and appear in every frame and log line, so they are short and plain.
    private static final Pattern CLIENT_ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    // [Q5: Edge inputs] Message IDs are opaque to us but capped, since we store them in the dedupe window.
    static final int MAX_MESSAGE_ID_LENGTH = 128;

    private Ids() {
    }

    static boolean isValidClientId(String id) {
        return id != null && CLIENT_ID.matcher(id).matches();
    }

    static boolean isValidMessageId(String id) {
        return id != null && !id.isBlank() && id.length() <= MAX_MESSAGE_ID_LENGTH;
    }
}
