package io.github.benlynchcode.relay.protocol;

/** Every error the server can report. Fixed at compile time, so a typo can't reach the wire. */
public enum ErrorCode {
    // Stream can't be trusted: the connection is closed after sending these.
    FRAME_TOO_LARGE,
    SERVER_BUSY,
    SHUTTING_DOWN,

    // Request was readable but invalid: reply and keep the connection open.
    MALFORMED,
    NOT_REGISTERED,
    ALREADY_REGISTERED,
    INVALID_ID,
    UNKNOWN_RECIPIENT,
    MAILBOX_FULL,
    DUPLICATE,
    ID_REUSED,
    ACK_UNKNOWN
}
