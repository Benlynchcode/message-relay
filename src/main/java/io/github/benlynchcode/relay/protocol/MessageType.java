package io.github.benlynchcode.relay.protocol;

/**
 * Every operation on the wire. An enum rather than a String so an unknown type ("SNED")
 * fails in decode as MALFORMED, and switches over it can be checked for missing cases.
 */
public enum MessageType {
    // client -> server
    REGISTER,
    SEND,
    ACK,
    PING,

    // server -> client
    REGISTERED,
    SEND_RESULT,
    DELIVER,
    PONG,
    ERROR
}
