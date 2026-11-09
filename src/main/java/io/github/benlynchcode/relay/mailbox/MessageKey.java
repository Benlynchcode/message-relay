package io.github.benlynchcode.relay.mailbox;

/** (from, id). Two senders can both use id "1", so id alone is not a unique key. */
record MessageKey(String from, String id) {
}
