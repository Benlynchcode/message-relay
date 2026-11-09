package io.github.benlynchcode.relay.protocol;

/** The frame was intact but its contents weren't a valid Message. The stream is still usable. */
public final class MalformedMessageException extends Exception {

    public MalformedMessageException(String message) {
        super(message);
    }

    public MalformedMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
