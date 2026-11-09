package io.github.benlynchcode.relay.protocol;

import java.io.IOException;

// [Q5: Edge inputs] An IOException because a bad length means the byte stream itself can't be trusted: the caller closes.
public final class FrameTooLargeException extends IOException {

    public FrameTooLargeException(int length, int maxFrame) {
        super("frame length " + length + " not in 1.." + maxFrame);
    }
}
