package io.github.benlynchcode.relay.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Pure in-memory tests: byte arrays stand in for the socket, so no ports, threads or timing.
@Timeout(5)
class FrameCodecTest {

    private static final int MAX_FRAME = 64;

    private final FrameCodec codec = new FrameCodec(MAX_FRAME);

    @Test
    void roundTripPreservesMessage() throws Exception {
        // Non-ASCII body proves the UTF-8 path: "é", "✓" and "你好" are 2, 3 and 3 bytes each.
        Message sent = new Message(MessageType.SEND, null, "m1", "bob", null, "héllo ✓ 你好",
                null, null, null);

        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        codec.writeFrame(new DataOutputStream(wire), codec.encode(sent));
        Message received = codec.decode(codec.readFrame(input(wire.toByteArray())));

        assertEquals(sent, received);
    }

    @Test
    void wireFormatUsesSnakeCaseAndOmitsNulls() {
        String json = new String(codec.encode(new Message(MessageType.REGISTER, "alice",
                null, null, null, null, null, null, null)), StandardCharsets.UTF_8);

        assertEquals("{\"type\":\"REGISTER\",\"client_id\":\"alice\"}", json);
    }

    @Test
    void twoFramesBackToBackAreReadSeparately() throws Exception {
        // The reason framing exists: TCP has no message boundaries, so both frames can
        // arrive in one read. The length prefix is what splits them apart again.
        byte[] first = "{\"type\":\"PING\"}".getBytes(StandardCharsets.UTF_8);
        byte[] second = "{\"type\":\"PONG\"}".getBytes(StandardCharsets.UTF_8);
        DataInputStream in = input(concat(framed(first), framed(second)));

        assertArrayEquals(first, codec.readFrame(in));
        assertArrayEquals(second, codec.readFrame(in));
        assertThrows(EOFException.class, () -> codec.readFrame(in));
    }

    @Test
    void oversizedLengthIsRejectedBeforeAllocating() {
        // Header claims ~2 GB and NO body follows. If the codec allocated and read first, it
        // would fail with EOFException (or run out of memory); FrameTooLargeException proves
        // the check happened before either.
        DataInputStream in = input(header(Integer.MAX_VALUE));

        assertThrows(FrameTooLargeException.class, () -> codec.readFrame(in));
    }

    @Test
    void lengthExactlyAtLimitIsAcceptedAndOneOverIsNot() throws Exception {
        byte[] atLimit = new byte[MAX_FRAME];

        assertArrayEquals(atLimit, codec.readFrame(input(framed(atLimit))));
        assertThrows(FrameTooLargeException.class,
                () -> codec.readFrame(input(framed(new byte[MAX_FRAME + 1]))));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
    void negativeAndZeroLengthsAreRejected(int length) {
        // readInt is signed: header bytes FF FF FF FF read as -1, not 4 billion.
        assertThrows(FrameTooLargeException.class, () -> codec.readFrame(input(header(length))));
    }

    @Test
    void truncatedBodyThrowsEof() {
        // Header promises 10 bytes, only 4 arrive before the stream ends.
        DataInputStream in = input(concat(header(10), new byte[4]));

        assertThrows(EOFException.class, () -> codec.readFrame(in));
    }

    @Test
    void truncatedHeaderThrowsEof() {
        assertThrows(EOFException.class, () -> codec.readFrame(input(new byte[]{0, 0})));
    }

    @Test
    void oversizedOutgoingFrameIsRefusedAndNothingIsWritten() {
        ByteArrayOutputStream wire = new ByteArrayOutputStream();

        assertThrows(FrameTooLargeException.class,
                () -> codec.writeFrame(new DataOutputStream(wire), new byte[MAX_FRAME + 1]));
        assertEquals(0, wire.size());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "not json",
            "{}",                                    // missing type
            "{\"type\":\"SNED\"}",                   // unknown type
            "{\"type\":\"SEND\",\"too\":\"bob\"}",   // unknown field
            "{\"type\":\"PING\"} junk"               // trailing tokens
    })
    void validFrameWithBadContentIsMalformedNotIoError(String json) throws Exception {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);

        // Framing succeeds, so the stream is still in sync...
        byte[] read = codec.readFrame(input(framed(body)));

        // ...and only decoding fails, with the checked type that means "reply, don't close".
        // (It is never an IOException: the compiler rejects even asking, so the split is enforced by types.)
        MalformedMessageException e = assertThrows(MalformedMessageException.class,
                () -> codec.decode(read));
        assertTrue(e.getMessage() != null && !e.getMessage().isBlank());
    }

    // ------------------------------------------------------------------ helpers

    private static DataInputStream input(byte[] bytes) {
        return new DataInputStream(new ByteArrayInputStream(bytes));
    }

    /** A 4-byte big-endian length header. ByteBuffer is big-endian by default. */
    private static byte[] header(int length) {
        return ByteBuffer.allocate(4).putInt(length).array();
    }

    private static byte[] framed(byte[] body) {
        return concat(header(body.length), body);
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }
}
