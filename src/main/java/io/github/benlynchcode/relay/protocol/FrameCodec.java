package io.github.benlynchcode.relay.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Wire format: a 4-byte big-endian length, then that many bytes of UTF-8 JSON.
 *
 * Two layers on purpose:
 *   readFrame/writeFrame move raw bytes and know nothing about JSON.
 *   decode/encode turn bytes into a Message and know nothing about sockets.
 */
public final class FrameCodec {

    private final int maxFrame;
    private final ObjectMapper mapper;

    public FrameCodec(int maxFrame) {
        if (maxFrame <= 0) {
            throw new IllegalArgumentException("maxFrame must be > 0");
        }
        this.maxFrame = maxFrame;
        // Built once and shared: creating a mapper is expensive, and it is thread-safe once configured.
        // Strict on purpose: an unknown field (a typo like "too") or junk after the JSON is MALFORMED, not silently ignored.
        this.mapper = JsonMapper.builder()
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .build();
    }

    /**
     * Reads exactly one frame body.
     *
     * @throws java.io.EOFException   the stream ended before a whole frame arrived (clean close or truncated)
     * @throws FrameTooLargeException length is <= 0 or > maxFrame; nothing was allocated
     */
    public byte[] readFrame(DataInputStream in) throws IOException {
        // Signed: a header of FF FF FF FF reads as -1, which is why <= 0 is checked too.
        int length = in.readInt();
        // Check before allocating, or a hostile 2 GB header makes us allocate 2 GB.
        if (length <= 0 || length > maxFrame) {
            throw new FrameTooLargeException(length, maxFrame);
        }
        byte[] body = new byte[length];
        // readFully, not read: read() may return fewer bytes than asked for.
        in.readFully(body);
        return body;
    }

    /** Writes one frame and flushes. Caller guarantees only one thread writes to `out`. */
    public void writeFrame(DataOutputStream out, byte[] body) throws IOException {
        // The server builds DELIVER frames itself, adding "from" to a body that fit as a SEND.
        if (body.length > maxFrame) {
            throw new FrameTooLargeException(body.length, maxFrame);
        }
        out.writeInt(body.length);
        out.write(body);
        // Without flush the frame can sit in the buffer while the peer waits forever.
        out.flush();
    }

    // [Q5: Edge inputs] A checked, separate exception so the reader can reply MALFORMED and keep a still-in-sync stream open.
    public Message decode(byte[] body) throws MalformedMessageException {
        Message message;
        try {
            message = mapper.readValue(body, Message.class);
        } catch (IOException e) {
            // Covers bad JSON, unknown fields, and unknown enum values like "type":"SNED".
            throw new MalformedMessageException("invalid message: " + e.getMessage(), e);
        }
        if (message.type() == null) {
            throw new MalformedMessageException("missing type");
        }
        return message;
    }

    public byte[] encode(Message message) {
        try {
            return mapper.writeValueAsBytes(message);
        } catch (JsonProcessingException e) {
            // Encoding our own record can only fail through a bug of ours, so don't make callers handle it.
            throw new IllegalStateException("could not encode " + message.type(), e);
        }
    }
}
