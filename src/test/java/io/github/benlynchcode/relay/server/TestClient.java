package io.github.benlynchcode.relay.server;

import io.github.benlynchcode.relay.protocol.FrameCodec;
import io.github.benlynchcode.relay.protocol.Message;
import io.github.benlynchcode.relay.protocol.MessageType;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A blocking test client over a real socket. Every read has a timeout, so a missing frame
 * fails the test quickly instead of hanging it: tests wait on real frames, never on sleeps.
 */
final class TestClient implements AutoCloseable {

    private static final int READ_TIMEOUT_MS = 5_000;

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final FrameCodec codec = new FrameCodec(1 << 20);

    private TestClient(Socket socket) throws IOException {
        this.socket = socket;
        socket.setSoTimeout(READ_TIMEOUT_MS);
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(socket.getOutputStream());
    }

    static TestClient connect(int port) throws IOException {
        return new TestClient(new Socket("127.0.0.1", port));
    }

    /** Connects and registers, asserting REGISTERED. */
    static TestClient registered(int port, String clientId) throws IOException {
        TestClient client = connect(port);
        client.send(Message.register(clientId));
        client.expect(MessageType.REGISTERED);
        return client;
    }

    void send(Message message) throws IOException {
        codec.writeFrame(out, codec.encode(message));
    }

    void sendRaw(byte[] bytes) throws IOException {
        out.write(bytes);
        out.flush();
    }

    /** The next frame from the server, or a test failure after the read timeout. */
    Message read() throws IOException {
        try {
            return codec.decode(codec.readFrame(in));
        } catch (SocketTimeoutException e) {
            return fail("no frame from the server within " + READ_TIMEOUT_MS + "ms");
        } catch (Exception e) {
            if (e instanceof IOException io) {
                throw io;
            }
            return fail("server sent an undecodable frame", e);
        }
    }

    Message expect(MessageType type) throws IOException {
        Message message = read();
        assertEquals(type, message.type(), () -> "unexpected frame: " + message);
        return message;
    }

    /** Asserts the server closes this connection (EOF or reset) within the read timeout. */
    void expectClosedByServer() throws IOException {
        try {
            while (true) {
                codec.readFrame(in);   // ignore anything sent before the close
            }
        } catch (EOFException | SocketException closed) {
            // expected
        } catch (SocketTimeoutException e) {
            fail("server did not close the connection within " + READ_TIMEOUT_MS + "ms");
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
