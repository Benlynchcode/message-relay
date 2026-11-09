package io.github.benlynchcode.relay.server;

import io.github.benlynchcode.relay.protocol.ErrorCode;
import io.github.benlynchcode.relay.protocol.Message;
import io.github.benlynchcode.relay.protocol.MessageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End to end over real TCP: a real server on an OS-chosen port, real sockets, no mocks.
 * Every wait is on an actual frame (with a read timeout), never on Thread.sleep.
 */
@Timeout(30)
class RelayServerTest {

    private RelayServer server;

    private int start(String... overrides) throws IOException {
        String[] args = new String[overrides.length + 2];
        args[0] = "--host=127.0.0.1";
        args[1] = "--port=0";   // the OS picks a free port, so tests never collide
        System.arraycopy(overrides, 0, args, 2, overrides.length);
        server = new RelayServer(ServerConfig.fromArgs(args));
        server.start();
        return server.port();
    }

    @AfterEach
    void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    // ------------------------------------------------------------------ core scenario

    @Test
    void happyPathSendDeliverAck() throws Exception {
        int port = start();
        try (TestClient alice = TestClient.registered(port, "alice");
             TestClient bob = TestClient.registered(port, "bob")) {

            alice.send(Message.send("m1", "bob", "hello"));
            Message result = alice.expect(MessageType.SEND_RESULT);
            assertEquals("m1", result.id());
            assertTrue(result.accepted());

            assertEquals(Message.deliver("m1", "alice", "hello"), bob.expect(MessageType.DELIVER));
            bob.send(Message.ack("alice", "m1"));

            // The ACK freed the in-flight slot, so the next message is delivered.
            alice.send(Message.send("m2", "bob", "again"));
            alice.expect(MessageType.SEND_RESULT);
            assertEquals("m2", bob.expect(MessageType.DELIVER).id());
        }
    }

    @Test
    void offlineMessagesAreDeliveredOnReconnect() throws Exception {
        int port = start();
        TestClient.registered(port, "bob").close();   // bob exists, then goes offline

        try (TestClient alice = TestClient.registered(port, "alice")) {
            alice.send(Message.send("m1", "bob", "while you were out"));
            assertTrue(alice.expect(MessageType.SEND_RESULT).accepted());
        }

        try (TestClient bob = TestClient.connect(port)) {
            bob.send(Message.register("bob"));
            assertEquals(1, bob.expect(MessageType.REGISTERED).pending());
            assertEquals("m1", bob.expect(MessageType.DELIVER).id());
        }
    }

    @Test
    void unackedMessageIsRedeliveredAfterReconnect() throws Exception {
        int port = start();
        try (TestClient alice = TestClient.registered(port, "alice")) {
            try (TestClient bob = TestClient.registered(port, "bob")) {
                alice.send(Message.send("m1", "bob", "please ack"));
                assertEquals("m1", bob.expect(MessageType.DELIVER).id());
                // bob disconnects without acknowledging
            }
            try (TestClient bob = TestClient.registered(port, "bob")) {
                assertEquals("m1", bob.expect(MessageType.DELIVER).id());
            }
        }
    }

    @Test
    void fifoOrderSurvivesAReconnect() throws Exception {
        int port = start();
        try (TestClient alice = TestClient.registered(port, "alice")) {
            try (TestClient bob = TestClient.registered(port, "bob")) {
                for (String id : new String[]{"1", "2", "3"}) {
                    alice.send(Message.send(id, "bob", "msg " + id));
                    alice.expect(MessageType.SEND_RESULT);
                }
                assertEquals("1", bob.expect(MessageType.DELIVER).id());
                // disconnect with "1" in flight and "2", "3" still pending
            }
            try (TestClient bob = TestClient.registered(port, "bob")) {
                for (String id : new String[]{"1", "2", "3"}) {
                    assertEquals(id, bob.expect(MessageType.DELIVER).id());
                    bob.send(Message.ack("alice", id));
                }
            }
        }
    }

    @Test
    void newConnectionTakesOverTheIdentity() throws Exception {
        int port = start();
        try (TestClient alice = TestClient.registered(port, "alice");
             TestClient oldBob = TestClient.registered(port, "bob")) {
            alice.send(Message.send("m1", "bob", "hi"));
            assertEquals("m1", oldBob.expect(MessageType.DELIVER).id());

            // bob reconnects while the old socket still looks alive (e.g. a half-open connection)
            try (TestClient newBob = TestClient.connect(port)) {
                newBob.send(Message.register("bob"));
                assertEquals(1, newBob.expect(MessageType.REGISTERED).pending());   // m1 requeued
                assertEquals("m1", newBob.expect(MessageType.DELIVER).id());
                oldBob.expectClosedByServer();
            }
        }
    }

    // ------------------------------------------------------------------ rejections and limits

    @Test
    void sendsAreRejectedWithAReason() throws Exception {
        int port = start("--mailbox-size=2", "--dedupe-window=2");
        TestClient.registered(port, "bob").close();

        try (TestClient alice = TestClient.registered(port, "alice")) {
            alice.send(Message.send("x", "nobody", "hi"));
            assertEquals(ErrorCode.UNKNOWN_RECIPIENT, alice.expect(MessageType.SEND_RESULT).code());

            alice.send(Message.send("m1", "bob", "1"));
            alice.expect(MessageType.SEND_RESULT);
            alice.send(Message.send("m2", "bob", "2"));
            alice.expect(MessageType.SEND_RESULT);

            alice.send(Message.send("m3", "bob", "3"));
            Message full = alice.expect(MessageType.SEND_RESULT);
            assertFalse(full.accepted());
            assertEquals(ErrorCode.MAILBOX_FULL, full.code());

            // A retry of a message already held is reported as a safe duplicate, not as full.
            alice.send(Message.send("m1", "bob", "1"));
            Message duplicate = alice.expect(MessageType.SEND_RESULT);
            assertTrue(duplicate.accepted());
            assertEquals(ErrorCode.DUPLICATE, duplicate.code());
        }
    }

    @Test
    void actionsBeforeRegisterAreRefused() throws Exception {
        int port = start();
        try (TestClient client = TestClient.connect(port)) {
            client.send(Message.send("m1", "bob", "hi"));
            assertEquals(ErrorCode.NOT_REGISTERED, client.expect(MessageType.ERROR).code());

            client.send(Message.register("not a valid id!"));
            assertEquals(ErrorCode.INVALID_ID, client.expect(MessageType.ERROR).code());
        }
    }

    @Test
    void badJsonGetsAnErrorButTheConnectionStaysOpen() throws Exception {
        int port = start();
        try (TestClient client = TestClient.connect(port)) {
            client.sendRaw(framed("this is not json"));
            assertEquals(ErrorCode.MALFORMED, client.expect(MessageType.ERROR).code());

            client.send(Message.register("alice"));   // same connection still works
            client.expect(MessageType.REGISTERED);
        }
    }

    @Test
    void oversizedFrameClosesOnlyThatConnection() throws Exception {
        int port = start();
        try (TestClient alice = TestClient.registered(port, "alice");
             TestClient bob = TestClient.registered(port, "bob");
             TestClient attacker = TestClient.connect(port)) {

            attacker.sendRaw(ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array());
            assertEquals(ErrorCode.FRAME_TOO_LARGE, attacker.expect(MessageType.ERROR).code());
            attacker.expectClosedByServer();

            alice.send(Message.send("m1", "bob", "still working"));
            assertEquals("m1", bob.expect(MessageType.DELIVER).id());
        }
    }

    @Test
    void connectionLimitIsEnforcedWithServerBusy() throws Exception {
        int port = start("--max-conns=1");
        try (TestClient first = TestClient.registered(port, "alice");
             TestClient second = TestClient.connect(port)) {
            assertEquals(ErrorCode.SERVER_BUSY, second.expect(MessageType.ERROR).code());
            second.expectClosedByServer();
        }
    }

    @Test
    void silentConnectionIsClosedAfterTheRegisterTimeout() throws Exception {
        int port = start("--register-timeout=200ms");
        try (TestClient silent = TestClient.connect(port)) {
            silent.expectClosedByServer();   // never sent REGISTER
        }
    }

    // ------------------------------------------------------------------ isolation and lifecycle

    @Test
    void clientThatStopsReadingIsDroppedWithoutBlockingOthers() throws Exception {
        int port = start("--outbound-buffer=4");
        try (TestClient alice = TestClient.registered(port, "alice");
             TestClient bob = TestClient.registered(port, "bob");
             Socket slow = new Socket("127.0.0.1", port)) {

            // The slow client registers, then floods PINGs and never reads a single PONG.
            DataOutputStream slowOut = new DataOutputStream(slow.getOutputStream());
            slowOut.write(framed("{\"type\":\"REGISTER\",\"client_id\":\"slow\"}"));
            byte[] pings = repeat(framed("{\"type\":\"PING\"}"), 5_000);
            CompletableFuture<Boolean> flood = CompletableFuture.supplyAsync(() -> {
                try {
                    while (true) {
                        slowOut.write(pings);   // eventually fails once the server drops us
                    }
                } catch (IOException dropped) {
                    return true;
                }
            });

            // Meanwhile, unrelated clients carry on as normal.
            alice.send(Message.send("m1", "bob", "unaffected"));
            assertTrue(alice.expect(MessageType.SEND_RESULT).accepted());
            assertEquals("m1", bob.expect(MessageType.DELIVER).id());

            assertTrue(flood.get(20, TimeUnit.SECONDS), "slow client was never disconnected");
        }
    }

    @Test
    void shutdownNotifiesClientsAndFinishesWithinTheGracePeriod() throws Exception {
        int port = start("--shutdown-grace=2s");
        try (TestClient alice = TestClient.registered(port, "alice")) {
            long started = System.nanoTime();
            server.shutdown();
            Duration took = Duration.ofNanos(System.nanoTime() - started);

            assertEquals(ErrorCode.SHUTTING_DOWN, alice.expect(MessageType.ERROR).code());
            alice.expectClosedByServer();
            assertTrue(took.compareTo(Duration.ofSeconds(2)) < 0, "shutdown took " + took);
        }
    }

    // ------------------------------------------------------------------ helpers

    private static byte[] framed(String json) {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(4 + body.length).putInt(body.length).put(body).array();
    }

    private static byte[] repeat(byte[] chunk, int times) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(chunk.length * times);
        for (int i = 0; i < times; i++) {
            out.writeBytes(chunk);
        }
        return out.toByteArray();
    }
}
