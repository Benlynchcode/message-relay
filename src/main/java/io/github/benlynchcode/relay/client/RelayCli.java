package io.github.benlynchcode.relay.client;

import io.github.benlynchcode.relay.protocol.FrameCodec;
import io.github.benlynchcode.relay.protocol.Message;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A small interactive client for demos. Type commands; server frames are printed as they arrive.
 *
 *   send <to> <text...>   send a message
 *   ack                   acknowledge the last message delivered to you
 *   quit                  disconnect (unacknowledged messages will be redelivered next time)
 */
public final class RelayCli {

    // [Q1: Idle timeout] A third of the server's 60s idle timeout, so one or two lost PINGs don't get us dropped.
    private static final long HEARTBEAT_SECONDS = 20;

    private final DataInputStream in;
    private final DataOutputStream out;
    private final FrameCodec codec = new FrameCodec(1 << 20);   // generous: we trust the server
    private final Object writeLock = new Object();             // main thread and heartbeat both write
    private volatile Message lastDelivered;
    private volatile boolean quitting;

    private RelayCli(Socket socket) throws IOException {
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: client <id> [--host=localhost] [--port=9000]");
            System.exit(2);
        }
        String id = args[0];
        String host = "localhost";
        int port = 9000;
        for (int i = 1; i < args.length; i++) {
            if (args[i].startsWith("--host=")) {
                host = args[i].substring("--host=".length());
            } else if (args[i].startsWith("--port=")) {
                port = Integer.parseInt(args[i].substring("--port=".length()));
            }
        }

        try (Socket socket = new Socket(host, port)) {
            new RelayCli(socket).run(id);
        }
    }

    private void run(String id) throws IOException {
        Thread.ofVirtual().start(this::printIncoming);
        write(Message.register(id));

        ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor(
                r -> Thread.ofPlatform().daemon().unstarted(r));
        heartbeat.scheduleAtFixedRate(this::ping, HEARTBEAT_SECONDS, HEARTBEAT_SECONDS, TimeUnit.SECONDS);

        System.out.println("commands: send <to> <text...> | ack | quit");
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = stdin.readLine()) != null) {
            String[] parts = line.trim().split("\\s+", 3);
            switch (parts[0]) {
                case "send" -> {
                    if (parts.length < 3) {
                        System.out.println("usage: send <to> <text...>");
                    } else {
                        // Unique per sender, so a retry with the same id is recognised as a duplicate.
                        String messageId = UUID.randomUUID().toString().substring(0, 8);
                        write(Message.send(messageId, parts[1], parts[2]));
                    }
                }
                case "ack" -> {
                    Message delivered = lastDelivered;
                    if (delivered == null) {
                        System.out.println("nothing to acknowledge");
                    } else {
                        write(Message.ack(delivered.from(), delivered.id()));
                        lastDelivered = null;
                        System.out.println("acked " + delivered.id());
                    }
                }
                case "quit" -> {
                    quitting = true;
                    heartbeat.shutdownNow();
                    return;
                }
                case "" -> { }
                default -> System.out.println("unknown command: " + parts[0]);
            }
        }
        heartbeat.shutdownNow();
    }

    private void printIncoming() {
        try {
            while (true) {
                Message m = codec.decode(codec.readFrame(in));
                switch (m.type()) {
                    case REGISTERED -> System.out.printf("registered as %s (%d waiting)%n", m.clientId(), m.pending());
                    case SEND_RESULT -> System.out.printf("send %s: %s%s%n", m.id(),
                            m.accepted() ? "accepted" : "rejected", m.code() == null ? "" : " (" + m.code() + ")");
                    case DELIVER -> {
                        lastDelivered = m;
                        System.out.printf("[%s] %s   (id %s; type 'ack')%n", m.from(), m.body(), m.id());
                    }
                    case PONG -> { }
                    case ERROR -> System.out.println("error: " + m.code());
                    default -> System.out.println("unexpected: " + m);
                }
            }
        } catch (Exception e) {
            if (!quitting) {
                System.out.println("disconnected from server");
                System.exit(0);
            }
        }
    }

    private void ping() {
        try {
            write(Message.ping());
        } catch (IOException e) {
            // the reader thread will notice the disconnect
        }
    }

    private void write(Message message) throws IOException {
        synchronized (writeLock) {
            codec.writeFrame(out, codec.encode(message));
        }
    }
}
