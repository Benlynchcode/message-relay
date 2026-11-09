package io.github.benlynchcode.relay.server;

import io.github.benlynchcode.relay.protocol.ErrorCode;
import io.github.benlynchcode.relay.protocol.FrameCodec;
import io.github.benlynchcode.relay.protocol.Message;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Listens, accepts, and shuts down predictably.
 *
 * Threads:
 *   1 accept thread (platform)            blocks in accept()
 *   2 virtual threads per connection      reader blocks on the socket, writer on its queue
 *   1 watchdog (platform, daemon)         closes connections whose write has been stuck too long
 *
 * Virtual threads make thread-per-connection cheap: a blocked virtual thread doesn't hold an OS thread.
 */
public final class RelayServer {

    private static final System.Logger LOG = System.getLogger(RelayServer.class.getName());

    private final ServerConfig config;
    private final FrameCodec codec;
    private final Registry registry;
    private final Semaphore connectionPermits;
    private final Set<ClientConnection> connections = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private final AtomicLong nextConnectionId = new AtomicLong(1);

    private final ExecutorService connectionThreads = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(
            r -> Thread.ofPlatform().name("relay-watchdog").daemon().unstarted(r));

    private volatile ServerSocket serverSocket;
    private volatile Thread acceptThread;

    public RelayServer(ServerConfig config) {
        this.config = config;
        this.codec = new FrameCodec(config.maxFrame());
        this.registry = new Registry(config.mailboxSize(), config.dedupeWindow());
        this.connectionPermits = new Semaphore(config.maxConnections());
    }

    public void start() throws IOException {
        ServerSocket socket = new ServerSocket();
        socket.setReuseAddress(true);   // lets a restarted server rebind the port straight away
        socket.bind(new InetSocketAddress(config.host(), config.port()));
        serverSocket = socket;

        acceptThread = Thread.ofPlatform().name("relay-accept").start(this::acceptLoop);

        long periodMs = Math.max(50, config.writeTimeout().toMillis() / 2);
        watchdog.scheduleAtFixedRate(this::closeStuckWriters, periodMs, periodMs, TimeUnit.MILLISECONDS);

        LOG.log(System.Logger.Level.INFO, "listening on {0}:{1,number,#}", config.host(), port());
    }

    /** The bound port: differs from the config when port 0 asked the OS to pick one (as tests do). */
    public int port() {
        return serverSocket.getLocalPort();
    }

    // ------------------------------------------------------------------ accepting

    private void acceptLoop() {
        while (true) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                // Closing the ServerSocket is how shutdown stops this loop.
                if (!shuttingDown.get()) {
                    LOG.log(System.Logger.Level.ERROR, "accept failed", e);
                }
                return;
            }
            handleNewSocket(socket);
        }
    }

    private void handleNewSocket(Socket socket) {
        if (!connectionPermits.tryAcquire()) {
            rejectBusy(socket);
            return;
        }
        ClientConnection connection;
        try {
            connection = new ClientConnection("conn-" + nextConnectionId.getAndIncrement(), socket,
                    config, codec, registry, shuttingDown::get, this::onClosed);
        } catch (IOException e) {
            connectionPermits.release();
            closeQuietly(socket);
            return;
        }

        // [Q6: Shutdown] Add first, THEN check the flag. Shutdown sets the flag, then walks the set, so one side always catches a late arrival.
        connections.add(connection);
        if (shuttingDown.get()) {
            connection.closeNow("server shutting down");
            return;
        }
        try {
            connectionThreads.execute(connection::runReader);
            connectionThreads.execute(connection::runWriter);
        } catch (RejectedExecutionException e) {
            connection.closeNow("server shutting down");
        }
    }

    /**
     * Over the connection cap. TCP can't refuse with a reason, so accept, say why, and close.
     * Written directly on the accept thread: one tiny frame fits in the socket's send buffer, so it can't block.
     */
    private void rejectBusy(Socket socket) {
        try (socket) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            codec.writeFrame(out, codec.encode(Message.error(ErrorCode.SERVER_BUSY)));
        } catch (IOException ignored) {
            // client already gone
        }
    }

    /** Called exactly once per connection, from ClientConnection.closeNow. */
    private void onClosed(ClientConnection connection) {
        if (connections.remove(connection)) {
            connectionPermits.release();
        }
    }

    // [Q8: Stuck writer] Blocking socket writes have no timeout in Java, so a watchdog closes sockets whose write has hung.
    private void closeStuckWriters() {
        long now = System.nanoTime();
        long timeout = config.writeTimeout().toNanos();
        for (ClientConnection connection : connections) {
            if (connection.isWriteStuck(now, timeout)) {
                connection.closeNow("write timed out");
            }
        }
    }

    // ------------------------------------------------------------------ shutdown

    /**
     * Stops the server. Safe to call more than once. Returns once every connection thread has
     * finished, or after the grace period plus a short force-close.
     */
    public void shutdown() {
        if (!shuttingDown.compareAndSet(false, true)) {
            return;
        }
        LOG.log(System.Logger.Level.INFO, "shutting down ({0} connections)", connections.size());

        // [Q6: Shutdown] 1. Stop new clients: closing the ServerSocket makes accept() throw.
        closeQuietly(serverSocket);

        // [Q6: Shutdown] 2+3. Tell every client why, then close: each writer flushes SHUTTING_DOWN before closing its socket.
        for (ClientConnection connection : connections) {
            connection.shutdownNotice();
        }

        // [Q6: Shutdown] 4. Wait, but only for the grace period; then force anything left.
        connectionThreads.shutdown();
        try {
            if (!connectionThreads.awaitTermination(config.shutdownGrace().toMillis(), TimeUnit.MILLISECONDS)) {
                LOG.log(System.Logger.Level.WARNING, "grace period expired; forcing remaining connections closed");
                for (ClientConnection connection : connections) {
                    connection.closeNow("shutdown grace expired");
                }
                connectionThreads.shutdownNow();
                connectionThreads.awaitTermination(1, TimeUnit.SECONDS);
            }
            acceptThread.join(TimeUnit.SECONDS.toMillis(1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            watchdog.shutdownNow();
        }
        // In-memory only: anything still queued or unacknowledged is lost here (documented limitation).
        LOG.log(System.Logger.Level.INFO, "stopped");
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
            // nothing useful to do
        }
    }
}
