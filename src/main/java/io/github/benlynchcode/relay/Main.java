package io.github.benlynchcode.relay;

import io.github.benlynchcode.relay.client.RelayCli;
import io.github.benlynchcode.relay.server.RelayServer;
import io.github.benlynchcode.relay.server.ServerConfig;

import java.util.Arrays;

/**
 * Entry point for the one jar:
 *
 *   java -jar target/relay.jar [server] [--key=value ...]     run the relay (the default)
 *   java -jar target/relay.jar client <id> [--host=..] [--port=..]   interactive demo client
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        // One line per log record: time, level, message.
        System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tT %4$-7s %5$s%6$s%n");

        if (args.length > 0 && args[0].equals("client")) {
            RelayCli.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        String[] options = args.length > 0 && args[0].equals("server")
                ? Arrays.copyOfRange(args, 1, args.length)
                : args;

        ServerConfig config;
        try {
            config = ServerConfig.fromArgs(options);
        } catch (IllegalArgumentException e) {
            System.err.println("relay: " + e.getMessage());
            System.exit(2);
            return;
        }

        RelayServer server = new RelayServer(config);
        server.start();
        // [Q6: Shutdown] Ctrl-C, SIGTERM and `docker stop` all run this hook. The accept thread keeps the JVM alive until then.
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown, "relay-shutdown"));
    }
}
