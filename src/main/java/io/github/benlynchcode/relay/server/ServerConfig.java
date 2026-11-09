package io.github.benlynchcode.relay.server;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Every limit and timeout in one place. All of them are bounded and configurable, which is
 * how the brief's "mailboxes, message sizes, active connections and buffers are bounded" is met.
 *
 * Set from the command line as --key=value, e.g. {@code --port=9000 --idle-timeout=30s}.
 */
public record ServerConfig(
        String host,
        int port,
        int maxConnections,
        int maxFrame,
        int mailboxSize,
        int outboundBuffer,
        Duration registerTimeout,
        Duration idleTimeout,
        Duration writeTimeout,
        Duration shutdownGrace,
        int dedupeWindow) {

    public ServerConfig {
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("port must be 0-65535");
        }
        requirePositive("max-conns", maxConnections);
        requirePositive("max-frame", maxFrame);
        requirePositive("mailbox-size", mailboxSize);
        // Room for at least a reply and a mailbox wake-up.
        if (outboundBuffer < 2) {
            throw new IllegalArgumentException("outbound-buffer must be >= 2");
        }
        // [Q3: Dedupe vs reject] Checked at startup: a window smaller than the mailbox forgets IDs of messages still queued.
        if (dedupeWindow < mailboxSize) {
            throw new IllegalArgumentException("dedupe-window must be >= mailbox-size");
        }
        requirePositive("register-timeout", registerTimeout);
        requirePositive("idle-timeout", idleTimeout);
        requirePositive("write-timeout", writeTimeout);
        requirePositive("shutdown-grace", shutdownGrace);
    }

    public static ServerConfig defaults() {
        return new ServerConfig(
                "0.0.0.0",              // all interfaces, so it is reachable from outside a container
                9000,
                100,                    // max-conns
                64 * 1024,              // max-frame (bytes)
                1000,                   // mailbox-size (messages, pending + in flight)
                64,                     // outbound-buffer (queued replies per connection)
                Duration.ofSeconds(5),  // register-timeout
                Duration.ofSeconds(60), // idle-timeout
                Duration.ofSeconds(5),  // write-timeout
                Duration.ofSeconds(5),  // shutdown-grace
                1000);                  // dedupe-window
    }

    /** Defaults, overridden by any {@code --key=value} arguments. Unknown keys are an error, not ignored. */
    public static ServerConfig fromArgs(String... args) {
        Map<String, String> values = new HashMap<>();
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (!arg.startsWith("--") || eq < 0) {
                throw new IllegalArgumentException("expected --key=value, got: " + arg);
            }
            values.put(arg.substring(2, eq), arg.substring(eq + 1));
        }

        ServerConfig d = defaults();
        ServerConfig config = new ServerConfig(
                stringArg(values, "host", d.host()),
                intArg(values, "port", d.port()),
                intArg(values, "max-conns", d.maxConnections()),
                intArg(values, "max-frame", d.maxFrame()),
                intArg(values, "mailbox-size", d.mailboxSize()),
                intArg(values, "outbound-buffer", d.outboundBuffer()),
                durationArg(values, "register-timeout", d.registerTimeout()),
                durationArg(values, "idle-timeout", d.idleTimeout()),
                durationArg(values, "write-timeout", d.writeTimeout()),
                durationArg(values, "shutdown-grace", d.shutdownGrace()),
                intArg(values, "dedupe-window", d.dedupeWindow()));
        if (!values.isEmpty()) {
            throw new IllegalArgumentException("unknown option(s): " + values.keySet());
        }
        return config;
    }

    // Each lookup removes its key, so whatever is left over at the end was a typo.
    private static String stringArg(Map<String, String> values, String key, String fallback) {
        String raw = values.remove(key);
        return raw == null ? fallback : raw;
    }

    private static int intArg(Map<String, String> values, String key, int fallback) {
        String raw = values.remove(key);
        if (raw == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + key + " must be a number, got: " + raw);
        }
    }

    /** Accepts "500ms" or "5s". */
    private static Duration durationArg(Map<String, String> values, String key, Duration fallback) {
        String raw = values.remove(key);
        if (raw == null) {
            return fallback;
        }
        try {
            if (raw.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(raw.substring(0, raw.length() - 2)));
            }
            if (raw.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(raw.substring(0, raw.length() - 1)));
            }
        } catch (NumberFormatException e) {
            // fall through to the error below
        }
        throw new IllegalArgumentException("--" + key + " must look like 500ms or 5s, got: " + raw);
    }

    private static void requirePositive(String name, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be > 0");
        }
    }

    private static void requirePositive(String name, Duration value) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be > 0");
        }
    }
}
