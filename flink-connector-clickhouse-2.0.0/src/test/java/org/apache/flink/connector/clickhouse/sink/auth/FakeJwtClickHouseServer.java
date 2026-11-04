package org.apache.flink.connector.clickhouse.sink.auth;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Base64;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Stand-in for a ClickHouse Cloud HTTP endpoint with JWT auth: verifies each Bearer token's RS256
 * signature, exp, iss and aud; accepts inserts (counting newline-terminated rows) or answers like
 * ClickHouse does for a rejected token (403 + X-ClickHouse-Exception-Code: 516).
 */
final class FakeJwtClickHouseServer implements AutoCloseable {

    private static final Pattern EXP = Pattern.compile("\"exp\":(\\d+)");

    private final HttpServer server;
    private final ExecutorService executor = Executors.newFixedThreadPool(4);
    private final PublicKey publicKey;
    private final String issuer;
    private final String audience;
    private final AtomicLong rows = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final Set<String> tokens = ConcurrentHashMap.newKeySet();

    FakeJwtClickHouseServer(PublicKey publicKey, String issuer, String audience) throws IOException {
        this.publicKey = publicKey;
        this.issuer = issuer;
        this.audience = audience;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.setExecutor(executor);
        server.start();
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/";
    }

    long rows() {
        return rows.get();
    }

    long rejected() {
        return rejected.get();
    }

    int distinctTokens() {
        return tokens.size();
    }

    private void handle(HttpExchange exchange) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        String auth = exchange.getRequestHeaders().getFirst("Authorization");
        String error = verify(auth);
        if (error != null) {
            rejected.incrementAndGet();
            byte[] message = ("Code: 516. DB::Exception: " + error + ". (AUTHENTICATION_FAILED)").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("X-ClickHouse-Exception-Code", "516");
            exchange.sendResponseHeaders(403, message.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(message);
            }
            return;
        }
        tokens.add(auth.substring("Bearer ".length()));
        long lines = 0;
        for (byte b : body) {
            if (b == '\n') {
                lines++;
            }
        }
        rows.addAndGet(lines);
        exchange.getResponseHeaders().add("X-ClickHouse-Summary", "{\"written_rows\":\"" + lines + "\"}");
        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    private String verify(String auth) {
        if (auth == null || !auth.startsWith("Bearer ")) {
            return "missing Bearer token";
        }
        String[] parts = auth.substring("Bearer ".length()).split("\\.");
        if (parts.length != 3) {
            return "malformed JWT";
        }
        try {
            Signature verifier = Signature.getInstance("SHA256withRSA");
            verifier.initVerify(publicKey);
            verifier.update((parts[0] + "." + parts[1]).getBytes(StandardCharsets.US_ASCII));
            if (!verifier.verify(Base64.getUrlDecoder().decode(parts[2]))) {
                return "bad signature";
            }
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            return "signature check failed";
        }
        String claims = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);
        Matcher exp = EXP.matcher(claims);
        if (!exp.find()) {
            return "no exp claim";
        }
        if (Long.parseLong(exp.group(1)) * 1000L < System.currentTimeMillis()) {
            return "token expired";
        }
        if (!claims.contains("\"iss\":\"" + issuer + "\"") || !claims.contains("\"aud\":\"" + audience + "\"")) {
            return "wrong iss or aud";
        }
        return null;
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }
}
