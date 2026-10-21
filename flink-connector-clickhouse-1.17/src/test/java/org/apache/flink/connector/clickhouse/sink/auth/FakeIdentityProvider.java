package org.apache.flink.connector.clickhouse.sink.auth;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stand-in for an identity provider's OAuth2 token endpoint (client-credentials grant). Issues
 * RS256 JWTs with a kid header and the claims ClickHouse Cloud requires.
 */
final class FakeIdentityProvider implements AutoCloseable {

    private static final Base64.Encoder B64URL = Base64.getUrlEncoder().withoutPadding();

    private final HttpServer server;
    private final PrivateKey key;
    private final String keyId;
    private final String issuer;
    private final String audience;
    private final String subject;
    private final List<String> grants;
    private final String clientId;
    private final String clientSecret;
    private final long ttlSeconds;
    private final AtomicInteger issued = new AtomicInteger();

    FakeIdentityProvider(PrivateKey key, String keyId, String issuer, String audience, String subject,
                         List<String> grants, String clientId, String clientSecret, long ttlSeconds) throws IOException {
        this.key = key;
        this.keyId = keyId;
        this.issuer = issuer;
        this.audience = audience;
        this.subject = subject;
        this.grants = grants;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.ttlSeconds = ttlSeconds;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/oauth2/token", this::handle);
        server.start();
    }

    String tokenEndpoint() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/oauth2/token";
    }

    int issued() {
        return issued.get();
    }

    private void handle(HttpExchange exchange) throws IOException {
        Map<String, String> form = parseForm(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
        String body;
        int status;
        if (!"client_credentials".equals(form.get("grant_type"))
                || !clientId.equals(form.get("client_id")) || !clientSecret.equals(form.get("client_secret"))) {
            status = 401;
            body = "{\"error\":\"invalid_client\"}";
        } else {
            try {
                status = 200;
                body = "{\"access_token\":\"" + sign() + "\",\"token_type\":\"Bearer\",\"expires_in\":" + ttlSeconds + "}";
                issued.incrementAndGet();
            } catch (GeneralSecurityException e) {
                status = 500;
                body = "{\"error\":\"server_error\"}";
            }
        }
        byte[] out = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, out.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(out);
        }
    }

    private String sign() throws GeneralSecurityException {
        long now = System.currentTimeMillis() / 1000;
        String header = "{\"alg\":\"RS256\",\"kid\":\"" + escape(keyId) + "\",\"typ\":\"JWT\"}";
        StringBuilder claims = new StringBuilder()
                .append("{\"iss\":\"").append(escape(issuer))
                .append("\",\"sub\":\"").append(escape(subject))
                .append("\",\"aud\":\"").append(escape(audience))
                .append("\",\"iat\":").append(now)
                .append(",\"exp\":").append(now + ttlSeconds);
        if (!grants.isEmpty()) {
            claims.append(",\"clickhouse:grants\":[");
            for (int i = 0; i < grants.size(); i++) {
                claims.append(i == 0 ? "" : ",").append('"').append(escape(grants.get(i))).append('"');
            }
            claims.append(']');
        }
        claims.append('}');
        String input = B64URL.encodeToString(header.getBytes(StandardCharsets.UTF_8)) + "."
                + B64URL.encodeToString(claims.toString().getBytes(StandardCharsets.UTF_8));
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(key);
        signature.update(input.getBytes(StandardCharsets.US_ASCII));
        return input + "." + B64URL.encodeToString(signature.sign());
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static Map<String, String> parseForm(String body) {
        Map<String, String> form = new HashMap<>();
        for (String pair : body.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                form.put(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        return form;
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
