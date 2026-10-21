package org.apache.flink.connector.clickhouse.token;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * OAuth2 client-credentials grant against the customer's identity provider. Called on the
 * JobManager only, each time Flink asks the provider for a new token.
 *
 * <p>JSON is parsed with the Jackson that Flink ships shaded in {@code lib/}.
 */
final class OAuthClientCredentialsSource {

    private static final Logger LOG = LoggerFactory.getLogger(OAuthClientCredentialsSource.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<String> CLOUD_ALGORITHMS = Arrays.asList("RS256", "ES256", "ES384", "ES512");
    private static final List<String> CLOUD_REQUIRED_CLAIMS = Arrays.asList("exp", "iat", "iss", "sub", "aud");
    static final long MIN_LIFETIME_MILLIS = 10_000;
    static final int MAX_RESPONSE_BYTES = 1024 * 1024;
    private static final int MAX_ERROR_BODY_CHARS = 500;
    /** Attempts per obtain() for transient failures (I/O errors, timeouts, HTTP 5xx and 429). */
    static final int MAX_ATTEMPTS = 4;

    private final URI tokenEndpoint;
    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private final String audience;
    private final Duration requestTimeout;
    private final Duration initialBackoff;
    private final HttpClient http;
    private final Clock clock;

    OAuthClientCredentialsSource(URI tokenEndpoint, String clientId, String clientSecret, String scope,
                                String audience, Duration requestTimeout, Duration initialBackoff, HttpClient http,
                                Clock clock) {
        this.tokenEndpoint = tokenEndpoint;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.audience = audience;
        this.requestTimeout = requestTimeout;
        this.initialBackoff = initialBackoff;
        this.http = http;
        this.clock = clock;
    }

    /**
     * One token request, retried in place for transient failures (I/O errors, timeouts, HTTP 5xx
     * and 429) with doubling backoff, the way the AWS SDK and Hadoop IPC retry for the other
     * delegation token providers. Client errors and invalid responses are not retried.
     */
    ClickHouseCredentials obtain() throws IOException, InterruptedException {
        Duration backoff = initialBackoff;
        for (int attempt = 1; ; attempt++) {
            try {
                return obtainOnce();
            } catch (TransientFailure e) {
                if (attempt >= MAX_ATTEMPTS) {
                    throw new IOException(e.getMessage() + " (gave up after " + MAX_ATTEMPTS + " attempts)", e);
                }
                LOG.info("OAuth token request attempt {}/{} failed, retrying in {}: {}", attempt, MAX_ATTEMPTS,
                        backoff, e.getMessage());
                Thread.sleep(backoff.toMillis());
                backoff = backoff.multipliedBy(2);
            }
        }
    }

    private ClickHouseCredentials obtainOnce() throws IOException, InterruptedException {
        StringBuilder form = new StringBuilder("grant_type=client_credentials");
        appendParam(form, "client_id", clientId);
        appendParam(form, "client_secret", clientSecret);
        if (scope != null) {
            appendParam(form, "scope", scope);
        }
        if (audience != null) {
            appendParam(form, "audience", audience);
        }
        HttpRequest request = HttpRequest.newBuilder(tokenEndpoint)
                .timeout(requestTimeout)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(form.toString(), StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response = send(request);
        String body = response.body();
        if (response.statusCode() != 200) {
            String message = "OAuth token request to " + ProviderConfig.displayable(tokenEndpoint)
                    + " failed with HTTP " + response.statusCode() + ": " + sanitize(body);
            boolean transientStatus = response.statusCode() >= 500 || response.statusCode() == 429;
            throw transientStatus ? new TransientFailure(message, null) : new IOException(message);
        }
        JsonNode json = parseObject(body);
        if (json == null) {
            throw new IOException("OAuth token response is not a JSON object");
        }
        JsonNode token = json.get("access_token");
        if (token == null || !token.isTextual() || token.asText().isEmpty()) {
            throw new IOException("OAuth token response has no access_token");
        }
        String jwt = token.asText();
        long validUntil = validUntilMillis(json.get("expires_in"), jwt);
        long lifetimeMillis = validUntil - clock.millis();
        if (lifetimeMillis < MIN_LIFETIME_MILLIS) {
            // Flink schedules the next obtain at ratio * remaining lifetime; a non-positive value
            // would make it re-request immediately in a loop. Failing makes Flink use its backoff.
            throw new IOException("Identity provider returned a token that expires in " + (lifetimeMillis / 1000)
                    + "s (minimum " + (MIN_LIFETIME_MILLIS / 1000) + "s); check expires_in/exp and the JobManager clock");
        }
        return new ClickHouseCredentials(jwt, validUntil);
    }

    /**
     * Sends with {@link #requestTimeout} as a deadline for the whole exchange, body included:
     * {@link HttpRequest#timeout} alone stops counting once the headers arrive, so an identity
     * provider that stalls mid-body would block Flink's token renewal thread forever.
     */
    private HttpResponse<String> send(HttpRequest request) throws IOException, InterruptedException {
        BoundedBodySubscriber subscriber = new BoundedBodySubscriber();
        CompletableFuture<HttpResponse<String>> future = http.sendAsync(request, info -> subscriber);
        try {
            return future.get(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            abort(future, subscriber);
            throw requestTimedOut(null);
        } catch (InterruptedException e) {
            abort(future, subscriber);
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ResponseTooLargeException) {
                throw (IOException) cause;
            }
            if (cause instanceof HttpConnectTimeoutException) {
                throw new TransientFailure("Connecting to " + ProviderConfig.displayable(tokenEndpoint)
                        + " timed out (" + ProviderConfig.PREFIX + "oauth.connect-timeout)", cause);
            }
            if (cause instanceof HttpTimeoutException) {
                // HttpRequest.timeout (same value) fired before the response headers arrived
                throw requestTimedOut(cause);
            }
            if (cause instanceof IOException) {
                // connection refused/reset, TLS handshake, HTTP timeouts: worth another attempt
                throw new TransientFailure("OAuth token request to " + ProviderConfig.displayable(tokenEndpoint)
                        + " failed: " + cause, cause);
            }
            throw new IOException("OAuth token request to " + ProviderConfig.displayable(tokenEndpoint)
                    + " failed: " + cause, cause);
        }
    }

    private TransientFailure requestTimedOut(Throwable cause) {
        return new TransientFailure("OAuth token request to " + ProviderConfig.displayable(tokenEndpoint)
                + " timed out after " + format(requestTimeout) + " (" + ProviderConfig.PREFIX
                + "oauth.request-timeout)", cause);
    }

    /** Before JDK 16, cancelling the future does not stop the exchange; cancelling the body subscription does. */
    private static void abort(CompletableFuture<?> future, BoundedBodySubscriber subscriber) {
        future.cancel(true);
        subscriber.cancel();
    }

    private static String format(Duration d) {
        return d.toMillis() % 1000 == 0 ? (d.toMillis() / 1000) + "s" : d.toMillis() + "ms";
    }

    /** Collects the response body as UTF-8 text, cancelling once it exceeds {@link #MAX_RESPONSE_BYTES}. */
    private final class BoundedBodySubscriber implements HttpResponse.BodySubscriber<String> {
        private final CompletableFuture<String> result = new CompletableFuture<>();
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private volatile Flow.Subscription subscription;
        private volatile boolean cancelled;

        @Override
        public CompletionStage<String> getBody() {
            return result;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            if (cancelled) {
                subscription.cancel();
                return;
            }
            subscription.request(Long.MAX_VALUE);
        }

        /** Called on timeout: stops reading and releases the connection. */
        void cancel() {
            cancelled = true;
            Flow.Subscription s = subscription;
            if (s != null) {
                s.cancel();
            }
            result.cancel(false);
        }

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            if (result.isDone()) {
                return;
            }
            for (ByteBuffer buffer : buffers) {
                if (bytes.size() + buffer.remaining() > MAX_RESPONSE_BYTES) {
                    subscription.cancel();
                    result.completeExceptionally(new ResponseTooLargeException("OAuth token response from "
                            + ProviderConfig.displayable(tokenEndpoint) + " is larger than " + MAX_RESPONSE_BYTES
                            + " bytes"));
                    return;
                }
                byte[] chunk = new byte[buffer.remaining()];
                buffer.get(chunk);
                bytes.write(chunk, 0, chunk.length);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            result.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            result.complete(new String(bytes.toByteArray(), StandardCharsets.UTF_8));
        }
    }

    /** A failure that may succeed on the next attempt. */
    private static final class TransientFailure extends IOException {
        TransientFailure(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static final class ResponseTooLargeException extends IOException {
        ResponseTooLargeException(String message) {
            super(message);
        }
    }

    /** Error bodies go into exception messages and logs: never the client secret, never unbounded. */
    private String sanitize(String body) {
        String text = body.trim();
        if (!clientSecret.isEmpty()) {
            text = text.replace(clientSecret, "****")
                    .replace(URLEncoder.encode(clientSecret, StandardCharsets.UTF_8), "****");
        }
        return text.length() <= MAX_ERROR_BODY_CHARS ? text : text.substring(0, MAX_ERROR_BODY_CHARS) + "…";
    }

    private long validUntilMillis(JsonNode expiresIn, String jwt) throws IOException {
        if (expiresIn == null || expiresIn.isNull()) {
            return expiryFromJwt(jwt);
        }
        BigDecimal seconds;
        if (expiresIn.isNumber()) {
            seconds = expiresIn.decimalValue();
        } else if (expiresIn.isTextual()) {
            try {
                seconds = new BigDecimal(expiresIn.asText().trim());
            } catch (NumberFormatException e) {
                throw new IOException("OAuth token response has a non-numeric expires_in");
            }
        } else {
            throw new IOException("OAuth token response has an expires_in that is not a number");
        }
        try {
            return Math.addExact(clock.millis(), toMillisExact(seconds));
        } catch (ArithmeticException e) {
            throw new IOException("OAuth token response has expires_in out of range: " + expiresIn);
        }
    }

    static long expiryFromJwt(String jwt) throws IOException {
        String[] parts = jwt.split("\\.");
        if (parts.length < 2) {
            throw new IOException("access_token is not a JWT and the response has no expires_in");
        }
        JsonNode claims = decodePart(parts[1]);
        if (claims == null) {
            throw new IOException("Cannot decode the JWT payload to read exp and the response has no expires_in");
        }
        JsonNode exp = claims.get("exp");
        if (exp == null || !exp.isNumber()) {
            throw new IOException("JWT has no numeric exp claim and the response has no expires_in");
        }
        try {
            return toMillisExact(exp.decimalValue());
        } catch (ArithmeticException e) {
            throw new IOException("JWT exp claim out of range: " + exp);
        }
    }

    /**
     * Reasons ClickHouse Cloud would likely reject this token (see the Cloud JWT reference:
     * kid header, RS256/ES*, claims exp/iat/iss/sub/aud with aud a single string). Not a
     * signature check; Cloud remains the authority. Empty list means no problems found.
     */
    static List<String> cloudCompatibilityProblems(String jwt) {
        List<String> problems = new ArrayList<>();
        String[] parts = jwt.split("\\.");
        if (parts.length != 3) {
            problems.add("access_token is not a JWT (expected header.payload.signature)");
            return problems;
        }
        JsonNode header = decodePart(parts[0]);
        JsonNode claims = decodePart(parts[1]);
        if (header == null || claims == null) {
            problems.add("access_token header or payload is not base64url-encoded JSON");
            return problems;
        }
        JsonNode alg = header.get("alg");
        if (alg == null || !alg.isTextual() || !CLOUD_ALGORITHMS.contains(alg.asText())) {
            problems.add("alg " + alg + " is not supported by ClickHouse Cloud (RS256, ES256, ES384, ES512)");
        }
        JsonNode kid = header.get("kid");
        if (kid == null || !kid.isTextual()) {
            problems.add("header has no kid (ClickHouse Cloud selects the JWKS key by kid)");
        }
        for (String claim : CLOUD_REQUIRED_CLAIMS) {
            if (!claims.has(claim)) {
                problems.add("missing required claim " + claim);
            }
        }
        JsonNode aud = claims.get("aud");
        if (aud != null && !aud.isTextual()) {
            problems.add("aud must be a single string, got " + (aud.isArray() ? "an array" : aud.getNodeType()));
        }
        return problems;
    }

    /** Whole seconds (fractions dropped) to milliseconds; throws ArithmeticException if it does not fit a long. */
    private static long toMillisExact(BigDecimal seconds) {
        return seconds.setScale(0, RoundingMode.FLOOR).multiply(BigDecimal.valueOf(1000)).longValueExact();
    }

    /** A base64url JWT segment as a JSON object, or {@code null} if it is not one. */
    private static JsonNode decodePart(String part) {
        try {
            return parseObject(new String(Base64.getUrlDecoder().decode(part), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** The text as a JSON object, or {@code null} if it is not valid JSON or not an object. */
    private static JsonNode parseObject(String text) {
        try {
            JsonNode node = MAPPER.readTree(text);
            return node != null && node.isObject() ? node : null;
        } catch (IOException | StackOverflowError e) {
            // StackOverflowError: older shaded Jackson (Flink 1.17/1.18) has no default nesting limit.
            return null;
        }
    }

    private static void appendParam(StringBuilder form, String key, String value) {
        form.append('&')
                .append(URLEncoder.encode(key, StandardCharsets.UTF_8))
                .append('=')
                .append(URLEncoder.encode(value, StandardCharsets.UTF_8));
    }
}
