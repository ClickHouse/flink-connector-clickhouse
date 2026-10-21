package org.apache.flink.connector.clickhouse.token;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OAuthClientCredentialsSourceTest {

    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochSecond(1_000_000L), ZoneOffset.UTC);
    private static final String SECRET = "s&c+r=t é";
    private StubHttpServer stub;

    @BeforeEach
    void start() throws IOException {
        stub = new StubHttpServer();
    }

    @AfterEach
    void stop() {
        stub.close();
    }

    private OAuthClientCredentialsSource source(String scope, String audience) {
        return source(scope, audience, Duration.ofSeconds(30));
    }

    private OAuthClientCredentialsSource source(String scope, String audience, Duration requestTimeout) {
        return new OAuthClientCredentialsSource(stub.uri("/oauth2/token"), "flink", SECRET, scope, audience,
                requestTimeout, Duration.ZERO, HttpClient.newHttpClient(), CLOCK);
    }

    private OAuthClientCredentialsSource sourceAt(URI endpoint, Duration requestTimeout, Duration initialBackoff) {
        return new OAuthClientCredentialsSource(endpoint, "flink", SECRET, null, null,
                requestTimeout, initialBackoff, HttpClient.newHttpClient(), CLOCK);
    }

    private static String b64url(String s) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String unsignedJwt(String headerJson, String claimsJson) {
        return b64url(headerJson) + "." + b64url(claimsJson) + ".c2ln";
    }

    @Test
    void usesExpiresInAndSendsClientCredentialsForm() throws Exception {
        stub.respond(200, "{\"access_token\":\"eyJ.payload.sig\",\"expires_in\":3600,\"token_type\":\"Bearer\"}");
        ClickHouseCredentials c = source("api://ch/.default", "my-service").obtain();

        assertEquals("eyJ.payload.sig", c.getJwt());
        assertEquals((1_000_000L + 3600L) * 1000L, c.getValidUntilMillis());
        assertEquals("/oauth2/token", stub.lastPath);
        assertEquals("application/x-www-form-urlencoded", stub.lastContentType);
        assertEquals("grant_type=client_credentials&client_id=flink&client_secret=s%26c%2Br%3Dt+%C3%A9"
                + "&scope=api%3A%2F%2Fch%2F.default&audience=my-service", stub.lastBody);
    }

    @Test
    void acceptsExpiresInAsString() throws Exception {
        stub.respond(200, "{\"access_token\":\"eyJ.payload.sig\",\"expires_in\":\"60\"}");
        assertEquals((1_000_000L + 60L) * 1000L, source(null, null).obtain().getValidUntilMillis());
        assertEquals("grant_type=client_credentials&client_id=flink&client_secret=s%26c%2Br%3Dt+%C3%A9", stub.lastBody);
    }

    @Test
    void fallsBackToJwtExpClaim() throws Exception {
        String jwt = unsignedJwt("{\"alg\":\"RS256\"}", "{\"exp\":1700000000,\"sub\":\"x\"}");
        stub.respond(200, "{\"access_token\":\"" + jwt + "\"}");
        assertEquals(1700000000000L, source(null, null).obtain().getValidUntilMillis());
    }

    @Test
    void non200ResponseFailsWithoutLeakingTheSecret() {
        stub.respond(401, "{\"error\":\"invalid_client\"}");
        IOException e = assertThrows(IOException.class, () -> source(null, null).obtain());
        assertTrue(e.getMessage().contains("HTTP 401"), e.getMessage());
        assertFalse(e.getMessage().contains(SECRET), e.getMessage());
    }

    @Test
    void errorMessagesDoNotLeakTheEndpointQueryString() {
        stub.respond(401, "{\"error\":\"invalid_client\"}");
        OAuthClientCredentialsSource source = new OAuthClientCredentialsSource(stub.uri("/oauth2/token?api_key=K3y"),
                "flink", SECRET, null, null, Duration.ofSeconds(30), Duration.ZERO, HttpClient.newHttpClient(), CLOCK);
        IOException e = assertThrows(IOException.class, source::obtain);
        assertTrue(e.getMessage().contains("/oauth2/token"), e.getMessage());
        assertFalse(e.getMessage().contains("K3y"), e.getMessage());
    }

    private static final String TOKEN_RESPONSE = "{\"access_token\":\"eyJ.payload.sig\",\"expires_in\":3600}";

    @Test
    void transientServerErrorsAreRetriedWithinOneObtain() throws Exception {
        stub.enqueue(503, "{\"error\":\"temporarily_unavailable\"}");
        stub.enqueue(502, "<html>bad gateway</html>");
        stub.respond(200, TOKEN_RESPONSE);
        assertEquals("eyJ.payload.sig", source(null, null).obtain().getJwt());
        assertEquals(3, stub.requests.get());
    }

    @Test
    void rateLimitingIsRetried() throws Exception {
        stub.enqueue(429, "{\"error\":\"slow_down\"}");
        stub.respond(200, TOKEN_RESPONSE);
        assertEquals("eyJ.payload.sig", source(null, null).obtain().getJwt());
        assertEquals(2, stub.requests.get());
    }

    @Test
    void clientErrorsAreNotRetried() {
        stub.respond(401, "{\"error\":\"invalid_client\"}");
        IOException e = assertThrows(IOException.class, () -> source(null, null).obtain());
        assertTrue(e.getMessage().contains("HTTP 401"), e.getMessage());
        assertEquals(1, stub.requests.get(), "a wrong client secret does not fix itself");
    }

    @Test
    void invalidSuccessResponseIsNotRetried() {
        stub.respond(200, "<html><body>proxy login</body></html>");
        assertThrows(IOException.class, () -> source(null, null).obtain());
        assertEquals(1, stub.requests.get());
    }

    @Test
    void givesUpAfterFourAttempts() {
        stub.respond(503, "{\"error\":\"temporarily_unavailable\"}");
        IOException e = assertThrows(IOException.class, () -> source(null, null).obtain());
        assertEquals(OAuthClientCredentialsSource.MAX_ATTEMPTS, stub.requests.get());
        assertEquals(4, OAuthClientCredentialsSource.MAX_ATTEMPTS);
        assertTrue(e.getMessage().contains("HTTP 503"), e.getMessage());
        assertTrue(e.getMessage().contains("after 4 attempts"), e.getMessage());
    }

    @Test
    void connectionFailuresAreRetried() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        IOException e = assertThrows(IOException.class, () ->
                sourceAt(URI.create("http://127.0.0.1:" + closedPort + "/token"), Duration.ofSeconds(5), Duration.ZERO).obtain());
        assertTrue(e.getMessage().contains("after 4 attempts"), e.getMessage());
    }

    @Test
    void backoffDoublesBetweenAttempts() {
        stub.respond(503, "{}");
        long start = System.nanoTime();
        assertThrows(IOException.class, () ->
                sourceAt(stub.uri("/oauth2/token"), Duration.ofSeconds(5), Duration.ofMillis(100)).obtain());
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMillis >= 700, "waits 100 + 200 + 400 ms between the 4 attempts, took " + elapsedMillis);
    }

    @Test
    void requestTimeoutCoversAStalledBody() {
        // Headers arrive, then the body stalls: HttpRequest.timeout alone stops at the headers.
        stub.respond(200, "{\"access_token\":\"eyJ.payload.sig\",\"expires_in\":3600}");
        stub.stallBody(60_000);
        long start = System.nanoTime();
        IOException e = assertThrows(IOException.class, () -> source(null, null, Duration.ofMillis(500)).obtain());
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(e.getMessage().contains("timed out after"), e.getMessage());
        assertTrue(e.getMessage().contains("/oauth2/token"), e.getMessage());
        assertTrue(elapsedMillis < 10_000, "took " + elapsedMillis + " ms");
    }

    @Test
    void requestTimeoutCoversSlowHeaders() {
        stub.respond(200, "{\"access_token\":\"eyJ.payload.sig\",\"expires_in\":3600}");
        stub.delayHeaders(60_000);
        long start = System.nanoTime();
        IOException e = assertThrows(IOException.class, () -> source(null, null, Duration.ofMillis(500)).obtain());
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(e.getMessage().contains("timed out"), e.getMessage());
        assertTrue(elapsedMillis < 10_000, "took " + elapsedMillis + " ms");
    }

    @Test
    void errorBodyEchoingTheSecretIsRedactedAndTruncated() {
        StringBuilder huge = new StringBuilder("{\"error\":\"invalid_request\",\"echo\":\"client_secret=" + SECRET + "\",\"page\":\"");
        for (int i = 0; i < 5_000; i++) {
            huge.append('x');
        }
        stub.respond(400, huge.append("\"}").toString());
        IOException e = assertThrows(IOException.class, () -> source(null, null).obtain());
        assertFalse(e.getMessage().contains(SECRET), e.getMessage());
        assertTrue(e.getMessage().contains("****"), e.getMessage());
        assertTrue(e.getMessage().length() < 1_000, "message length " + e.getMessage().length());
    }

    @Test
    void zeroLifetimeTokenIsRefusedSoFlinkDoesNotLoop() {
        stub.respond(200, "{\"access_token\":\"eyJ.payload.sig\",\"expires_in\":0}");
        IOException e = assertThrows(IOException.class, () -> source(null, null).obtain());
        assertTrue(e.getMessage().contains("expires in"), e.getMessage());
    }

    @Test
    void nearlyExpiredTokenIsRefused() {
        stub.respond(200, "{\"access_token\":\"eyJ.payload.sig\",\"expires_in\":5}");
        assertThrows(IOException.class, () -> source(null, null).obtain());
    }

    @Test
    void alreadyExpiredJwtExpIsRefused() {
        String jwt = unsignedJwt("{\"alg\":\"RS256\"}", "{\"exp\":999990,\"sub\":\"x\"}"); // clock is at 1_000_000 s
        stub.respond(200, "{\"access_token\":\"" + jwt + "\"}");
        IOException e = assertThrows(IOException.class, () -> source(null, null).obtain());
        assertTrue(e.getMessage().contains("expires in"), e.getMessage());
    }

    @Test
    void nonJsonSuccessBodyFails() {
        stub.respond(200, "<html><body>proxy login</body></html>");
        IOException e = assertThrows(IOException.class, () -> source(null, null).obtain());
        assertTrue(e.getMessage().contains("not a JSON object"), e.getMessage());
    }

    @Test
    void deeplyNestedResponseFailsAsIOExceptionNotError() {
        StringBuilder deep = new StringBuilder();
        for (int i = 0; i < 100_000; i++) {
            deep.append('[');
        }
        stub.respond(200, deep.toString());
        // Must surface as IOException (Flink retries), never StackOverflowError (kills the renewal task).
        assertThrows(IOException.class, () -> source(null, null).obtain());
    }

    @Test
    void nonStringAccessTokenFails() {
        stub.respond(200, "{\"access_token\":12345,\"expires_in\":3600}");
        IOException e = assertThrows(IOException.class, () -> source(null, null).obtain());
        assertTrue(e.getMessage().contains("access_token"), e.getMessage());
    }

    @Test
    void outOfRangeExpiresInIsRejectedClearly() {
        stub.respond(200, "{\"access_token\":\"eyJ.payload.sig\",\"expires_in\":1e300}");
        IOException e = assertThrows(IOException.class, () -> source(null, null).obtain());
        assertTrue(e.getMessage().contains("expires_in out of range"), e.getMessage());
    }

    @Test
    void expiresInAsDecimalStringIsAccepted() throws Exception {
        stub.respond(200, "{\"access_token\":\"eyJ.payload.sig\",\"expires_in\":\"3599.0\"}");
        assertEquals((1_000_000L + 3599L) * 1000L, source(null, null).obtain().getValidUntilMillis());
    }

    @Test
    void oversizedResponseIsRejected() {
        StringBuilder big = new StringBuilder("{\"access_token\":\"eyJ.payload.sig\",\"expires_in\":3600,\"pad\":\"");
        for (int i = 0; i < 2_000_000; i++) {
            big.append('x');
        }
        stub.respond(200, big.append("\"}").toString());
        IOException e = assertThrows(IOException.class, () -> source(null, null).obtain());
        assertTrue(e.getMessage().contains("larger than"), e.getMessage());
    }

    @Test
    void missingAccessTokenFails() {
        stub.respond(200, "{\"expires_in\":3600}");
        assertThrows(IOException.class, () -> source(null, null).obtain());
    }

    @Test
    void opaqueTokenWithoutExpiresInFails() {
        stub.respond(200, "{\"access_token\":\"opaque\"}");
        IOException e = assertThrows(IOException.class, () -> source(null, null).obtain());
        assertTrue(e.getMessage().contains("expires_in"), e.getMessage());
    }

    @Test
    void cloudCompatibleTokenHasNoProblems() {
        String jwt = unsignedJwt("{\"alg\":\"RS256\",\"kid\":\"k1\",\"typ\":\"JWT\"}",
                "{\"iss\":\"https://idp\",\"sub\":\"app\",\"aud\":\"svc\",\"iat\":1,\"exp\":2}");
        assertTrue(OAuthClientCredentialsSource.cloudCompatibilityProblems(jwt).isEmpty());
    }

    @Test
    void flagsMissingClaimsAndArrayAudience() {
        String jwt = unsignedJwt("{\"alg\":\"RS256\",\"kid\":\"k1\"}",
                "{\"iss\":\"https://idp\",\"aud\":[\"svc\",\"other\"],\"exp\":2}");
        List<String> problems = OAuthClientCredentialsSource.cloudCompatibilityProblems(jwt);
        assertTrue(problems.toString().contains("missing required claim sub"), problems.toString());
        assertTrue(problems.toString().contains("missing required claim iat"), problems.toString());
        assertTrue(problems.toString().contains("aud must be a single string"), problems.toString());
    }

    @Test
    void flagsMissingKidAndUnsupportedAlg() {
        String jwt = unsignedJwt("{\"alg\":\"HS256\"}",
                "{\"iss\":\"https://idp\",\"sub\":\"app\",\"aud\":\"svc\",\"iat\":1,\"exp\":2}");
        List<String> problems = OAuthClientCredentialsSource.cloudCompatibilityProblems(jwt);
        assertTrue(problems.toString().contains("kid"), problems.toString());
        assertTrue(problems.toString().contains("HS256"), problems.toString());
        assertEquals(1, OAuthClientCredentialsSource.cloudCompatibilityProblems("opaque").size());
    }
}
