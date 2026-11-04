# Flink Delegation Token Support (OAuth 2.0 client credentials → ClickHouse Cloud JWT) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the ClickHouse sink authenticate to ClickHouse Cloud with short-lived JWTs that Flink's delegation token framework obtains on the JobManager, via the OAuth2 client-credentials grant against the customer's identity provider, and distributes to TaskManagers.

**Architecture:**
- A new dependency-free `flink-connector-clickhouse-token` module, deployed to Flink's `lib/`, provides a `DelegationTokenProvider` that calls the identity provider's token endpoint, and a `DelegationTokenReceiver`. Both live in package `org.apache.flink.connector.clickhouse.token`.
- The receiver publishes the JWT into a static, Flink-free `ClickHouseCredentialsHolder`.
- The sink's `ClickHouseClientConfig` reads it through a bridge in `-base` before every batch and calls client-v2's `updateBearerToken`.

**Tech Stack:** Java 11 (`--release 11` for the token module), Gradle Kotlin DSL, Flink `flink-core` 1.17.2 delegation token API (`compileOnly`), JDK `java.net.http.HttpClient`, `com.sun.net.httpserver` (tests), `java.security` (tests), ClickHouse client-v2 0.10.0, JUnit 5, Flink MiniCluster.

**Spec:** `docs/superpowers/specs/2026-10-03-delegation-tokens-design.md` (revision 3, OAuth 2.0 client credentials only)

## Global Constraints

- **No git operations.** Never run `git add`, `git commit` or `git push` unless the user explicitly asks for that specific action. Every task ends with a **Checkpoint**: stop, report, and let the user review or commit.
- **client-v2 version:** `0.10.0` (Task 1).
- **Token module dependencies:** nothing at runtime besides `flink-core` (provided by Flink). No client-v2, no Jackson.
- **Token module Java level:** Java 11 bytecode. One jar serves Flink 1.17 through 2.x.
- **Token package:** every token-module class lives in `org.apache.flink.connector.clickhouse.token`. This is required: Flink's default parent-first pattern `org.apache.flink.` makes the `lib/` copy win.
- **Service name:** `clickhouse`. Config prefix: `security.delegation.token.provider.clickhouse.`
- **Provider activation:** `oauth.token-endpoint` absent or blank means inactive, so `delegationTokensRequired()` returns `false` and nothing throws. Set, but with a missing client id or secret, or an invalid URL, means `init()` throws `IllegalConfigurationException`.
- **Shadow JARs:** the sink shadow JARs must **not** contain any `org/apache/flink/connector/clickhouse/token/` class.
- **Secrets in messages:** no exception message or log line may contain a full JWT or the client secret. JWTs are shown via `ClickHouseCredentials.redact` (first 4 characters plus `…`).
- **Retriable auth errors:** `516` and `720` are retriable **only** in delegation-token mode.
- **HTTPS:** `forDelegationToken` rejects `http://` URLs except for loopback hosts ("Always send JWTs over HTTPS", per the ClickHouse Cloud docs).
- **Server:** ClickHouse Cloud Enterprise, service 26.4+.
- **Mirroring:** every change to `-1.17` is mirrored identically in `-2.0.0`.

## Review Focus

1. **The sink config is serialized on the client and deserialized on TaskManagers.** A `forDelegationToken` config must survive Java serialization with its mode intact and no JWT inside. Test in Task 8 (`configSurvivesJavaSerialization`).
2. **The writer opens before the first token arrives**, or the token never arrives. It must wait up to the timeout, then fail with a message naming the three likely causes. Tests in Task 7 (`awaitTimesOutWithDiagnosticMessage`) and Task 8 (`createClientTimesOutWithoutToken`).
3. **A client secret containing `&`, `+`, `=`, spaces or non-ASCII characters** must be form-URL-encoded correctly. Test in Task 4 (`usesExpiresInAndSendsClientCredentialsForm`).
4. **An identity provider returning `expires_in` as a string, or omitting it,** must still give a correct expiry. Tests in Task 4 (`acceptsExpiresInAsString`, `fallsBackToJwtExpClaim`).
5. **Config values pasted with surrounding whitespace** (e.g. a client id with a trailing space) must be trimmed, not sent as-is. Test in Task 5 (`valuesAreTrimmed`).

Cloud setup rules from the [JWT authentication setup](https://clickhouse.com/docs/products/cloud/guides/security/cloud-access-management/jwt-authentication-setup) page, each pinned by a test:
- **An identity-provider token Cloud would reject** (no `kid`, unsupported `alg`, missing `exp`/`iat`/`iss`/`sub`/`aud`, array `aud`) produces a JobManager WARN. Task 4 (`flagsMissingClaimsAndArrayAudience`, `flagsMissingKidAndUnsupportedAlg`).
- **"Always send JWTs over HTTPS".** Task 8 (`forDelegationTokenRequiresHttpsExceptForLoopback`).

---

## File Structure

**New module `flink-connector-clickhouse-token`** (sources under `src/main/java/org/apache/flink/connector/clickhouse/token/`):

| File | Responsibility |
|---|---|
| `build.gradle.kts` | Module build; `compileOnly` flink-core 1.17.2; publication `com.clickhouse.flink:flink-connector-clickhouse-token` |
| `ClickHouseCredentials.java` | Immutable `(jwt, validUntilMillis)` + `redact` |
| `ClickHouseCredentialsCodec.java` | Versioned binary encoding |
| `ClickHouseCredentialsHolder.java` | JVM-wide `static volatile` slot; no Flink types |
| `Json.java` | Minimal JSON parser |
| `OAuthClientCredentialsSource.java` | Client-credentials request, expiry, Cloud-compatibility check |
| `ProviderConfig.java` | Parses and validates the provider's config keys; renewal warning |
| `ClickHouseDelegationTokenProvider.java` / `ClickHouseDelegationTokenReceiver.java` | Flink provider and receiver |
| `src/main/resources/META-INF/services/…DelegationTokenProvider` / `…Receiver` | ServiceLoader registration |

**`flink-connector-clickhouse-base`**: new package `org.apache.flink.connector.clickhouse.auth` (`AuthMode`, `ResolvedCredentials`, `DelegationTokenBridge`), plus `com/clickhouse/utils/Utils.java` (modify).

**`flink-connector-clickhouse-1.17` and `-2.0.0`** (mirrored):
- `ClickHouseClientConfig.java`, `ClickHouseAsyncWriter.java`, `build.gradle.kts` (modify)
- tests: `ClickHouseClientConfigDelegationTokenTests`, `FakeIdentityProvider`, `FakeJwtClickHouseServer`, `ClickHouseDelegationTokenTests`, `ClickHouseDelegationTokenCloudFakeIdpTests`
- 1.17 only: `EmbeddedFlinkClusterForTests.setUp(Configuration)`

---

### Task 1: Upgrade client-v2 to 0.10.0

**Files:**
- Modify: `build.gradle.kts:14`

**Interfaces:**
- Produces: an atomic, thread-safe `Client#updateBearerToken(String)` on every module's classpath.

- [ ] **Step 1: Confirm the current version**

Run: `./gradlew :flink-connector-clickhouse-base:dependencies --configuration compileClasspath | grep client-v2`
Expected: `com.clickhouse:client-v2:0.9.5`

- [ ] **Step 2: Bump it**

In `build.gradle.kts`:

```kotlin
val clickhouseVersion by extra(providers.gradleProperty("clickhouse_client_v2_version").getOrElse("0.10.0"))
```

- [ ] **Step 3: Compile and run the Docker-free tests**

Run: `./gradlew :flink-connector-clickhouse-base:test :flink-connector-clickhouse-1.17:compileTestJava :flink-connector-clickhouse-2.0.0:compileTestJava`
Expected: `BUILD SUCCESSFUL`. If a client-v2 API changed, adapt the call site and list each change in the checkpoint report.

- [ ] **Step 4: Run the full suites (needs Docker)**

Run: `./gradlew :flink-connector-clickhouse-1.17:test :flink-connector-clickhouse-2.0.0:test`
Expected: the same results as on `main`. If Docker isn't available, report that; don't claim success.

- [ ] **Step 5: Checkpoint**

Stop. Report. Do not commit.

---

### Task 2: Token module skeleton, credentials, codec and holder

**Files:**
- Modify: `settings.gradle.kts`
- Create: `flink-connector-clickhouse-token/build.gradle.kts`
- Create: `flink-connector-clickhouse-token/src/main/java/org/apache/flink/connector/clickhouse/token/ClickHouseCredentials.java`
- Create: `flink-connector-clickhouse-token/src/main/java/org/apache/flink/connector/clickhouse/token/ClickHouseCredentialsCodec.java`
- Create: `flink-connector-clickhouse-token/src/main/java/org/apache/flink/connector/clickhouse/token/ClickHouseCredentialsHolder.java`
- Test: `flink-connector-clickhouse-token/src/test/java/org/apache/flink/connector/clickhouse/token/ClickHouseCredentialsCodecTest.java`

**Interfaces:**
- Produces:
  - `ClickHouseCredentials(String jwt, long validUntilMillis)`, with `getJwt()`, `getValidUntilMillis()`, `static redact(String)`
  - `ClickHouseCredentialsCodec.encode/decode`. `decode` throws `IllegalArgumentException` on an unknown version or bad bytes.
  - `ClickHouseCredentialsHolder.get/set/reset` (static)

- [ ] **Step 1: Register the module**

In `settings.gradle.kts`:

```kotlin
include("flink-connector-clickhouse-base", "flink-connector-clickhouse-token", "flink-connector-clickhouse-2.0.0", "flink-connector-clickhouse-1.17", "flink-connector-clickhouse-integration")
```

- [ ] **Step 2: Create `flink-connector-clickhouse-token/build.gradle.kts`**

```kotlin
/*
 * Flink delegation token provider/receiver for the ClickHouse sink (OAuth 2.0 client credentials → ClickHouse Cloud JWT).
 * Deployed to Flink's lib/ directory; depends only on flink-core (provided by Flink).
 */

plugins {
    `maven-publish`
    java
    signing
    id("com.gradleup.nmcp") version "0.0.8"
}

val sinkVersion: String by rootProject.extra

// Oldest Flink with the delegation token API (FLIP-272). The interfaces are identical through 2.x.
val flinkTokenApiVersion = "1.17.2"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.flink:flink-core:$flinkTokenApiVersion")

    testImplementation("org.apache.flink:flink-core:$flinkTokenApiVersion")
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile> {
    options.release.set(11)
}

val sourcesJar by tasks.registering(Jar::class) {
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifact(sourcesJar)
            groupId = "com.clickhouse.flink"
            artifactId = "flink-connector-clickhouse-token"
            version = sinkVersion

            pom {
                name.set("ClickHouse Flink Connector - Delegation Tokens")
                description.set("Flink delegation token provider/receiver for the ClickHouse sink (install in Flink lib/)")
                url.set("https://github.com/ClickHouse/flink-connector-clickhouse")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://github.com/ClickHouse/flink-connector-clickhouse/blob/main/LICENSE")
                    }
                }
                developers {
                    developer {
                        id.set("mzitnik")
                        name.set("Mark Zitnik")
                        email.set("mark@clickhouse.com")
                    }
                }
                scm {
                    connection.set("git@github.com:ClickHouse/flink-connector-clickhouse.git")
                    url.set("https://github.com/ClickHouse/flink-connector-clickhouse")
                }
            }
        }
    }
}

signing {
    val signingKey = System.getenv("SIGNING_KEY")
    val signingPassword = System.getenv("SIGNING_PASSWORD")
    if (signingKey != null && signingPassword != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications["maven"])
    }
}

nmcp {
    publish("maven") {
        username = System.getenv("NMCP_USERNAME")
        password = System.getenv("NMCP_PASSWORD")
        publicationType = "USER_MANAGED"
    }
}
```

- [ ] **Step 3: Write the failing test**

`ClickHouseCredentialsCodecTest.java`:

```java
package org.apache.flink.connector.clickhouse.token;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ClickHouseCredentialsCodecTest {

    @AfterEach
    void resetHolder() {
        ClickHouseCredentialsHolder.reset();
    }

    @Test
    void roundTrip() {
        ClickHouseCredentials in = new ClickHouseCredentials("eyJhbGciOiJSUzI1NiJ9.e30.c2ln", 1791061965000L);
        ClickHouseCredentials out = ClickHouseCredentialsCodec.decode(ClickHouseCredentialsCodec.encode(in));
        assertEquals("eyJhbGciOiJSUzI1NiJ9.e30.c2ln", out.getJwt());
        assertEquals(1791061965000L, out.getValidUntilMillis());
    }

    @Test
    void jwtLargerThan64KiBRoundTrips() {
        StringBuilder big = new StringBuilder();
        for (int i = 0; i < 70_000; i++) {
            big.append('a');
        }
        ClickHouseCredentials in = new ClickHouseCredentials(big.toString(), 1L);
        assertEquals(big.toString(), ClickHouseCredentialsCodec.decode(ClickHouseCredentialsCodec.encode(in)).getJwt());
    }

    @Test
    void unknownVersionIsRejected() {
        byte[] bytes = ClickHouseCredentialsCodec.encode(new ClickHouseCredentials("jwt-value", 1L));
        bytes[0] = 9;
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ClickHouseCredentialsCodec.decode(bytes));
        assertTrue(e.getMessage().contains("version 9"), e.getMessage());
    }

    @Test
    void truncatedBytesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> ClickHouseCredentialsCodec.decode(new byte[] {1, 0, 0}));
        assertThrows(IllegalArgumentException.class, () -> ClickHouseCredentialsCodec.decode(new byte[0]));
    }

    @Test
    void toStringNeverShowsTheFullJwt() {
        ClickHouseCredentials c = new ClickHouseCredentials("eyJhbGciOiJSUzI1NiJ9.e30.c2ln", 1L);
        assertFalse(c.toString().contains("eyJhbGciOiJSUzI1NiJ9.e30.c2ln"));
        assertTrue(c.toString().contains("eyJh…"));
        assertEquals("****", ClickHouseCredentials.redact("abc"));
    }

    @Test
    void emptyJwtIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ClickHouseCredentials("", 1L));
        assertThrows(IllegalArgumentException.class, () -> new ClickHouseCredentials(null, 1L));
    }

    @Test
    void holderPublishesAndResets() {
        assertNull(ClickHouseCredentialsHolder.get());
        ClickHouseCredentials c = new ClickHouseCredentials("jwt-value", 1L);
        ClickHouseCredentialsHolder.set(c);
        assertSame(c, ClickHouseCredentialsHolder.get());
        ClickHouseCredentialsHolder.reset();
        assertNull(ClickHouseCredentialsHolder.get());
    }
}
```

- [ ] **Step 4: Run it to verify it fails**

Run: `./gradlew :flink-connector-clickhouse-token:test --tests 'org.apache.flink.connector.clickhouse.token.ClickHouseCredentialsCodecTest'`
Expected: compilation FAILS with `cannot find symbol: class ClickHouseCredentials`.

- [ ] **Step 5: Implement**

`ClickHouseCredentials.java`:

```java
package org.apache.flink.connector.clickhouse.token;

/** A short-lived ClickHouse Cloud JWT obtained by the delegation token provider. Immutable. */
public final class ClickHouseCredentials {

    private final String jwt;
    private final long validUntilMillis;

    public ClickHouseCredentials(String jwt, long validUntilMillis) {
        if (jwt == null || jwt.isEmpty()) {
            throw new IllegalArgumentException("ClickHouse JWT must not be empty");
        }
        this.jwt = jwt;
        this.validUntilMillis = validUntilMillis;
    }

    public String getJwt() {
        return jwt;
    }

    public long getValidUntilMillis() {
        return validUntilMillis;
    }

    /** Safe-to-log form of a secret: first 4 characters followed by an ellipsis. */
    public static String redact(String secret) {
        if (secret == null || secret.length() <= 4) {
            return "****";
        }
        return secret.substring(0, 4) + "…";
    }

    @Override
    public String toString() {
        return "ClickHouseCredentials{jwt=" + redact(jwt) + ", validUntilMillis=" + validUntilMillis + '}';
    }
}
```

`ClickHouseCredentialsCodec.java`:

```java
package org.apache.flink.connector.clickhouse.token;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Versioned binary form of {@link ClickHouseCredentials} as shipped by Flink from the provider
 * (JobManager) to the receivers (TaskManagers).
 *
 * <pre>
 * byte version (1) | int length + UTF-8 jwt | long validUntilMillis
 * </pre>
 */
public final class ClickHouseCredentialsCodec {

    static final byte VERSION = 1;

    private ClickHouseCredentialsCodec() {}

    public static byte[] encode(ClickHouseCredentials credentials) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(VERSION);
            byte[] jwt = credentials.getJwt().getBytes(StandardCharsets.UTF_8);
            out.writeInt(jwt.length);
            out.write(jwt);
            out.writeLong(credentials.getValidUntilMillis());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    public static ClickHouseCredentials decode(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("Empty ClickHouse delegation token payload");
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte version = in.readByte();
            if (version != VERSION) {
                throw new IllegalArgumentException("Unsupported ClickHouse delegation token payload version " + version
                        + "; make sure every Flink process uses the same flink-connector-clickhouse-token version");
            }
            int length = in.readInt();
            if (length < 0 || length > in.available()) {
                throw new IllegalArgumentException("Corrupt ClickHouse delegation token payload (length " + length + ")");
            }
            byte[] jwt = new byte[length];
            in.readFully(jwt);
            long validUntilMillis = in.readLong();
            return new ClickHouseCredentials(new String(jwt, StandardCharsets.UTF_8), validUntilMillis);
        } catch (EOFException e) {
            throw new IllegalArgumentException("Truncated ClickHouse delegation token payload", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
```

`ClickHouseCredentialsHolder.java`:

```java
package org.apache.flink.connector.clickhouse.token;

/**
 * JVM-wide slot for the current ClickHouse JWT.
 *
 * <p>{@link ClickHouseDelegationTokenReceiver} writes it; the sink reads it before every batch.
 * Deliberately free of Flink types so that sink code without a Flink dependency can read it. It
 * works across classloaders only because this class is loaded from Flink's {@code lib/}
 * (package {@code org.apache.flink.*} is parent-first by default).
 */
public final class ClickHouseCredentialsHolder {

    private static volatile ClickHouseCredentials credentials;

    private ClickHouseCredentialsHolder() {}

    /** Current credentials, or {@code null} if none were received yet. */
    public static ClickHouseCredentials get() {
        return credentials;
    }

    public static void set(ClickHouseCredentials newCredentials) {
        if (newCredentials == null) {
            throw new IllegalArgumentException("credentials must not be null");
        }
        credentials = newCredentials;
    }

    /** Clears the slot. For tests. */
    public static void reset() {
        credentials = null;
    }
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `./gradlew :flink-connector-clickhouse-token:test --tests 'org.apache.flink.connector.clickhouse.token.ClickHouseCredentialsCodecTest'`
Expected: 7 tests PASS.

- [ ] **Step 7: Checkpoint**

Stop. Report. Do not commit.

---

### Task 3: Minimal JSON parser

**Files:**
- Create: `flink-connector-clickhouse-token/src/main/java/org/apache/flink/connector/clickhouse/token/Json.java`
- Test: `flink-connector-clickhouse-token/src/test/java/org/apache/flink/connector/clickhouse/token/JsonTest.java`

**Interfaces:**
- Produces: package-private `final class Json` with `static Object parse(String)` and `static Map<String, Object> parseObject(String)`. Integers parse as `Long`, decimals as `Double`, strings as `String`, arrays as `List<Object>`, objects as `Map<String, Object>`. Malformed input throws `IllegalArgumentException`.

- [ ] **Step 1: Write the failing test**

`JsonTest.java`:

```java
package org.apache.flink.connector.clickhouse.token;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonTest {

    @Test
    void parsesTypicalTokenResponse() {
        Map<String, Object> m = Json.parseObject(
                " { \"access_token\" : \"eyJ.a.b\", \"expires_in\": 3599, \"token_type\":\"Bearer\","
                        + " \"ext\": {\"n\": [1, 2.5, false, null, true]} } ");
        assertEquals("eyJ.a.b", m.get("access_token"));
        assertEquals(3599L, m.get("expires_in"));
        @SuppressWarnings("unchecked")
        List<Object> n = (List<Object>) ((Map<String, Object>) m.get("ext")).get("n");
        assertEquals(Arrays.asList(1L, 2.5, false, null, true), n);
    }

    @Test
    void parsesStringEscapes() {
        Map<String, Object> m = Json.parseObject("{\"s\":\"a\\\"b\\\\c\\/d\\n\\u00df\"}");
        assertEquals("a\"b\\c/d\nß", m.get("s"));
    }

    @Test
    void rejectsMalformedInput() {
        assertThrows(IllegalArgumentException.class, () -> Json.parseObject("{\"a\":}"));
        assertThrows(IllegalArgumentException.class, () -> Json.parseObject("{\"a\":1} trailing"));
        assertThrows(IllegalArgumentException.class, () -> Json.parseObject("[1,2]"));
        assertThrows(IllegalArgumentException.class, () -> Json.parseObject("{\"a\":\"unterminated}"));
        assertThrows(IllegalArgumentException.class, () -> Json.parseObject(""));
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./gradlew :flink-connector-clickhouse-token:test --tests 'org.apache.flink.connector.clickhouse.token.JsonTest'`
Expected: compilation FAILS with `cannot find symbol: variable Json`.

- [ ] **Step 3: Implement `Json.java`**

```java
package org.apache.flink.connector.clickhouse.token;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Minimal JSON parser for OAuth token responses and JWT claims (keeps the token jar dependency-free). */
final class Json {

    private Json() {}

    static Object parse(String text) {
        Parser p = new Parser(text);
        p.skipWhitespace();
        Object value = p.value();
        p.skipWhitespace();
        if (p.pos != text.length()) {
            throw p.error("Trailing characters");
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> parseObject(String text) {
        Object value = parse(text);
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException("Expected a JSON object");
        }
        return (Map<String, Object>) value;
    }

    private static final class Parser {
        private final String s;
        private int pos;

        Parser(String s) {
            this.s = s;
        }

        Object value() {
            if (pos >= s.length()) {
                throw error("Unexpected end of input");
            }
            char c = s.charAt(pos);
            switch (c) {
                case '{': return object();
                case '[': return array();
                case '"': return string();
                case 't': return literal("true", Boolean.TRUE);
                case 'f': return literal("false", Boolean.FALSE);
                case 'n': return literal("null", null);
                default:
                    if (c == '-' || (c >= '0' && c <= '9')) {
                        return number();
                    }
                    throw error("Unexpected character '" + c + "'");
            }
        }

        private Map<String, Object> object() {
            Map<String, Object> map = new LinkedHashMap<>();
            pos++;
            skipWhitespace();
            if (peek('}')) {
                pos++;
                return map;
            }
            while (true) {
                skipWhitespace();
                if (!peek('"')) {
                    throw error("Expected object key");
                }
                String key = string();
                skipWhitespace();
                expect(':');
                skipWhitespace();
                map.put(key, value());
                skipWhitespace();
                if (peek(',')) {
                    pos++;
                    continue;
                }
                expect('}');
                return map;
            }
        }

        private List<Object> array() {
            List<Object> list = new ArrayList<>();
            pos++;
            skipWhitespace();
            if (peek(']')) {
                pos++;
                return list;
            }
            while (true) {
                skipWhitespace();
                list.add(value());
                skipWhitespace();
                if (peek(',')) {
                    pos++;
                    continue;
                }
                expect(']');
                return list;
            }
        }

        private String string() {
            pos++; // opening quote
            StringBuilder sb = new StringBuilder();
            while (true) {
                if (pos >= s.length()) {
                    throw error("Unterminated string");
                }
                char c = s.charAt(pos++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c != '\\') {
                    sb.append(c);
                    continue;
                }
                if (pos >= s.length()) {
                    throw error("Unterminated escape");
                }
                char e = s.charAt(pos++);
                switch (e) {
                    case '"': sb.append('"'); break;
                    case '\\': sb.append('\\'); break;
                    case '/': sb.append('/'); break;
                    case 'b': sb.append('\b'); break;
                    case 'f': sb.append('\f'); break;
                    case 'n': sb.append('\n'); break;
                    case 'r': sb.append('\r'); break;
                    case 't': sb.append('\t'); break;
                    case 'u':
                        if (pos + 4 > s.length()) {
                            throw error("Bad unicode escape");
                        }
                        sb.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
                        pos += 4;
                        break;
                    default:
                        throw error("Bad escape '\\" + e + "'");
                }
            }
        }

        private Number number() {
            int start = pos;
            if (peek('-')) {
                pos++;
            }
            while (pos < s.length() && "0123456789.eE+-".indexOf(s.charAt(pos)) >= 0) {
                pos++;
            }
            String n = s.substring(start, pos);
            try {
                if (n.contains(".") || n.contains("e") || n.contains("E")) {
                    return Double.parseDouble(n);
                }
                return Long.parseLong(n);
            } catch (NumberFormatException ex) {
                throw error("Bad number '" + n + "'");
            }
        }

        private Object literal(String word, Object value) {
            if (!s.startsWith(word, pos)) {
                throw error("Expected '" + word + "'");
            }
            pos += word.length();
            return value;
        }

        void skipWhitespace() {
            while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) {
                pos++;
            }
        }

        private boolean peek(char c) {
            return pos < s.length() && s.charAt(pos) == c;
        }

        private void expect(char c) {
            if (!peek(c)) {
                throw error("Expected '" + c + "'");
            }
            pos++;
        }

        IllegalArgumentException error(String message) {
            return new IllegalArgumentException(message + " at position " + pos);
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :flink-connector-clickhouse-token:test --tests 'org.apache.flink.connector.clickhouse.token.JsonTest'`
Expected: 3 tests PASS.

- [ ] **Step 5: Checkpoint**

Stop. Report. Do not commit.

---

### Task 4: `OAuthClientCredentialsSource`

**Files:**
- Create (test helper): `flink-connector-clickhouse-token/src/test/java/org/apache/flink/connector/clickhouse/token/StubHttpServer.java`
- Create: `flink-connector-clickhouse-token/src/main/java/org/apache/flink/connector/clickhouse/token/OAuthClientCredentialsSource.java`
- Test: `flink-connector-clickhouse-token/src/test/java/org/apache/flink/connector/clickhouse/token/OAuthClientCredentialsSourceTest.java`

**Interfaces:**
- Consumes: `ClickHouseCredentials` (Task 2), `Json.parseObject` (Task 3).
- Produces:
  - package-private `final class OAuthClientCredentialsSource`, with:
    - constructor `(URI tokenEndpoint, String clientId, String clientSecret, String scope /* nullable */, String audience /* nullable */, HttpClient http, Clock clock)`
    - `ClickHouseCredentials obtain() throws IOException, InterruptedException`
    - `static long expiryFromJwt(String) throws IOException`
    - `static List<String> cloudCompatibilityProblems(String jwt)` (empty means OK)
  - test helper `StubHttpServer`: `respond(int, String)`, `uri(String)`, `close()`, and fields `lastBody`, `lastPath`, `lastContentType`. Task 6 reuses it.

- [ ] **Step 1: Create the stub helper**

`StubHttpServer.java`:

```java
package org.apache.flink.connector.clickhouse.token;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/** Single-route HTTP stub that records the last request. Test helper. */
final class StubHttpServer implements AutoCloseable {

    private final HttpServer server;
    volatile String lastBody;
    volatile String lastPath;
    volatile String lastContentType;
    private volatile int status = 200;
    private volatile String responseBody = "";

    StubHttpServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    void respond(int status, String body) {
        this.status = status;
        this.responseBody = body;
    }

    URI uri(String path) {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path);
    }

    private void handle(HttpExchange exchange) throws IOException {
        lastBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        lastContentType = exchange.getRequestHeaders().getFirst("Content-Type");
        lastPath = exchange.getRequestURI().getPath();
        byte[] out = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, out.length == 0 ? -1 : out.length);
        if (out.length > 0) {
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(out);
            }
        }
        exchange.close();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
```

- [ ] **Step 2: Write the failing test**

`OAuthClientCredentialsSourceTest.java`:

```java
package org.apache.flink.connector.clickhouse.token;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
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
        return new OAuthClientCredentialsSource(stub.uri("/oauth2/token"), "flink", SECRET, scope, audience,
                HttpClient.newHttpClient(), CLOCK);
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
```

- [ ] **Step 3: Run it to verify it fails**

Run: `./gradlew :flink-connector-clickhouse-token:test --tests 'org.apache.flink.connector.clickhouse.token.OAuthClientCredentialsSourceTest'`
Expected: compilation FAILS with `cannot find symbol: class OAuthClientCredentialsSource`.

- [ ] **Step 4: Implement `OAuthClientCredentialsSource.java`**

```java
package org.apache.flink.connector.clickhouse.token;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * OAuth2 client-credentials grant against the customer's identity provider. Called on the
 * JobManager only, each time Flink asks the provider for a new token.
 */
final class OAuthClientCredentialsSource {

    private static final List<String> CLOUD_ALGORITHMS = Arrays.asList("RS256", "ES256", "ES384", "ES512");
    private static final List<String> CLOUD_REQUIRED_CLAIMS = Arrays.asList("exp", "iat", "iss", "sub", "aud");

    private final URI tokenEndpoint;
    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private final String audience;
    private final HttpClient http;
    private final Clock clock;

    OAuthClientCredentialsSource(URI tokenEndpoint, String clientId, String clientSecret, String scope,
                                String audience, HttpClient http, Clock clock) {
        this.tokenEndpoint = tokenEndpoint;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.audience = audience;
        this.http = http;
        this.clock = clock;
    }

    ClickHouseCredentials obtain() throws IOException, InterruptedException {
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
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(form.toString(), StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() != 200) {
            throw new IOException("OAuth token request to " + tokenEndpoint + " failed with HTTP "
                    + response.statusCode() + ": " + response.body().trim());
        }
        Map<String, Object> json;
        try {
            json = Json.parseObject(response.body());
        } catch (IllegalArgumentException e) {
            throw new IOException("OAuth token response is not a JSON object", e);
        }
        Object token = json.get("access_token");
        if (!(token instanceof String) || ((String) token).isEmpty()) {
            throw new IOException("OAuth token response has no access_token");
        }
        String jwt = (String) token;
        return new ClickHouseCredentials(jwt, validUntilMillis(json.get("expires_in"), jwt));
    }

    private long validUntilMillis(Object expiresIn, String jwt) throws IOException {
        if (expiresIn instanceof Number) {
            return clock.millis() + ((Number) expiresIn).longValue() * 1000L;
        }
        if (expiresIn instanceof String) {
            try {
                return clock.millis() + Long.parseLong(((String) expiresIn).trim()) * 1000L;
            } catch (NumberFormatException e) {
                throw new IOException("OAuth token response has a non-numeric expires_in");
            }
        }
        return expiryFromJwt(jwt);
    }

    static long expiryFromJwt(String jwt) throws IOException {
        String[] parts = jwt.split("\\.");
        if (parts.length < 2) {
            throw new IOException("access_token is not a JWT and the response has no expires_in");
        }
        Map<String, Object> claims;
        try {
            claims = decodePart(parts[1]);
        } catch (IllegalArgumentException e) {
            throw new IOException("Cannot decode the JWT payload to read exp and the response has no expires_in", e);
        }
        Object exp = claims.get("exp");
        if (!(exp instanceof Number)) {
            throw new IOException("JWT has no numeric exp claim and the response has no expires_in");
        }
        return ((Number) exp).longValue() * 1000L;
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
        Map<String, Object> header;
        Map<String, Object> claims;
        try {
            header = decodePart(parts[0]);
            claims = decodePart(parts[1]);
        } catch (IllegalArgumentException e) {
            problems.add("access_token header or payload is not base64url-encoded JSON");
            return problems;
        }
        Object alg = header.get("alg");
        if (!CLOUD_ALGORITHMS.contains(alg)) {
            problems.add("alg " + alg + " is not supported by ClickHouse Cloud (RS256, ES256, ES384, ES512)");
        }
        if (!(header.get("kid") instanceof String)) {
            problems.add("header has no kid (ClickHouse Cloud selects the JWKS key by kid)");
        }
        for (String claim : CLOUD_REQUIRED_CLAIMS) {
            if (!claims.containsKey(claim)) {
                problems.add("missing required claim " + claim);
            }
        }
        Object aud = claims.get("aud");
        if (aud != null && !(aud instanceof String)) {
            problems.add("aud must be a single string, got " + (aud instanceof List ? "an array" : aud.getClass().getSimpleName()));
        }
        return problems;
    }

    private static Map<String, Object> decodePart(String part) {
        return Json.parseObject(new String(Base64.getUrlDecoder().decode(part), StandardCharsets.UTF_8));
    }

    private static void appendParam(StringBuilder form, String key, String value) {
        form.append('&')
                .append(URLEncoder.encode(key, StandardCharsets.UTF_8))
                .append('=')
                .append(URLEncoder.encode(value, StandardCharsets.UTF_8));
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `./gradlew :flink-connector-clickhouse-token:test --tests 'org.apache.flink.connector.clickhouse.token.OAuthClientCredentialsSourceTest'`
Expected: 9 tests PASS.

- [ ] **Step 6: Checkpoint**

Stop. Report. Do not commit.

---

### Task 5: `ProviderConfig`

**Files:**
- Create: `flink-connector-clickhouse-token/src/main/java/org/apache/flink/connector/clickhouse/token/ProviderConfig.java`
- Test: `flink-connector-clickhouse-token/src/test/java/org/apache/flink/connector/clickhouse/token/ProviderConfigTest.java`

**Interfaces:**
- Consumes: Flink `TimeUtils`, `IllegalConfigurationException`.
- Produces: `public final class ProviderConfig`, with:
  - `PREFIX`
  - `static ProviderConfig parse(Map<String, String>)`
  - `isActive()`, `getTokenEndpoint()` (`URI`), `getClientId()`, `getClientSecret()`, `getScope()`, `getAudience()`
  - `Optional<String> renewalWarning(Duration tokenLifetime)`

- [ ] **Step 1: Write the failing test**

`ProviderConfigTest.java`:

```java
package org.apache.flink.connector.clickhouse.token;

import org.apache.flink.configuration.IllegalConfigurationException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.connector.clickhouse.token.ProviderConfig.PREFIX;
import static org.junit.jupiter.api.Assertions.*;

class ProviderConfigTest {

    private static Map<String, String> oauth() {
        Map<String, String> m = new HashMap<>();
        m.put(PREFIX + "oauth.token-endpoint", "https://idp.example.com/oauth2/token");
        m.put(PREFIX + "oauth.client-id", "flink");
        m.put(PREFIX + "oauth.client-secret", "s3cret");
        return m;
    }

    @Test
    void noTokenEndpointMeansInactive() {
        assertFalse(ProviderConfig.parse(Collections.emptyMap()).isActive());
    }

    @Test
    void blankTokenEndpointMeansInactive() {
        assertFalse(ProviderConfig.parse(Collections.singletonMap(PREFIX + "oauth.token-endpoint", "  ")).isActive());
    }

    @Test
    void parsesAllKeys() {
        Map<String, String> m = oauth();
        m.put(PREFIX + "oauth.scope", "api://ch/.default");
        m.put(PREFIX + "oauth.audience", "my-service");
        ProviderConfig c = ProviderConfig.parse(m);
        assertTrue(c.isActive());
        assertEquals(URI.create("https://idp.example.com/oauth2/token"), c.getTokenEndpoint());
        assertEquals("flink", c.getClientId());
        assertEquals("s3cret", c.getClientSecret());
        assertEquals("api://ch/.default", c.getScope());
        assertEquals("my-service", c.getAudience());
    }

    @Test
    void optionalKeysDefaultToNull() {
        ProviderConfig c = ProviderConfig.parse(oauth());
        assertNull(c.getScope());
        assertNull(c.getAudience());
    }

    @Test
    void valuesAreTrimmed() {
        Map<String, String> m = oauth();
        m.put(PREFIX + "oauth.client-id", " flink \t");
        m.put(PREFIX + "oauth.token-endpoint", " https://idp.example.com/oauth2/token ");
        ProviderConfig c = ProviderConfig.parse(m);
        assertEquals("flink", c.getClientId());
        assertEquals(URI.create("https://idp.example.com/oauth2/token"), c.getTokenEndpoint());
    }

    @Test
    void tokenEndpointMustBeHttpOrHttps() {
        Map<String, String> m = oauth();
        m.put(PREFIX + "oauth.token-endpoint", "idp.example.com/token");
        IllegalConfigurationException e = assertThrows(IllegalConfigurationException.class, () -> ProviderConfig.parse(m));
        assertTrue(e.getMessage().contains(PREFIX + "oauth.token-endpoint"), e.getMessage());
    }

    @Test
    void missingClientIdNamesTheKey() {
        Map<String, String> m = oauth();
        m.remove(PREFIX + "oauth.client-id");
        IllegalConfigurationException e = assertThrows(IllegalConfigurationException.class, () -> ProviderConfig.parse(m));
        assertTrue(e.getMessage().contains(PREFIX + "oauth.client-id"), e.getMessage());
    }

    @Test
    void missingClientSecretNamesTheKey() {
        Map<String, String> m = oauth();
        m.remove(PREFIX + "oauth.client-secret");
        IllegalConfigurationException e = assertThrows(IllegalConfigurationException.class, () -> ProviderConfig.parse(m));
        assertTrue(e.getMessage().contains(PREFIX + "oauth.client-secret"), e.getMessage());
        assertFalse(e.getMessage().contains("s3cret"));
    }

    @Test
    void renewalWarningForOneHourTokensWithFlinkDefaults() {
        String warning = ProviderConfig.parse(oauth()).renewalWarning(Duration.ofHours(1)).orElseThrow(AssertionError::new);
        assertTrue(warning.contains("security.delegation.tokens.renewal.retry.backoff"), warning);
        assertTrue(warning.contains("at most 300s"), warning);
    }

    @Test
    void noRenewalWarningWithShortBackoffOrWhenInactive() {
        Map<String, String> m = oauth();
        m.put("security.delegation.tokens.renewal.retry.backoff", "2min");
        assertFalse(ProviderConfig.parse(m).renewalWarning(Duration.ofHours(1)).isPresent());
        assertFalse(ProviderConfig.parse(Collections.emptyMap()).renewalWarning(Duration.ofHours(1)).isPresent());
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./gradlew :flink-connector-clickhouse-token:test --tests 'org.apache.flink.connector.clickhouse.token.ProviderConfigTest'`
Expected: compilation FAILS with `cannot find symbol: class ProviderConfig`.

- [ ] **Step 3: Implement `ProviderConfig.java`**

```java
package org.apache.flink.connector.clickhouse.token;

import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.util.TimeUtils;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/** Typed, validated view of {@code security.delegation.token.provider.clickhouse.*}. */
public final class ProviderConfig {

    public static final String PREFIX = "security.delegation.token.provider.clickhouse.";
    static final String RENEWAL_BACKOFF_KEY = "security.delegation.tokens.renewal.retry.backoff";
    static final String RENEWAL_RATIO_KEY = "security.delegation.tokens.renewal.time-ratio";

    private boolean active;
    private URI tokenEndpoint;
    private String clientId;
    private String clientSecret;
    private String scope;
    private String audience;
    private Duration renewalBackoff;
    private double renewalRatio;

    private ProviderConfig() {}

    public static ProviderConfig parse(Map<String, String> conf) {
        ProviderConfig c = new ProviderConfig();
        c.renewalBackoff = conf.containsKey(RENEWAL_BACKOFF_KEY)
                ? TimeUtils.parseDuration(conf.get(RENEWAL_BACKOFF_KEY)) : Duration.ofHours(1);
        c.renewalRatio = conf.containsKey(RENEWAL_RATIO_KEY)
                ? Double.parseDouble(conf.get(RENEWAL_RATIO_KEY)) : 0.75;

        String endpoint = optional(conf, "oauth.token-endpoint");
        if (endpoint == null) {
            c.active = false;
            return c;
        }
        c.active = true;
        c.tokenEndpoint = httpUri("oauth.token-endpoint", endpoint);
        c.clientId = required(conf, "oauth.client-id");
        c.clientSecret = required(conf, "oauth.client-secret");
        c.scope = optional(conf, "oauth.scope");
        c.audience = optional(conf, "oauth.audience");
        return c;
    }

    /**
     * A WARN message if one failed re-obtain could outlive a token of this lifetime; empty
     * otherwise. Called after the first token, when the identity provider's lifetime is known.
     */
    public Optional<String> renewalWarning(Duration tokenLifetime) {
        if (!active || tokenLifetime.isNegative() || tokenLifetime.isZero()) {
            return Optional.empty();
        }
        long safetyMillis = (long) ((1.0 - renewalRatio) * tokenLifetime.toMillis());
        long maxBackoffMillis = safetyMillis / 3;
        if (renewalBackoff.toMillis() <= maxBackoffMillis) {
            return Optional.empty();
        }
        return Optional.of(String.format(
                "%s=%s leaves fewer than 3 retries before a %s ClickHouse JWT expires (safety window %s); "
                        + "one failed attempt can leave the sink without valid credentials. Set %s to at most %ds.",
                RENEWAL_BACKOFF_KEY, renewalBackoff, tokenLifetime, Duration.ofMillis(safetyMillis),
                RENEWAL_BACKOFF_KEY, Math.max(1, maxBackoffMillis / 1000)));
    }

    private static String optional(Map<String, String> conf, String key) {
        String value = conf.get(PREFIX + key);
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private static String required(Map<String, String> conf, String key) {
        String value = optional(conf, key);
        if (value == null) {
            throw new IllegalConfigurationException("Missing required option " + PREFIX + key
                    + " (required because " + PREFIX + "oauth.token-endpoint is set)");
        }
        return value;
    }

    private static URI httpUri(String key, String value) {
        URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalConfigurationException("Invalid URI '" + value + "' for " + PREFIX + key, e);
        }
        if (!"http".equalsIgnoreCase(uri.getScheme()) && !"https".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalConfigurationException(PREFIX + key + " must be an http:// or https:// URL, got " + value);
        }
        return uri;
    }

    public boolean isActive() { return active; }
    public URI getTokenEndpoint() { return tokenEndpoint; }
    public String getClientId() { return clientId; }
    public String getClientSecret() { return clientSecret; }
    public String getScope() { return scope; }
    public String getAudience() { return audience; }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :flink-connector-clickhouse-token:test --tests 'org.apache.flink.connector.clickhouse.token.ProviderConfigTest'`
Expected: 10 tests PASS.

- [ ] **Step 5: Checkpoint**

Stop. Report. Do not commit.

---

### Task 6: Provider, receiver and ServiceLoader registration

**Files:**
- Create: `flink-connector-clickhouse-token/src/main/java/org/apache/flink/connector/clickhouse/token/ClickHouseDelegationTokenProvider.java`
- Create: `flink-connector-clickhouse-token/src/main/java/org/apache/flink/connector/clickhouse/token/ClickHouseDelegationTokenReceiver.java`
- Create: `flink-connector-clickhouse-token/src/main/resources/META-INF/services/org.apache.flink.core.security.token.DelegationTokenProvider`
- Create: `flink-connector-clickhouse-token/src/main/resources/META-INF/services/org.apache.flink.core.security.token.DelegationTokenReceiver`
- Test: `flink-connector-clickhouse-token/src/test/java/org/apache/flink/connector/clickhouse/token/ClickHouseDelegationTokenProviderTest.java`

**Interfaces:**
- Consumes: Tasks 2–5; Flink `DelegationTokenProvider`, `DelegationTokenReceiver`, `Configuration`, `ConfigOptions`.
- Produces: `public class ClickHouseDelegationTokenProvider` (with `SERVICE_NAME = "clickhouse"`) and `public class ClickHouseDelegationTokenReceiver`.

- [ ] **Step 1: Write the failing test**

`ClickHouseDelegationTokenProviderTest.java`:

```java
package org.apache.flink.connector.clickhouse.token;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.core.security.token.DelegationTokenReceiver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ServiceLoader;

import static org.apache.flink.connector.clickhouse.token.ProviderConfig.PREFIX;
import static org.junit.jupiter.api.Assertions.*;

class ClickHouseDelegationTokenProviderTest {

    private StubHttpServer idp;

    @BeforeEach
    void start() throws IOException {
        idp = new StubHttpServer();
    }

    @AfterEach
    void stop() {
        idp.close();
        ClickHouseCredentialsHolder.reset();
    }

    private static void put(Configuration conf, String key, String value) {
        conf.set(ConfigOptions.key(key).stringType().noDefaultValue(), value);
    }

    @Test
    void unconfiguredProviderIsInactive() throws Exception {
        ClickHouseDelegationTokenProvider provider = new ClickHouseDelegationTokenProvider();
        provider.init(new Configuration());
        assertEquals("clickhouse", provider.serviceName());
        assertFalse(provider.delegationTokensRequired());
    }

    @Test
    void obtainsEncodedCredentialsFromTheIdentityProvider() throws Exception {
        idp.respond(200, "{\"access_token\":\"eyJ.payload.sig\",\"expires_in\":3600}");
        Configuration conf = new Configuration();
        put(conf, PREFIX + "oauth.token-endpoint", idp.uri("/token").toString());
        put(conf, PREFIX + "oauth.client-id", "flink");
        put(conf, PREFIX + "oauth.client-secret", "s3cret");

        ClickHouseDelegationTokenProvider provider = new ClickHouseDelegationTokenProvider();
        provider.init(conf);
        assertTrue(provider.delegationTokensRequired());

        long before = System.currentTimeMillis();
        DelegationTokenProvider.ObtainedDelegationTokens obtained = provider.obtainDelegationTokens();
        long validUntil = obtained.getValidUntil().orElseThrow(AssertionError::new);
        assertTrue(validUntil >= before + 3_600_000L && validUntil <= System.currentTimeMillis() + 3_600_000L);
        assertEquals("eyJ.payload.sig", ClickHouseCredentialsCodec.decode(obtained.getTokens()).getJwt());
        assertEquals("/token", idp.lastPath);
    }

    @Test
    void invalidConfigurationFailsInit() {
        Configuration conf = new Configuration();
        put(conf, PREFIX + "oauth.token-endpoint", "https://idp.example.com/token");
        ClickHouseDelegationTokenProvider provider = new ClickHouseDelegationTokenProvider();
        assertThrows(IllegalConfigurationException.class, () -> provider.init(conf));
    }

    @Test
    void receiverPublishesCredentialsToHolder() throws Exception {
        ClickHouseDelegationTokenReceiver receiver = new ClickHouseDelegationTokenReceiver();
        receiver.init(new Configuration());
        assertEquals("clickhouse", receiver.serviceName());
        receiver.onNewTokensObtained(ClickHouseCredentialsCodec.encode(new ClickHouseCredentials("jwt-1", 5L)));
        assertEquals("jwt-1", ClickHouseCredentialsHolder.get().getJwt());
    }

    @Test
    void receiverKeepsPreviousCredentialsOnBadPayload() throws Exception {
        ClickHouseDelegationTokenReceiver receiver = new ClickHouseDelegationTokenReceiver();
        receiver.onNewTokensObtained(ClickHouseCredentialsCodec.encode(new ClickHouseCredentials("jwt-1", 5L)));
        assertThrows(IllegalArgumentException.class, () -> receiver.onNewTokensObtained(new byte[] {9, 1}));
        assertEquals("jwt-1", ClickHouseCredentialsHolder.get().getJwt());
    }

    @Test
    void serviceLoaderFindsProviderAndReceiver() {
        boolean provider = false;
        for (DelegationTokenProvider p : ServiceLoader.load(DelegationTokenProvider.class)) {
            provider |= p instanceof ClickHouseDelegationTokenProvider;
        }
        boolean receiver = false;
        for (DelegationTokenReceiver r : ServiceLoader.load(DelegationTokenReceiver.class)) {
            receiver |= r instanceof ClickHouseDelegationTokenReceiver;
        }
        assertTrue(provider, "provider registered in META-INF/services");
        assertTrue(receiver, "receiver registered in META-INF/services");
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./gradlew :flink-connector-clickhouse-token:test --tests 'org.apache.flink.connector.clickhouse.token.ClickHouseDelegationTokenProviderTest'`
Expected: compilation FAILS with `cannot find symbol: class ClickHouseDelegationTokenProvider`.

- [ ] **Step 3: Implement**

`ClickHouseDelegationTokenProvider.java`:

```java
package org.apache.flink.connector.clickhouse.token;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Obtains short-lived ClickHouse Cloud JWTs on the JobManager with the OAuth2 client-credentials
 * grant. Flink decides when to call {@link #obtainDelegationTokens()}; this class sends the
 * request to the identity provider. Inactive unless
 * {@code security.delegation.token.provider.clickhouse.oauth.token-endpoint} is set.
 */
public class ClickHouseDelegationTokenProvider implements DelegationTokenProvider {

    public static final String SERVICE_NAME = "clickhouse";

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseDelegationTokenProvider.class);

    private ProviderConfig config;
    private OAuthClientCredentialsSource source;
    private boolean renewalSettingsChecked;

    @Override
    public String serviceName() {
        return SERVICE_NAME;
    }

    @Override
    public void init(Configuration configuration) {
        config = ProviderConfig.parse(configuration.toMap());
        if (!config.isActive()) {
            LOG.debug("ClickHouse delegation token provider is not configured ({}oauth.token-endpoint not set); inactive",
                    ProviderConfig.PREFIX);
            return;
        }
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        source = new OAuthClientCredentialsSource(config.getTokenEndpoint(), config.getClientId(),
                config.getClientSecret(), config.getScope(), config.getAudience(), http, Clock.systemUTC());
        LOG.info("ClickHouse delegation token provider initialized: token endpoint {}", config.getTokenEndpoint());
    }

    @Override
    public boolean delegationTokensRequired() {
        return config != null && config.isActive();
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() throws Exception {
        ClickHouseCredentials credentials = source.obtain();
        LOG.info("Obtained ClickHouse JWT {} valid until {}", ClickHouseCredentials.redact(credentials.getJwt()),
                Instant.ofEpochMilli(credentials.getValidUntilMillis()));

        List<String> problems = OAuthClientCredentialsSource.cloudCompatibilityProblems(credentials.getJwt());
        if (!problems.isEmpty()) {
            LOG.warn("The identity provider's access token will likely be rejected by ClickHouse Cloud: {}. "
                    + "Fix the identity provider app or the Cloud JWT provider settings.", String.join("; ", problems));
        }
        if (!renewalSettingsChecked) {
            renewalSettingsChecked = true;
            config.renewalWarning(Duration.ofMillis(credentials.getValidUntilMillis() - System.currentTimeMillis()))
                    .ifPresent(LOG::warn);
        }
        return new ObtainedDelegationTokens(ClickHouseCredentialsCodec.encode(credentials),
                Optional.of(credentials.getValidUntilMillis()));
    }
}
```

`ClickHouseDelegationTokenReceiver.java`:

```java
package org.apache.flink.connector.clickhouse.token;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/** Receives the ClickHouse JWT on every TaskManager and publishes it to {@link ClickHouseCredentialsHolder}. */
public class ClickHouseDelegationTokenReceiver implements DelegationTokenReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseDelegationTokenReceiver.class);

    @Override
    public String serviceName() {
        return ClickHouseDelegationTokenProvider.SERVICE_NAME;
    }

    @Override
    public void init(Configuration configuration) {
        // Nothing to configure: the JWT carries everything the sink needs.
    }

    @Override
    public void onNewTokensObtained(byte[] tokens) {
        ClickHouseCredentials credentials = ClickHouseCredentialsCodec.decode(tokens);
        ClickHouseCredentialsHolder.set(credentials);
        LOG.info("Received ClickHouse JWT {} valid until {}", ClickHouseCredentials.redact(credentials.getJwt()),
                Instant.ofEpochMilli(credentials.getValidUntilMillis()));
    }
}
```

`src/main/resources/META-INF/services/org.apache.flink.core.security.token.DelegationTokenProvider`:

```
org.apache.flink.connector.clickhouse.token.ClickHouseDelegationTokenProvider
```

`src/main/resources/META-INF/services/org.apache.flink.core.security.token.DelegationTokenReceiver`:

```
org.apache.flink.connector.clickhouse.token.ClickHouseDelegationTokenReceiver
```

- [ ] **Step 4: Run the whole module suite**

Run: `./gradlew :flink-connector-clickhouse-token:test`
Expected: 35 tests PASS (7 codec + 3 JSON + 9 OAuth + 10 config + 6 provider).

- [ ] **Step 5: Check that the jar has no third-party classes**

Run: `./gradlew :flink-connector-clickhouse-token:jar && unzip -l flink-connector-clickhouse-token/build/libs/flink-connector-clickhouse-token-*.jar | grep '\.class' | grep -v 'org/apache/flink/connector/clickhouse/token/' || echo CLEAN`
Expected: `CLEAN`

- [ ] **Step 6: Checkpoint**

Stop. Report. Do not commit.

---

### Task 7: Sink bridge (`-base`) and retriable auth errors

**Files:**
- Modify: `flink-connector-clickhouse-base/build.gradle.kts`
- Create: `flink-connector-clickhouse-base/src/main/java/org/apache/flink/connector/clickhouse/auth/AuthMode.java`
- Create: `flink-connector-clickhouse-base/src/main/java/org/apache/flink/connector/clickhouse/auth/ResolvedCredentials.java`
- Create: `flink-connector-clickhouse-base/src/main/java/org/apache/flink/connector/clickhouse/auth/DelegationTokenBridge.java`
- Modify: `flink-connector-clickhouse-base/src/main/java/com/clickhouse/utils/Utils.java:50-58`
- Test: `flink-connector-clickhouse-base/src/test/java/org/apache/flink/connector/clickhouse/auth/DelegationTokenBridgeTest.java`
- Test: `flink-connector-clickhouse-base/src/test/java/com/clickhouse/utils/UtilsTest.java` (append)

**Interfaces:**
- Consumes: `ClickHouseCredentials`, `ClickHouseCredentialsHolder` (Task 2), `compileOnly`.
- Produces:
  - `AuthMode { PASSWORD, DELEGATION_TOKEN }`
  - `ResolvedCredentials implements Serializable`, with `getJwt()` and `getValidUntilMillis()`
  - `DelegationTokenBridge`, with `static ResolvedCredentials current()` (same instance until the holder changes) and `static ResolvedCredentials await(Duration) throws InterruptedException`
  - `Utils.handleException(Throwable, boolean)`

- [ ] **Step 1: Add the dependency**

In `flink-connector-clickhouse-base/build.gradle.kts`, inside `dependencies { … }`, add after the client-v2 `implementation(...)` block:

```kotlin
    // Delegation tokens: compile against the token module but never bundle it (it is deployed to Flink's lib/).
    compileOnly(project(":flink-connector-clickhouse-token"))
    testImplementation(project(":flink-connector-clickhouse-token"))
```

- [ ] **Step 2: Write the failing bridge test**

`DelegationTokenBridgeTest.java`:

```java
package org.apache.flink.connector.clickhouse.auth;

import org.apache.flink.connector.clickhouse.token.ClickHouseCredentials;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentialsHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class DelegationTokenBridgeTest {

    @AfterEach
    void reset() {
        ClickHouseCredentialsHolder.reset();
    }

    @Test
    void currentIsNullBeforeAnyToken() {
        assertNull(DelegationTokenBridge.current());
    }

    @Test
    void currentReturnsSameInstanceUntilHolderChanges() {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("jwt-1", 10L));
        ResolvedCredentials first = DelegationTokenBridge.current();
        assertSame(first, DelegationTokenBridge.current());
        assertEquals("jwt-1", first.getJwt());
        assertEquals(10L, first.getValidUntilMillis());

        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("jwt-2", 20L));
        ResolvedCredentials second = DelegationTokenBridge.current();
        assertNotSame(first, second);
        assertEquals("jwt-2", second.getJwt());
    }

    @Test
    void awaitReturnsImmediatelyWhenPresent() throws Exception {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("jwt-1", 10L));
        assertEquals("jwt-1", DelegationTokenBridge.await(Duration.ofMillis(1)).getJwt());
    }

    @Test
    void awaitPicksUpTokenThatArrivesLater() throws Exception {
        Thread publisher = new Thread(() -> {
            try {
                Thread.sleep(300);
            } catch (InterruptedException ignored) {
            }
            ClickHouseCredentialsHolder.set(new ClickHouseCredentials("late", 10L));
        });
        publisher.start();
        assertEquals("late", DelegationTokenBridge.await(Duration.ofSeconds(5)).getJwt());
        publisher.join();
    }

    @Test
    void awaitTimesOutWithDiagnosticMessage() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> DelegationTokenBridge.await(Duration.ofMillis(200)));
        assertTrue(e.getMessage().contains("security.delegation.token.provider.clickhouse.oauth.token-endpoint"), e.getMessage());
        assertTrue(e.getMessage().contains("lib/"), e.getMessage());
        assertTrue(e.getMessage().contains("classloader.parent-first-patterns.default"), e.getMessage());
    }

    @Test
    void resolvedCredentialsToStringIsRedacted() {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("abcdefghijkl", 10L));
        assertFalse(DelegationTokenBridge.current().toString().contains("abcdefghijkl"));
    }
}
```

- [ ] **Step 3: Append the failing `Utils` tests**

In `UtilsTest.java`, add these imports if they're missing: `com.clickhouse.client.api.ServerException`, `org.apache.flink.connector.clickhouse.exception.FlinkWriteException`, `static org.junit.jupiter.api.Assertions.assertFalse`, `static org.junit.jupiter.api.Assertions.assertThrows`. Then append inside the class.

`RetriableException` extends `FlinkWriteException`, so the "stays fatal" tests also assert the exception is not retriable:

```java
    @Test void authenticationFailedIsRetriableInDelegationTokenMode() {
        assertThrows(RetriableException.class,
                () -> Utils.handleException(new ServerException(516, "Authentication failed", 403, "q1"), true));
    }

    @Test void userExpiredIsRetriableInDelegationTokenMode() {
        assertThrows(RetriableException.class,
                () -> Utils.handleException(new ServerException(720, "User expired", 403, "q1"), true));
    }

    @Test void authenticationFailedStaysFatalInPasswordMode() {
        FlinkWriteException e1 = assertThrows(FlinkWriteException.class,
                () -> Utils.handleException(new ServerException(516, "Authentication failed", 403, "q1"), false));
        assertFalse(e1 instanceof RetriableException);
        FlinkWriteException e2 = assertThrows(FlinkWriteException.class,
                () -> Utils.handleException(new ServerException(516, "Authentication failed", 403, "q1")));
        assertFalse(e2 instanceof RetriableException);
    }

    @Test void accessDeniedStaysFatalInDelegationTokenMode() {
        FlinkWriteException e = assertThrows(FlinkWriteException.class,
                () -> Utils.handleException(new ServerException(497, "Not enough privileges", 403, "q1"), true));
        assertFalse(e instanceof RetriableException);
    }
```

- [ ] **Step 4: Run them to verify they fail**

Run: `./gradlew :flink-connector-clickhouse-base:test --tests 'org.apache.flink.connector.clickhouse.auth.DelegationTokenBridgeTest' --tests 'com.clickhouse.utils.UtilsTest'`
Expected: compilation FAILS with `cannot find symbol: class DelegationTokenBridge` and `no suitable method found for handleException(ServerException,boolean)`.

- [ ] **Step 5: Implement the bridge classes**

`AuthMode.java`:

```java
package org.apache.flink.connector.clickhouse.auth;

/** How the sink authenticates to ClickHouse. */
public enum AuthMode {
    /** Username/password configured in job code. */
    PASSWORD,
    /** Short-lived ClickHouse Cloud JWT from Flink's delegation token framework (flink-connector-clickhouse-token in lib/). */
    DELEGATION_TOKEN
}
```

`ResolvedCredentials.java`:

```java
package org.apache.flink.connector.clickhouse.auth;

import java.io.Serializable;

/**
 * Sink-side copy of the current delegation token JWT. Contains no token-module types, so sink
 * classes holding it stay loadable when the token jar is absent (password mode).
 */
public final class ResolvedCredentials implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jwt;
    private final long validUntilMillis;

    ResolvedCredentials(String jwt, long validUntilMillis) {
        this.jwt = jwt;
        this.validUntilMillis = validUntilMillis;
    }

    public String getJwt() {
        return jwt;
    }

    public long getValidUntilMillis() {
        return validUntilMillis;
    }

    @Override
    public String toString() {
        String shown = jwt == null || jwt.length() <= 4 ? "****" : jwt.substring(0, 4) + "…";
        return "ResolvedCredentials{jwt=" + shown + ", validUntilMillis=" + validUntilMillis + '}';
    }
}
```

`DelegationTokenBridge.java`:

```java
package org.apache.flink.connector.clickhouse.auth;

import org.apache.flink.connector.clickhouse.token.ClickHouseCredentials;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentialsHolder;

import java.time.Duration;

/**
 * The only sink class that touches flink-connector-clickhouse-token. Loaded only in
 * {@link AuthMode#DELEGATION_TOKEN} mode, so password-mode jobs never need the token jar.
 */
public final class DelegationTokenBridge {

    static final String MISSING_JAR_MESSAGE =
            "ClickHouse delegation token auth requires flink-connector-clickhouse-token.jar in Flink's lib/ directory "
                    + "on the JobManager and every TaskManager";

    static final String TIMEOUT_MESSAGE =
            "No ClickHouse delegation token received within %s. Check that (1) "
                    + "security.delegation.token.provider.clickhouse.oauth.token-endpoint (with client id/secret) is set "
                    + "in the Flink configuration and the JobManager log shows 'Obtained ClickHouse JWT', (2) "
                    + "flink-connector-clickhouse-token.jar is in lib/ on the JobManager and every TaskManager, and (3) "
                    + "classloader.parent-first-patterns.default still contains org.apache.flink.";

    private static final long POLL_MILLIS = 100;

    private static ClickHouseCredentials lastSource;
    private static ResolvedCredentials lastResolved;

    private DelegationTokenBridge() {}

    /** Current JWT or {@code null}; the same instance is returned until a new token arrives. */
    public static synchronized ResolvedCredentials current() {
        ClickHouseCredentials source;
        try {
            source = ClickHouseCredentialsHolder.get();
        } catch (NoClassDefFoundError e) {
            throw new IllegalStateException(MISSING_JAR_MESSAGE, e);
        }
        if (source == null) {
            return null;
        }
        if (source != lastSource) {
            lastResolved = new ResolvedCredentials(source.getJwt(), source.getValidUntilMillis());
            lastSource = source;
        }
        return lastResolved;
    }

    /** Waits up to {@code timeout} for the first token. */
    public static ResolvedCredentials await(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            ResolvedCredentials credentials = current();
            if (credentials != null) {
                return credentials;
            }
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException(String.format(TIMEOUT_MESSAGE, timeout));
            }
            Thread.sleep(POLL_MILLIS);
        }
    }
}
```

- [ ] **Step 6: Implement the `Utils` overload**

In `Utils.java`, replace:

```java
    public static void handleException(Throwable e) {
        LOG.warn("Deciding how to handle exception: {}", e.getLocalizedMessage());
        Exception rootCause = Utils.getRootCause(e, true);
        if (rootCause instanceof ServerException) {
            ServerException serverException = (ServerException) rootCause;
            LOG.warn("ClickHouse Server Exception Code: {} isRetryable: {}", serverException.getCode(), serverException.isRetryable());
            if (serverException.isRetryable()) {
                throw new RetriableException(e);
            }
```

with:

```java
    public static void handleException(Throwable e) {
        handleException(e, false);
    }

    /**
     * @param delegationTokenMode when {@code true}, authentication failures caused by an expired
     *     short-lived JWT (516 AUTHENTICATION_FAILED, 720 USER_EXPIRED) are retriable: the next
     *     attempt picks up the re-obtained token.
     */
    public static void handleException(Throwable e, boolean delegationTokenMode) {
        LOG.warn("Deciding how to handle exception: {}", e.getLocalizedMessage());
        Exception rootCause = Utils.getRootCause(e, true);
        if (rootCause instanceof ServerException) {
            ServerException serverException = (ServerException) rootCause;
            LOG.warn("ClickHouse Server Exception Code: {} isRetryable: {}", serverException.getCode(), serverException.isRetryable());
            if (serverException.isRetryable()) {
                throw new RetriableException(e);
            }
            if (delegationTokenMode && (serverException.getCode() == 516 || serverException.getCode() == 720)) {
                LOG.error("ClickHouse rejected the delegation token JWT (code {}); retrying with the latest token. "
                        + "If this repeats, check the identity provider app and the Cloud JWT provider (issuer, audience, JWKS URL).",
                        serverException.getCode());
                throw new RetriableException(e);
            }
```

Leave the rest of the method unchanged.

- [ ] **Step 7: Run the tests**

Run: `./gradlew :flink-connector-clickhouse-base:test`
Expected: all base tests PASS, including 6 bridge tests and 4 new `UtilsTest` tests.

- [ ] **Step 8: Checkpoint**

Stop. Report. Do not commit.

---

### Task 8: `ClickHouseClientConfig` and `ClickHouseAsyncWriter` (both versioned modules)

**Files:**
- Modify: `flink-connector-clickhouse-{2.0.0,1.17}/build.gradle.kts`
- Modify: `flink-connector-clickhouse-{2.0.0,1.17}/src/main/java/org/apache/flink/connector/clickhouse/sink/ClickHouseClientConfig.java`
- Modify: `flink-connector-clickhouse-2.0.0/src/main/java/org/apache/flink/connector/clickhouse/sink/ClickHouseAsyncWriter.java:179,258`
- Modify: `flink-connector-clickhouse-1.17/src/main/java/org/apache/flink/connector/clickhouse/sink/ClickHouseAsyncWriter.java:175,259`
- Test: `flink-connector-clickhouse-{2.0.0,1.17}/src/test/java/org/apache/flink/connector/clickhouse/sink/ClickHouseClientConfigDelegationTokenTests.java`

**Interfaces:**
- Consumes: `AuthMode`, `ResolvedCredentials`, `DelegationTokenBridge`, `Utils.handleException(Throwable, boolean)` (Task 7); client-v2 0.10.0 `updateBearerToken` (Task 1).
- Produces, on `ClickHouseClientConfig`:
  - `static forDelegationToken(String url, String database, String tableName)`, which throws `IllegalArgumentException` for non-HTTPS, non-loopback URLs
  - `setDelegationTokenWaitTimeout(Duration)`
  - `isDelegationTokenMode()`
  - `refreshCredentialsIfChanged()`
  - package-private `getAppliedCredentials()`

- [ ] **Step 1: Add the dependencies in both build files**

Right after `implementation(project(":flink-connector-clickhouse-base"))`:

```kotlin
    // Delegation tokens: deployed to Flink's lib/, never bundled into the shadow JAR.
    compileOnly(project(":flink-connector-clickhouse-token"))
    testImplementation(project(":flink-connector-clickhouse-token"))
```

- [ ] **Step 2: Write the failing test (2.0.0)**

`flink-connector-clickhouse-2.0.0/src/test/java/org/apache/flink/connector/clickhouse/sink/ClickHouseClientConfigDelegationTokenTests.java`:

```java
package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.Client;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentials;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentialsHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/** No ClickHouse server needed: client-v2 builds clients lazily and these tests never send a request. */
class ClickHouseClientConfigDelegationTokenTests {

    // Port 1 is never listening: proves no connection is attempted while building the config.
    private static final String UNREACHABLE_URL = "https://127.0.0.1:1/";

    @AfterEach
    void reset() {
        ClickHouseCredentialsHolder.reset();
    }

    private static void publish(String jwt) {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials(jwt, System.currentTimeMillis() + 60_000));
    }

    @Test
    void forDelegationTokenRequiresHttpsExceptForLoopback() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ClickHouseClientConfig.forDelegationToken("http://my-service.clickhouse.cloud:8123/", "db", "events"));
        assertTrue(e.getMessage().contains("HTTPS"), e.getMessage());
        assertTrue(ClickHouseClientConfig.forDelegationToken("http://127.0.0.1:8123/", "db", "events").isDelegationTokenMode());
        assertTrue(ClickHouseClientConfig.forDelegationToken("http://localhost:8123/", "db", "events").isDelegationTokenMode());
    }

    @Test
    void forDelegationTokenDoesNotPingTheServer() {
        long start = System.nanoTime();
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        assertTrue(config.isDelegationTokenMode());
        assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() < 1000, "constructor must not ping/retry");
    }

    @Test
    void createClientUsesTheJwtFromTheHolder() {
        publish("eyJ.jwt1.sig");
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        assertNotNull(config.createClient());
        assertEquals("eyJ.jwt1.sig", config.getAppliedCredentials().getJwt());
    }

    @Test
    void refreshSwapsTheJwtOnTheSameClient() {
        publish("eyJ.jwt1.sig");
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        Client client = config.createClient();

        config.refreshCredentialsIfChanged();
        assertEquals("eyJ.jwt1.sig", config.getAppliedCredentials().getJwt(), "no change, no swap");

        publish("eyJ.jwt2.sig");
        config.refreshCredentialsIfChanged();
        assertSame(client, config.createClient());
        assertEquals("eyJ.jwt2.sig", config.getAppliedCredentials().getJwt());
    }

    @Test
    void createClientTimesOutWithoutToken() {
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        config.setDelegationTokenWaitTimeout(Duration.ofMillis(200));
        IllegalStateException e = assertThrows(IllegalStateException.class, config::createClient);
        assertTrue(e.getMessage().contains("security.delegation.token.provider.clickhouse.oauth.token-endpoint"), e.getMessage());
    }

    @Test
    void refreshIsNoOpBeforeClientExists() {
        publish("eyJ.jwt1.sig");
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        config.refreshCredentialsIfChanged();
        assertNull(config.getAppliedCredentials());
    }

    @Test
    void configSurvivesJavaSerialization() throws Exception {
        publish("SECRET-JWT-7f3a9c");
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        config.createClient(); // populates transient state that must not travel

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(config);
        }
        assertFalse(new String(bytes.toByteArray(), StandardCharsets.ISO_8859_1).contains("SECRET-JWT-7f3a9c"),
                "the JWT must not be serialized with the config");
        ClickHouseClientConfig copy;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            copy = (ClickHouseClientConfig) in.readObject();
        }
        assertTrue(copy.isDelegationTokenMode());
        assertNull(copy.getAppliedCredentials());
        assertNotNull(copy.createClient());
        assertEquals("SECRET-JWT-7f3a9c", copy.getAppliedCredentials().getJwt(), "re-resolved from the holder on the TaskManager side");
    }
}
```

Copy it to 1.17:

Run: `cp flink-connector-clickhouse-2.0.0/src/test/java/org/apache/flink/connector/clickhouse/sink/ClickHouseClientConfigDelegationTokenTests.java flink-connector-clickhouse-1.17/src/test/java/org/apache/flink/connector/clickhouse/sink/`

- [ ] **Step 3: Run it to verify it fails**

Run: `./gradlew :flink-connector-clickhouse-2.0.0:test --tests 'org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfigDelegationTokenTests'`
Expected: compilation FAILS with `cannot find symbol: method forDelegationToken`.

- [ ] **Step 4: Implement the `ClickHouseClientConfig` changes (2.0.0)**

(a) Add imports:

```java
import org.apache.flink.connector.clickhouse.auth.AuthMode;
import org.apache.flink.connector.clickhouse.auth.DelegationTokenBridge;
import org.apache.flink.connector.clickhouse.auth.ResolvedCredentials;

import java.net.URI;
import java.time.Duration;
```

(b) Add fields after `private BatchFailureStrategy batchFailureStrategy = BatchFailureStrategy.STOP_FLINK;`:

```java
    private AuthMode authMode = AuthMode.PASSWORD;
    private Duration delegationTokenWaitTimeout = Duration.ofSeconds(60);
    private transient ResolvedCredentials appliedCredentials = null;
```

(c) Make the password constructor's log line null-safe. Replace:

```java
        LOG.info("ClickHouseClientConfig: url={}, user={}, password={}, database={}", url, username, "x".repeat(password.length()), database);
```

with:

```java
        LOG.info("ClickHouseClientConfig: url={}, user={}, password={}, database={}", url, username,
                password == null ? "null" : "x".repeat(password.length()), database);
```

(d) After the last existing public constructor, add:

```java
    private ClickHouseClientConfig(String url, String database, String tableName, AuthMode authMode) {
        this.url = url;
        this.username = null;
        this.password = null;
        this.database = database;
        this.tableName = tableName;
        this.fullProductName = String.format("Flink-ClickHouse-Sink/%s (fv:flink/%s, lv:scala/%s)", ClickHouseSinkVersion.getVersion(), EnvironmentInformation.getVersion(), EnvironmentInformation.getScalaVersion());
        this.options = new HashMap<>();
        this.serverSettings = new HashMap<>();
        this.authMode = authMode;
        // No ping here: this runs where the job graph is built, which never receives delegation tokens.
        LOG.info("ClickHouseClientConfig: url={}, auth=delegation-token (JWT), database={}", url, database);
    }

    /**
     * Config that authenticates to ClickHouse Cloud with short-lived JWTs from Flink's delegation
     * token framework. Requires flink-connector-clickhouse-token in Flink's lib/ and
     * {@code security.delegation.token.provider.clickhouse.oauth.*} in the Flink configuration.
     */
    public static ClickHouseClientConfig forDelegationToken(String url, String database, String tableName) {
        requireHttpsOrLoopback(url);
        return new ClickHouseClientConfig(url, database, tableName, AuthMode.DELEGATION_TOKEN);
    }

    /** ClickHouse Cloud: "Always send JWTs over HTTPS". Plain HTTP is allowed only for loopback test endpoints. */
    static void requireHttpsOrLoopback(String url) {
        URI uri = URI.create(url);
        String scheme = uri.getScheme();
        String host = uri.getHost();
        boolean loopback = "localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host)
                || "[::1]".equals(host) || "::1".equals(host);
        if ("https".equalsIgnoreCase(scheme) || ("http".equalsIgnoreCase(scheme) && loopback)) {
            return;
        }
        throw new IllegalArgumentException("ClickHouse JWTs must be sent over HTTPS (got " + url
                + "); plain http:// is only allowed for loopback test endpoints");
    }
```

(e) In `initClient`, replace:

```java
        Client.Builder clientBuilder = new Client.Builder()
                .addEndpoint(url)
                .setUsername(username)
                .setPassword(password)
                .setDefaultDatabase(database)
                .setClientName(fullProductName)
                .setOption(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), "true")
                .setOptions(options);
```

with:

```java
        Client.Builder clientBuilder = new Client.Builder()
                .addEndpoint(url)
                .setDefaultDatabase(database)
                .setClientName(fullProductName)
                .setOption(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), "true")
                .setOptions(options);
        if (authMode == AuthMode.DELEGATION_TOKEN) {
            clientBuilder.useBearerTokenAuth(appliedCredentials.getJwt());
        } else {
            clientBuilder.setUsername(username).setPassword(password);
        }
```

Keep the existing `serverSettings` loop and `return clientBuilder.build();`.

(f) Replace `createClient(String database)`:

```java
    public Client createClient(String database) {
        if (this.client == null) {
            this.client = initClient(database);
        }
        return client;
    }
```

with:

```java
    public Client createClient(String database) {
        if (this.client == null) {
            if (authMode == AuthMode.DELEGATION_TOKEN) {
                try {
                    appliedCredentials = DelegationTokenBridge.await(delegationTokenWaitTimeout);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for ClickHouse delegation token", e);
                }
            }
            this.client = initClient(database);
        }
        return client;
    }

    /**
     * Applies a newer delegation token JWT to the live client, if one arrived since the last call.
     * Cheap no-op otherwise (one volatile read). Call before every insert.
     */
    public void refreshCredentialsIfChanged() {
        if (authMode != AuthMode.DELEGATION_TOKEN || client == null) {
            return;
        }
        ResolvedCredentials current = DelegationTokenBridge.current();
        if (current == null || current == appliedCredentials) {
            return;
        }
        client.updateBearerToken(current.getJwt());
        appliedCredentials = current;
        LOG.info("Applied re-obtained ClickHouse delegation token {}", current);
    }

    public boolean isDelegationTokenMode() {
        return authMode == AuthMode.DELEGATION_TOKEN;
    }

    public void setDelegationTokenWaitTimeout(Duration delegationTokenWaitTimeout) {
        this.delegationTokenWaitTimeout = Objects.requireNonNull(delegationTokenWaitTimeout, "delegationTokenWaitTimeout");
    }

    ResolvedCredentials getAppliedCredentials() {
        return appliedCredentials;
    }
```

- [ ] **Step 5: Wire the writer (2.0.0)**

In `ClickHouseAsyncWriter.java`, at line 179, replace:

```java
        Client chClient = this.clickHouseClientConfig.createClient();
```

with:

```java
        Client chClient = this.clickHouseClientConfig.createClient();
        this.clickHouseClientConfig.refreshCredentialsIfChanged();
```

At line 258 (now 259), replace:

```java
            Utils.handleException(error);
```

with:

```java
            Utils.handleException(error, clickHouseClientConfig.isDelegationTokenMode());
```

- [ ] **Step 6: Run the 2.0.0 test**

Run: `./gradlew :flink-connector-clickhouse-2.0.0:test --tests 'org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfigDelegationTokenTests'`
Expected: 7 tests PASS.

- [ ] **Step 7: Apply the identical changes to 1.17**

Repeat Steps 4 and 5 verbatim on the 1.17 `ClickHouseClientConfig.java` and `ClickHouseAsyncWriter.java`. In the 1.17 writer, `createClient()` is at line 175 and `Utils.handleException(error);` is at 259.

- [ ] **Step 8: Run both modules' config tests**

Run: `./gradlew :flink-connector-clickhouse-1.17:test --tests 'org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfigDelegationTokenTests' :flink-connector-clickhouse-2.0.0:test --tests 'org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfigDelegationTokenTests'`
Expected: 7 + 7 tests PASS.

- [ ] **Step 9: Check that the shadow JARs don't contain token classes**

Run: `./gradlew :flink-connector-clickhouse-1.17:shadowJar :flink-connector-clickhouse-2.0.0:shadowJar && for j in flink-connector-clickhouse-1.17/build/libs/*-all.jar flink-connector-clickhouse-2.0.0/build/libs/*-all.jar; do echo "$j: $(unzip -l "$j" | grep -c 'org/apache/flink/connector/clickhouse/token/')"; done`
Expected: both lines end with `: 0`.

- [ ] **Step 10: Regression-run the existing suites (needs Docker)**

Run: `./gradlew :flink-connector-clickhouse-1.17:test :flink-connector-clickhouse-2.0.0:test`
Expected: the same results as after Task 1. Report honestly if Docker isn't available.

- [ ] **Step 11: Checkpoint**

Stop. Report. Do not commit.

---

### Task 9: Local end-to-end with a fake identity provider and a JWT-checking fake ClickHouse (both modules)

**Files:**
- Modify: `flink-connector-clickhouse-1.17/src/test/java/org/apache/flink/connector/test/embedded/flink/EmbeddedFlinkClusterForTests.java:26-37`
- Create: `flink-connector-clickhouse-{2.0.0,1.17}/src/test/java/org/apache/flink/connector/clickhouse/sink/FakeIdentityProvider.java`
- Create: `flink-connector-clickhouse-{2.0.0,1.17}/src/test/java/org/apache/flink/connector/clickhouse/sink/FakeJwtClickHouseServer.java`
- Test: `flink-connector-clickhouse-{2.0.0,1.17}/src/test/java/org/apache/flink/connector/clickhouse/sink/ClickHouseDelegationTokenTests.java`

**Interfaces:**
- Consumes: Task 8 API; provider and receiver on the test classpath; `EmbeddedFlinkClusterForTests`; `ClickHouseSinkTestUtils` constants.
- Produces:
  - `FakeIdentityProvider(PrivateKey, String keyId, String issuer, String audience, String subject, List<String> grants, String clientId, String clientSecret, long ttlSeconds)`, with `tokenEndpoint()`, `issued()`, `close()`, and `static PrivateKey loadRsaPkcs8Pem(Path)`
  - `FakeJwtClickHouseServer(PublicKey, String issuer, String audience)`, with `url()`, `rows()`, `rejected()`, `distinctTokens()`, `close()`
  - 1.17 `EmbeddedFlinkClusterForTests.setUp(Configuration)`

Background: client-v2 0.10.0 treats a `200` with no body as a successful insert, and maps `X-ClickHouse-Exception-Code` to `ServerException`. It sends nothing besides the `INSERT`. STRING mode appends `\n` to each record.

- [ ] **Step 1: Add the 1.17 `setUp(Configuration)` overload**

In `flink-connector-clickhouse-1.17/.../test/embedded/flink/EmbeddedFlinkClusterForTests.java`, replace the existing `setUp()`:

```java
    public static void setUp() throws Exception {
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, REST_PORT); // web UI port (optional)
        config.set(TaskManagerOptions.NUM_TASK_SLOTS, NUM_TASK_SLOTS);
        flinkCluster = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(NUM_TASK_SLOTS_PER_TASK)
                        .setNumberTaskManagers(NUM_TASK_MANAGERS)
                        .setConfiguration(config)
                        .build());
        flinkCluster.before();
    }
```

with:

```java
    public static void setUp() throws Exception {
        Configuration config = new Configuration();
        setUp(config);
    }

    public static void setUp(Configuration config) throws Exception {
        config.set(RestOptions.PORT, REST_PORT); // web UI port (optional)
        config.set(TaskManagerOptions.NUM_TASK_SLOTS, NUM_TASK_SLOTS);
        flinkCluster = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(NUM_TASK_SLOTS_PER_TASK)
                        .setNumberTaskManagers(NUM_TASK_MANAGERS)
                        .setConfiguration(config)
                        .build());
        flinkCluster.before();
    }
```

Run: `./gradlew :flink-connector-clickhouse-1.17:compileTestJava`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 2: Create `FakeIdentityProvider.java` (2.0.0)**

```java
package org.apache.flink.connector.clickhouse.sink;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
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

    /** Loads an RSA private key from a PKCS#8 PEM file ({@code -----BEGIN PRIVATE KEY-----}). */
    static PrivateKey loadRsaPkcs8Pem(Path file) throws IOException, GeneralSecurityException {
        String pem = Files.readString(file, StandardCharsets.US_ASCII)
                .replaceAll("-----(BEGIN|END) PRIVATE KEY-----", "").replaceAll("\\s", "");
        return KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(pem)));
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
```

- [ ] **Step 3: Create `FakeJwtClickHouseServer.java` (2.0.0)**

```java
package org.apache.flink.connector.clickhouse.sink;

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
```

- [ ] **Step 4: Write the end-to-end test (2.0.0)**

`ClickHouseDelegationTokenTests.java`:

```java
package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentialsHolder;
import org.apache.flink.connector.test.embedded.flink.EmbeddedFlinkClusterForTests;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.clickhouse.sink.ClickHouseSinkTestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * OAuth client-credentials end to end without Cloud: the provider fetches 20s JWTs from a fake identity provider
 * inside the MiniCluster, a JWT-verifying fake ClickHouse accepts inserts, and the sink must write
 * every row across several rotations with zero rejected requests.
 */
class ClickHouseDelegationTokenTests {

    private static final String PREFIX = "security.delegation.token.provider.clickhouse.";
    private static final String ISSUER = "https://idp.test";
    private static final String AUDIENCE = "fake-clickhouse";
    private static final int ROWS = 700;

    private static FakeIdentityProvider idp;
    private static FakeJwtClickHouseServer clickhouse;
    private static boolean clusterStarted;

    private static void put(Configuration conf, String key, String value) {
        conf.set(ConfigOptions.key(key).stringType().noDefaultValue(), value);
    }

    @BeforeAll
    static void setUp() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        KeyPair keyPair = gen.generateKeyPair();
        idp = new FakeIdentityProvider(keyPair.getPrivate(), "test-key", ISSUER, AUDIENCE, "flink-it",
                Collections.singletonList("INSERT ON dt.events"), "flink", "s&cr3t", 20);
        clickhouse = new FakeJwtClickHouseServer(keyPair.getPublic(), ISSUER, AUDIENCE);

        // The provider runs inside the MiniCluster and is called at cluster start.
        Configuration conf = new Configuration();
        put(conf, PREFIX + "oauth.token-endpoint", idp.tokenEndpoint());
        put(conf, PREFIX + "oauth.client-id", "flink");
        put(conf, PREFIX + "oauth.client-secret", "s&cr3t");
        put(conf, "security.delegation.tokens.renewal.retry.backoff", "1s");
        EmbeddedFlinkClusterForTests.setUp(conf);
        clusterStarted = true;
    }

    @AfterAll
    static void tearDown() {
        // Only stop the cluster this class started: a failed setUp must not stop another class's cluster twice.
        if (clusterStarted) {
            EmbeddedFlinkClusterForTests.tearDown();
            clusterStarted = false;
        }
        ClickHouseCredentialsHolder.reset();
        if (clickhouse != null) {
            clickhouse.close();
        }
        if (idp != null) {
            idp.close();
        }
    }

    @Test
    void sinkKeepsWritingAcrossJwtRotations() throws Exception {
        StreamExecutionEnvironment env = EmbeddedFlinkClusterForTests.getMiniCluster().getTestStreamEnvironment();
        env.setParallelism(1);

        ClickHouseAsyncSink<String> sink = ClickHouseAsyncSink.<String>builder()
                .setElementConverter(new ClickHouseConvertor<>(String.class))
                .setMaxBatchSize(50)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(1000)
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(ClickHouseClientConfig.forDelegationToken(clickhouse.url(), "dt", "events"))
                .setClickHouseFormat(ClickHouseFormat.CSV)
                .build();

        // ~70s of steady writes: with 20s tokens Flink re-obtains about every 15s.
        env.fromSequence(1, ROWS)
                .map(i -> {
                    Thread.sleep(100);
                    return i + ",row-" + i;
                })
                .returns(Types.STRING)
                .sinkTo(sink);

        JobClient job = env.executeAsync("delegation-token-oauth");
        // Bounded source: the job FINISHES; a fatal auth failure (STOP_FLINK) would fail it here.
        job.getJobExecutionResult().get(180, TimeUnit.SECONDS);

        assertEquals(ROWS, clickhouse.rows());
        assertEquals(0, clickhouse.rejected(), "no request may be sent with an invalid or expired JWT");
        assertTrue(idp.issued() >= 3, "the provider must have re-obtained tokens, identity provider issued: " + idp.issued());
        assertTrue(clickhouse.distinctTokens() >= 3,
                "expected at least 2 rotations reaching ClickHouse, server saw JWTs: " + clickhouse.distinctTokens());
    }
}
```

- [ ] **Step 5: Run it (2.0.0)**

Run: `./gradlew :flink-connector-clickhouse-2.0.0:test --tests 'org.apache.flink.connector.clickhouse.sink.ClickHouseDelegationTokenTests'`
Expected: 1 test PASS in about 80–100 s. The log shows `Obtained ClickHouse JWT` at least 3 times and `Applied re-obtained ClickHouse delegation token` at least twice. No Docker needed.

If Flink rejects the lambda's type information, `.returns(Types.STRING)` is already present; replace the lambda with an anonymous `MapFunction<Long, String>`.

- [ ] **Step 6: Copy the three files to 1.17 and run**

Run: `cp flink-connector-clickhouse-2.0.0/src/test/java/org/apache/flink/connector/clickhouse/sink/{FakeIdentityProvider,FakeJwtClickHouseServer,ClickHouseDelegationTokenTests}.java flink-connector-clickhouse-1.17/src/test/java/org/apache/flink/connector/clickhouse/sink/`
Run: `./gradlew :flink-connector-clickhouse-1.17:test --tests 'org.apache.flink.connector.clickhouse.sink.ClickHouseDelegationTokenTests'`
Expected: 1 test PASS.

- [ ] **Step 7: Checkpoint**

Stop. Report the observed rotation count and duration. Do not commit.

---

### Task 10: Cloud-gated end-to-end (both modules)

**Files:**
- Test: `flink-connector-clickhouse-{2.0.0,1.17}/src/test/java/org/apache/flink/connector/clickhouse/sink/ClickHouseDelegationTokenCloudFakeIdpTests.java`

**Interfaces:**
- Consumes: `FakeIdentityProvider` (Task 9), `ClickHouseTestHelpers.getClient(host, port, ssl, user, password)`, `EmbeddedFlinkClusterForTests`.
- Environment variables:
  - `CLICKHOUSE_CLOUD_HOST`, `CLICKHOUSE_CLOUD_PASSWORD` (admin `default`, for setup)
  - `CLICKHOUSE_CLOUD_JWT_KEY_FILE`: an RSA PKCS#8 PEM whose public JWKS is registered in the service's JWT provider
  - `CLICKHOUSE_CLOUD_JWT_KEY_ID`, `CLICKHOUSE_CLOUD_JWT_ISSUER`, `CLICKHOUSE_CLOUD_JWT_AUDIENCE`
  - Cloud fetches only the JWKS. The token endpoint is the local `FakeIdentityProvider`, so this exercises our real OAuth code against Cloud's real JWT validation.
  - Candidate: clickhouse-java's CI key `JWT_PKEY` plus `.github/jwks.json`. Convert the key with `openssl pkcs8 -topk8 -nocrypt` if it isn't PKCS#8.
- Produces: nothing.

- [ ] **Step 1: Write the test (2.0.0)**

`ClickHouseDelegationTokenCloudFakeIdpTests.java`:

```java
package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentialsHolder;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseTestHelpers;
import org.apache.flink.connector.test.embedded.flink.EmbeddedFlinkClusterForTests;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.clickhouse.sink.ClickHouseSinkTestUtils.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * OAuth client-credentials path against a ClickHouse Cloud Enterprise service: tokens come from a local fake identity
 * provider signing with a key whose JWKS is registered on the service. Skipped unless the env vars are set.
 */
class ClickHouseDelegationTokenCloudFakeIdpTests {

    private static final String PREFIX = "security.delegation.token.provider.clickhouse.";
    private static final int ROWS = 1000;
    private static final int HTTPS_PORT = 8443;

    private static String host;
    private static String database;
    private static FakeIdentityProvider idp;
    private static boolean clusterStarted;

    private static String env(String name) {
        String v = System.getenv(name);
        return v == null || v.trim().isEmpty() ? null : v.trim();
    }

    private static void put(Configuration conf, String key, String value) {
        conf.set(ConfigOptions.key(key).stringType().noDefaultValue(), value);
    }

    private static Client admin() {
        return ClickHouseTestHelpers.getClient(host, HTTPS_PORT, true, "default", env("CLICKHOUSE_CLOUD_PASSWORD"));
    }

    private static void sql(String statement) throws Exception {
        admin().execute(statement).get().close();
    }

    @BeforeAll
    static void setUp() throws Exception {
        for (String name : new String[] {"CLICKHOUSE_CLOUD_HOST", "CLICKHOUSE_CLOUD_PASSWORD", "CLICKHOUSE_CLOUD_JWT_KEY_FILE",
                "CLICKHOUSE_CLOUD_JWT_KEY_ID", "CLICKHOUSE_CLOUD_JWT_ISSUER", "CLICKHOUSE_CLOUD_JWT_AUDIENCE"}) {
            assumeTrue(env(name) != null, name + " not set; skipping Cloud JWT test");
        }
        host = env("CLICKHOUSE_CLOUD_HOST");
        database = "flink_dt_jwt_" + System.currentTimeMillis();
        sql("CREATE DATABASE IF NOT EXISTS `" + database + "`");
        sql("CREATE TABLE `" + database + "`.events (id UInt64, s String) ENGINE = MergeTree ORDER BY id");

        idp = new FakeIdentityProvider(
                FakeIdentityProvider.loadRsaPkcs8Pem(Paths.get(env("CLICKHOUSE_CLOUD_JWT_KEY_FILE"))),
                env("CLICKHOUSE_CLOUD_JWT_KEY_ID"), env("CLICKHOUSE_CLOUD_JWT_ISSUER"), env("CLICKHOUSE_CLOUD_JWT_AUDIENCE"),
                "flink-connector-it", Collections.singletonList("INSERT ON `" + database + "`.events"),
                "flink", "it-secret", 40);

        Configuration conf = new Configuration();
        put(conf, PREFIX + "oauth.token-endpoint", idp.tokenEndpoint());
        put(conf, PREFIX + "oauth.client-id", "flink");
        put(conf, PREFIX + "oauth.client-secret", "it-secret");
        put(conf, "security.delegation.tokens.renewal.retry.backoff", "2s");
        EmbeddedFlinkClusterForTests.setUp(conf);
        clusterStarted = true;
    }

    @AfterAll
    static void tearDown() throws Exception {
        // Only stop the cluster this class started: a skipped setUp must not stop another class's cluster twice.
        if (clusterStarted) {
            EmbeddedFlinkClusterForTests.tearDown();
            clusterStarted = false;
        }
        ClickHouseCredentialsHolder.reset();
        if (idp != null) {
            idp.close();
        }
        if (database != null) {
            sql("DROP DATABASE IF EXISTS `" + database + "`");
        }
    }

    @Test
    void sinkKeepsWritingAcrossJwtRotations() throws Exception {
        StreamExecutionEnvironment env = EmbeddedFlinkClusterForTests.getMiniCluster().getTestStreamEnvironment();
        env.setParallelism(1);

        ClickHouseAsyncSink<String> sink = ClickHouseAsyncSink.<String>builder()
                .setElementConverter(new ClickHouseConvertor<>(String.class))
                .setMaxBatchSize(50)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(1000)
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(ClickHouseClientConfig.forDelegationToken(
                        "https://" + host + ":" + HTTPS_PORT + "/", database, "events"))
                .setClickHouseFormat(ClickHouseFormat.CSV)
                .build();

        // ~100s of writes: with 40s tokens Flink re-obtains about every 30s.
        env.fromSequence(1, ROWS)
                .map(i -> {
                    Thread.sleep(100);
                    return i + ",row-" + i;
                })
                .returns(Types.STRING)
                .sinkTo(sink);

        JobClient job = env.executeAsync("delegation-token-oauth-cloud");
        job.getJobExecutionResult().get(240, TimeUnit.SECONDS);

        long rows = admin().queryAll("SELECT count() FROM `" + database + "`.events").get(0).getLong(1);
        assertEquals(ROWS, rows);
        assertTrue(idp.issued() >= 3, "expected at least 2 rotations, identity provider issued: " + idp.issued());
    }
}
```

Copy it to 1.17:

Run: `cp flink-connector-clickhouse-2.0.0/src/test/java/org/apache/flink/connector/clickhouse/sink/ClickHouseDelegationTokenCloudFakeIdpTests.java flink-connector-clickhouse-1.17/src/test/java/org/apache/flink/connector/clickhouse/sink/`

- [ ] **Step 2: Verify that it skips cleanly without Cloud env vars**

Run: `env -u CLICKHOUSE_CLOUD_HOST ./gradlew :flink-connector-clickhouse-2.0.0:test --tests 'org.apache.flink.connector.clickhouse.sink.ClickHouseDelegationTokenCloudFakeIdpTests'`
Expected: `BUILD SUCCESSFUL`, test SKIPPED.

If `getLong` doesn't exist on the query result record, use `getBigInteger(1).longValue()`.

- [ ] **Step 3: Run it against Cloud if the user provides the env vars**

Run: `./gradlew :flink-connector-clickhouse-2.0.0:test --tests 'org.apache.flink.connector.clickhouse.sink.ClickHouseDelegationTokenCloudFakeIdpTests'`
Expected: 1 test PASS. Otherwise report "not run (no Cloud JWT environment)". Never report it as passing. On an auth error, record the exact server code (spec open question 1).

- [ ] **Step 4: Checkpoint**

Stop. Report. Do not commit.

---

### Task 11: Documentation and spec sync

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`
- Modify: `docs/superpowers/specs/2026-10-03-delegation-tokens-design.md` (status line)

- [ ] **Step 1: Append the user guide to `README.md`**

Insert before a `## Contributing` / `## License` heading if one exists, otherwise at the end:

````markdown
## Delegation tokens (short-lived JWTs for ClickHouse Cloud)

Instead of putting a long-lived password into job code, the sink can authenticate to ClickHouse Cloud with
short-lived JWTs obtained by [Flink's delegation token framework](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/security/security-delegation-token/).
The JobManager holds your identity provider's client id and secret, requests an access token (OAuth2
client-credentials grant), and Flink distributes and re-obtains it on every TaskManager.

Requirements: ClickHouse Cloud **Enterprise**, service **26.4+**, and an identity provider (Entra ID, Okta,
Auth0, Keycloak, …) with a machine-to-machine (client-credentials) app.

1. **Identity provider:** create a client-credentials app. Its access token must be a JWT with a `kid` header,
   RS256 (or ES256/384/512 on 26.8+), and the claims `exp`, `iat`, `iss`, `sub`, `aud` (`aud` a single string),
   plus the ClickHouse roles in a claim (e.g. Entra app roles in `roles`).
2. **Cloud console** (*Settings → Security → JWT authentication*, see
   [JWT authentication setup](https://clickhouse.com/docs/products/cloud/guides/security/cloud-access-management/jwt-authentication-setup)):

   | Field | Value |
   |---|---|
   | Issuer | the token's `iss` (Entra v2: `https://login.microsoftonline.com/<tenant>/v2.0`) |
   | Audience | the token's `aud` |
   | JWKS URL | your identity provider's keys URL (e.g. `…/discovery/v2.0/keys`, `…/.well-known/jwks.json`) |
   | Roles claim | the claim holding roles (e.g. `roles`) |

   The roles must exist on the service. Rights are `permission limit (the default user) ∩ roles/grants`;
   anything beyond the `default` user's rights is silently dropped.
3. **Flink:** copy `flink-connector-clickhouse-token-<version>.jar` into `lib/` on **every** JobManager and
   TaskManager (same version everywhere), and configure:

   ```yaml
   security.delegation.token.provider.clickhouse.oauth.token-endpoint: https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token
   security.delegation.token.provider.clickhouse.oauth.client-id: <client-id>
   security.delegation.token.provider.clickhouse.oauth.client-secret: <secret>
   security.delegation.token.provider.clickhouse.oauth.scope: api://<client-id>/.default   # optional
   security.delegation.token.provider.clickhouse.oauth.audience: <api identifier>          # optional (Auth0-style)
   security.delegation.tokens.renewal.retry.backoff: 2min
   security.ssl.internal.enabled: true
   ```
4. **Job code:** `ClickHouseClientConfig.forDelegationToken("https://<service>:8443/", database, table)`.
   No username or password. Plain `http://` is rejected.

Notes:
- Only the JobManager needs the client secret, and only the JobManager talks to the identity provider. TaskManagers
  only receive short-lived JWTs.
- The JobManager log warns if the identity provider's token is missing something ClickHouse Cloud requires.
- Tokens apply to the whole Flink cluster: on a session cluster every job shares the same ClickHouse identity.
- Flink retries a failed token request after `security.delegation.tokens.renewal.retry.backoff` (default 1h),
  which is too long for 1h tokens, so set it to a few minutes (the provider logs a warning otherwise).
- Keep `org.apache.flink.` in `classloader.parent-first-patterns.default` (Flink's default).
- Self-managed / OSS ClickHouse doesn't support JWT; keep using username/password there.
````

- [ ] **Step 2: Update `CLAUDE.md`**

(a) In "Module Structure", change "Four Gradle modules" to "Five Gradle modules" and add after the base bullet:

```markdown
- **flink-connector-clickhouse-token** - Flink delegation token provider/receiver: OAuth 2.0 client-credentials → ClickHouse Cloud JWT (`org.apache.flink.connector.clickhouse.token`). Deployed to Flink's `lib/`, depends only on `flink-core` (compileOnly 1.17.2, `--release 11`). The sink modules depend on it `compileOnly` and must never bundle it in `shadowJar`.
```

(b) Under the base module's key classes, add:

```markdown
- `org.apache.flink.connector.clickhouse.auth.DelegationTokenBridge` — the only sink class touching the token module; `ClickHouseClientConfig.forDelegationToken(...)` + `refreshCredentialsIfChanged()` pull the JWT from `ClickHouseCredentialsHolder` before every batch (`client.updateBearerToken`). In token mode `Utils.handleException(e, true)` treats 516/720 as retriable.
```

- [ ] **Step 3: Update the spec status**

Change the spec's `**Status:**` line to:

```markdown
**Status:** Approved (revision 3: OAuth 2.0 client credentials only); implemented per docs/superpowers/plans/2026-10-04-delegation-tokens.md
```

- [ ] **Step 4: Full verification**

Run: `./gradlew :flink-connector-clickhouse-token:test :flink-connector-clickhouse-base:test`
Expected: PASS.
Run: `./gradlew :flink-connector-clickhouse-1.17:test --tests '*DelegationToken*' :flink-connector-clickhouse-2.0.0:test --tests '*DelegationToken*'`
Expected: PASS (the Cloud test is SKIPPED without env vars).
Run (Docker): `./gradlew :flink-connector-clickhouse-1.17:test :flink-connector-clickhouse-2.0.0:test`
Expected: PASS. Report honestly if Docker isn't available.

- [ ] **Step 5: Checkpoint**

Stop. Report the final state, with every changed file listed. Do not commit. The user decides the PR split: the spec suggests (1) Task 1, (2) Tasks 2–9 and 11, and (3) Task 10.
