# Flink Delegation Token Support (OAuth 2.0 client credentials → ClickHouse Cloud JWT) Design

**Status:** Approved (revision 3: OAuth 2.0 client credentials only); implemented per docs/superpowers/plans/2026-10-04-delegation-tokens.md
**Date:** 2026-10-03, revised 2026-10-04
**Author:** mark@clickhouse.com (with Claude)

## 1. Motivation

Today the sink authenticates with a username and password written in job code. Every TaskManager receives that long-lived secret, and rotating it means redeploying the job.

Flink 1.17 added a delegation token framework ([FLIP-272](https://cwiki.apache.org/confluence/display/FLINK/FLIP-272%3A+Generalized+delegation+token+support), [docs](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/security/security-delegation-token/)). One component on the JobManager holds the long-lived credential and obtains **short-lived** tokens. Flink sends them to every TaskManager and re-obtains them before they expire.

This design plugs the sink into that framework:

| | |
|---|---|
| Server | ClickHouse Cloud, **Enterprise** plan, service **26.4+**, with a JWT provider configured for the customer's identity provider ([JWT authentication setup](https://clickhouse.com/docs/products/cloud/guides/security/cloud-access-management/jwt-authentication-setup)) |
| Token | The identity provider's access token, a JWT (RS256, or ES256/384/512 on 26.8+), obtained with the **OAuth2 client-credentials grant** |
| Long-lived secret | OAuth client id and secret, held on the **JobManager only** |
| Presented as | `Authorization: Bearer <jwt>` over HTTPS |
| Server identity | Ephemeral user from the token's claims (`sub`, roles/grants), capped by the service's permission limit (`default` user) |

OSS ClickHouse rejects JWT (`"JWT is available only in ClickHouse Cloud"`; `Bearer` returns `516`), so token mode is Cloud-only.

## 2. Non-goals

- **Self-managed/OSS ClickHouse (`CREATE TOKEN`).** Dropped. Those users keep password auth.
- **Self-signed JWTs** (the JobManager signing with its own key). Dropped. Customers need an identity provider with a machine-to-machine (client-credentials) app.
- **Several ClickHouse services or identities per Flink cluster.** One `clickhouse` service, one identity.
- **Per-job credentials on session clusters.** Delegation tokens apply to the whole cluster by design.
- **Interactive flows** (device code, the built-in Cloud `--login` authenticator).
- **Doing the identity-provider exchange inside client-v2.** client-v2 stays a token consumer. The exchange runs on the JobManager only, so the client secret never reaches TaskManagers.
- **A Table API / SQL option.** There's no table module in the repo.

The wire format (§6.3) has a version byte, so other token kinds can be added later.

## 3. Background: what Flink provides

Verified in flink-runtime 2.0.0 sources; the interfaces are identical in 1.17.2 and 2.0.0.

1. **Discovery.** Providers and receivers are found with `ServiceLoader` from the **system classpath** (`lib/`) at **process start**.
2. **Obtain and re-obtain.** On the JobManager, Flink calls `obtainDelegationTokens()`. The next call is scheduled at `validUntil × security.delegation.tokens.renewal.time-ratio` (0.75). A provider exception fails the update for **every** provider and is retried after `security.delegation.tokens.renewal.retry.backoff` (default **1h**), so our provider never throws after `init` (§9). Flink re-creates tokens from the long-lived credential: each round is a new client-credentials request, and no refresh token is involved.
3. **Broadcast.** The ResourceManager RPCs the bytes to every TaskManager. TaskManagers that register later get them in their registration response.
4. **Delivery.** On each TaskManager, the receiver with the matching `serviceName()` gets `onNewTokensObtained(byte[])`. **Flink's responsibility ends here.**

Rules:
- **Unconfigured providers must be silent.** Providers are enabled by default, so an unconfigured provider returns `false` from `delegationTokensRequired()` and must never throw.
- **A provider's `init()` failure stops the JobManager.**
- **A receiver's `onNewTokensObtained()` failure is only logged.**
- **The provider and receiver ship together.**

**Who sends the request to the identity provider:** Flink decides *when* (on the JobManager), and our provider sends the HTTP request (on the JobManager). TaskManagers, the sink and client-v2 never contact the identity provider.

## 4. Prior art

- **Flink S3 filesystems** (`AbstractS3DelegationTokenProvider/Receiver`): a `static volatile` holder that the client pulls from on every request. We copy this.
- **`apache/flink-connector-hive`:** an external connector with its own provider; the jar goes in `lib/`.

## 5. Architecture

```
 JobManager                                           TaskManager (×N)
 ─────────────────────────────────────                ────────────────────────────────────────
 ClickHouseDelegationTokenProvider                     ClickHouseDelegationTokenReceiver
   OAuthClientCredentialsSource                           onNewTokensObtained(bytes):
     POST {token-endpoint} grant_type=client_credentials   ClickHouseCredentialsHolder.set(decode(bytes))
     → access_token (JWT), expires_in                                     ▲
   returns (Codec.encode(jwt), validUntil)                                │ pull, once per batch
            │                                                             │
            └──── Flink: re-obtain at 75%, RPC broadcast ─────►   ClickHouseAsyncWriter.submitRequestEntries
                                                                    clientConfig.refreshCredentialsIfChanged()
                                                                      → client.updateBearerToken(jwt)
```

### 5.1 Artifacts and classloading

- **New module `flink-connector-clickhouse-token`,** deployed to Flink's **`lib/`** on the JobManager and every TaskManager.
  - It depends only on what Flink already ships in `lib/`: `flink-core` and Flink's shaded Jackson (`org.apache.flink:flink-shaded-jackson`, package `org.apache.flink.shaded.jackson2.*`, the same in every supported Flink version), both `compileOnly` and never bundled. A hand-written JSON parser was replaced with it on 2026-10-11.
  - HTTP uses the JDK's `HttpClient`.
  - It compiles with `--release 11`, so one jar serves Flink 1.17 through 2.x.
- **Package `org.apache.flink.connector.clickhouse.token` is required.** Flink's default parent-first pattern `org.apache.flink.` makes the `lib/` copy win over a bundled one.
  - Verified on Flink 1.20.3: 75/75 inserts across 6 rotations.
  - A `com.clickhouse.*` copy was shadowed, and its static stayed `null`.
- **`ClickHouseCredentialsHolder` is Flink-free,** because `-base` has no Flink dependency.
- **The sink modules depend on the token module `compileOnly` and never bundle it.** Only `DelegationTokenBridge` (`-base`) touches token classes, and it is loaded only in token mode. It hands the sink a sink-side `ResolvedCredentials`.

## 6. Token module

### 6.1 Classes

| Class | Responsibility |
|---|---|
| `ClickHouseDelegationTokenProvider` | `serviceName() = "clickhouse"`. Parses config, calls the OAuth source, encodes the result. Never throws after `init`: on failure it re-sends the last good token (or empty bytes) with a short scheduling `validUntil` (§9). |
| `ClickHouseDelegationTokenReceiver` | Decodes and publishes to `ClickHouseCredentialsHolder`. |
| `ClickHouseCredentialsHolder` | Flink-free `static volatile` slot. |
| `ClickHouseCredentials` | `(String jwt, long validUntilMillis)`; redacted `toString()`. |
| `ClickHouseCredentialsCodec` | Versioned encoding (§6.3). |
| `ProviderConfig` | Typed, validated config (§6.2). |
| `OAuthClientCredentialsSource` | The client-credentials request, expiry handling, Cloud-compatibility check (§6.4). |
| `META-INF/services/…` | ServiceLoader registration. |

### 6.2 Configuration

Every key is prefixed with `security.delegation.token.provider.clickhouse.`. Flink masks keys containing `secret`.

| Key | Required | Description |
|---|---|---|
| `oauth.token-endpoint` | **yes, to activate** | The identity provider's OAuth2 token endpoint. Must be `https://`; plain `http://` only to `localhost`/`127.0.0.1`/`[::1]` unless `oauth.allow-insecure-http` is set. If absent or blank, the provider is inactive. |
| `oauth.client-id` | yes | |
| `oauth.client-secret` | yes | Stays on the JobManager. |
| `oauth.scope` | no | e.g. `api://<app>/.default` (Entra). |
| `oauth.audience` | no | Sent as an `audience` form parameter (Auth0-style identity providers). |
| `oauth.allow-insecure-http` | no (default `false`) | `true` accepts a plain `http://` endpoint on any host (e.g. a sidecar or an in-cluster proxy that terminates TLS). The provider logs a WARN at every startup while it is set. |
| `oauth.connect-timeout` | no (default `10s`) | TCP/TLS connect timeout to the token endpoint (`HttpClient.connectTimeout`). Flink duration syntax. |
| `oauth.request-timeout` | no (default `30s`) | Deadline for the whole token request, response body included. |
| `oauth.retry-interval` | no (default `60s`) | After a failed obtain (in-call retries exhausted), how soon Flink calls the provider again. Positive duration. |

How validation behaves:
- **`oauth.token-endpoint` set, but the client id or secret missing:** `init()` throws `IllegalConfigurationException` naming the key.
- **Endpoint rules (`init()` throws, naming the key):** must be an absolute `https://` URL with a host; no `user:password@` userinfo (use the client id/secret keys); no `#fragment`. Plain `http://` is rejected unless the host is literally `localhost`, `127.0.0.1` or `[::1]` (no DNS lookup, so `localhost.evil.com` and `127.0.0.2` are rejected), or `oauth.allow-insecure-http=true`. The rationale: the client secret is sent in the request body, so the default must not depend on one customer's network setup, while an explicit, loudly logged opt-out covers TLS-terminating sidecars.
- **Timeouts:** parsed with Flink's duration syntax (`10s`, `2min`, `500ms`). Must be positive; an unparseable or non-positive value fails `init()` naming the key. `oauth.request-timeout` is enforced as an overall deadline (`sendAsync().get(timeout)`), because `HttpRequest.timeout` stops counting once the response headers arrive (JDK 17 `MultiExchange`). Without that, an identity provider that stalls mid-body would block Flink's renewal thread forever. On timeout the body subscription is cancelled too, since `CompletableFuture.cancel` doesn't abort the exchange before JDK 16. A timeout is a transient failure: it is retried within the same obtain call (§9).
- **`oauth.allow-insecure-http`:** only `true`/`false` (case-insensitive) are accepted; anything else fails naming the key.
- **No secrets in messages:** the raw endpoint value is never echoed (it may carry credentials or an API key in the query). Logs and exception messages use `scheme://host[:port]/path` only (`ProviderConfig.displayable`).
- **Whitespace:** values are trimmed.

### 6.3 Wire format

```
byte version = 1 | int length + UTF-8 jwt | long validUntilMillis
```

There's no Java serialization. An unknown version makes the receiver throw, and the previous token stays in place.

### 6.4 `OAuthClientCredentialsSource`

```
POST {oauth.token-endpoint}
Content-Type: application/x-www-form-urlencoded
grant_type=client_credentials&client_id=…&client_secret=…[&scope=…][&audience=…]
→ {"access_token": "<jwt>", "expires_in": 3600, …}
```

- **Expiry:** `validUntil = now + expires_in`, where `expires_in` may be a number or a numeric string. If it's absent, use the JWT's `exp`, base64url-decoded without verification.
- **Errors:** a non-200 response, a missing `access_token`, or no way to determine the expiry each give an `IOException`. Messages never contain the client secret.
- **Parsing limits:** the response body is capped at 1 MiB, JSON is parsed with Flink's shaded Jackson, invalid or deeply nested JSON is a normal `IOException`, and `expires_in`/`exp` values that do not fit a `long` in milliseconds are rejected as out of range.
- **Cloud-compatibility check.** After each obtain, decode the JWT, without verifying it, and log a WARN listing any problems Cloud would reject:
  - not a JWT;
  - no `kid` header;
  - `alg` not RS256/ES256/ES384/ES512;
  - any of `exp`, `iat`, `iss`, `sub`, `aud` missing;
  - `aud` not a single string.

  This turns an opaque `516` on TaskManagers into an actionable message on the JobManager. It only warns, because Cloud is the authority.

## 7. Sink changes (`-base`, `-1.17`, `-2.0.0`)

### 7.1 Prerequisite: client-v2 0.10.0
In 0.9.5, `updateBearerToken` writes into a plain `HashMap` that concurrent async inserts read. 0.10.0 routes it through the atomic `CredentialsManager`. The rotation probe ran 8 rotations under 8 concurrent threads with 0 failures.

### 7.2 Public API
```java
ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(url, database, tableName);
config.setDelegationTokenWaitTimeout(Duration.ofSeconds(60));   // optional
```

### 7.3 `ClickHouseClientConfig`
- **New fields:** `AuthMode authMode`, `Duration delegationTokenWaitTimeout` (default 60s), `transient ResolvedCredentials appliedCredentials`.
- **`forDelegationToken` requires `https://`.** Cloud's docs say "Always send JWTs over HTTPS". `http://` is allowed only for loopback hosts, which the local test uses.
- **No ping in the constructor:** it runs where the job graph is built, which has no token.
- **`createClient()`:** `await` the first token, then `useBearerTokenAuth(jwt)`.
- **`refreshCredentialsIfChanged()`:** if the bridge returns a new instance, call `client.updateBearerToken(jwt)`.
- **`DelegationTokenBridge`:**
  - `current()` returns the same instance until the holder changes.
  - `await(timeout)` polls every 100 ms.
  - `NoClassDefFoundError` becomes "put flink-connector-clickhouse-token.jar in lib/".
  - The timeout message names the three likely causes.

### 7.4 `ClickHouseAsyncWriter`
```java
Client chClient = this.clickHouseClientConfig.createClient();
this.clickHouseClientConfig.refreshCredentialsIfChanged();
```
Handle errors with `Utils.handleException(error, clickHouseClientConfig.isDelegationTokenMode())`.

### 7.5 Error handling
In token mode, `516` and `720` become `RetriableException`, logged at ERROR. client-v2 reports both as non-retryable, so today the default `STOP_FLINK` strategy would kill the job on any late renewal.

## 8. Deployment

**Identity provider:** create a client-credentials app. Its access token must contain `sub`, a single-string `aud`, `iss`, `iat`, `exp`, a `kid` header, RS256/ES*, and roles in a claim (e.g. Entra app roles in `roles`).

**Cloud console:** add a JWT provider:
- **Issuer:** the identity provider's `iss` (Entra v2: `https://login.microsoftonline.com/<tenant>/v2.0`).
- **Audience:** the token's `aud`.
- **JWKS URL:** the identity provider's keys URL.
- **Roles claim:** e.g. `roles`.

The roles must exist on the service and stay within the `default` user's rights.

**Flink:**
```yaml
security.delegation.token.provider.clickhouse.oauth.token-endpoint: https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token
security.delegation.token.provider.clickhouse.oauth.client-id: <client-id>
security.delegation.token.provider.clickhouse.oauth.client-secret: <secret>
security.delegation.token.provider.clickhouse.oauth.scope: api://<client-id>/.default
security.ssl.internal.enabled: true
```

**Jar and job code:** put the token jar in `lib/` everywhere, and use `ClickHouseClientConfig.forDelegationToken("https://<service>:8443/", db, table)` in the job.

### 8.1 Compatibility with the Cloud JWT setup page

Checked against [JWT authentication setup](https://clickhouse.com/docs/products/cloud/guides/security/cloud-access-management/jwt-authentication-setup) and the [JWT reference](https://clickhouse.com/docs/concepts/features/security/external-authenticators/jwt) on 2026-10-04.

| Cloud requirement | How it's met |
|---|---|
| Enterprise, service 26.4+ (EC 26.8+) | Documented prerequisite |
| The server picks the key by the `kid` header; checks signature, `iss`, `aud` | Identity provider config plus the console form; the provider WARNs if `kid` is missing |
| Required claims `exp`, `iat`, `iss`, `sub`, `aud` (`aud` a string) | Identity provider config; the provider WARNs on any gap (§6.4) |
| Configurable Roles claim | Customer sets it to their identity provider's roles claim; documented |
| Rights = permission limit ∩ (grants ∪ roles) | Documented (excess is silently dropped) |
| Public HTTPS JWKS | Identity providers publish one |
| "Always send JWTs over HTTPS" | `forDelegationToken` rejects `http://` except loopback |
| A newer `iat` re-evaluates claims; an older one reuses the user | Each re-obtain is newer. During the overlap, old-token requests reuse the same user. |
| Ephemeral user GC after `exp` | Rotation at 75% of the lifetime |
| Deleting the provider stops acceptance | The sink retries 516 and logs ERROR |

## 9. Failure modes

- **Token never arrives:** `await` times out with a diagnostic message, and the task restarts.
- **Expiry race:** at 75% of the lifetime there is an overlap. A request that loses the race gets 516/720 and is retried with the new token.
- **Identity provider down.** The JobManager handles this in two layers.
  - **Why throwing is not an option:** Flink 1.17 and 2.0 `DefaultDelegationTokenManager` wrap any provider exception in `FlinkRuntimeException`. That fails the update for **all** providers, and the next attempt waits `renewal.retry.backoff` (1h). A 60-90 minute JWT renewed at 0.75 has only a 15-22 minute margin. The other providers (S3: 12h STS credentials; HDFS/HBase/Hive: renewal every 24h+) have hours of margin, and their client libraries (AWS SDK, Hadoop IPC) retry transient errors inside the call.
  - **Layer 1, retries inside the call** (`OAuthClientCredentialsSource`): up to 4 attempts with 1s/2s/4s backoff for I/O errors, timeouts (connect or request), HTTP 5xx and 429. Other 4xx (e.g. 401 `invalid_client`) and invalid 200 responses are not retried.
  - **Layer 2, never throw** (`ClickHouseDelegationTokenProvider`): if the call still fails, return the last good token's bytes, with its real expiry inside, or empty bytes before the first token. The receiver ignores empty bytes. The returned `validUntil` is `now + oauth.retry-interval / renewal.time-ratio`. It only schedules the next call, so Flink calls again after `oauth.retry-interval` (default 60s). Logging: WARN while the last token is still valid, ERROR once it has expired or if there is none.
  - **Cost:** Flink schedules every provider at the earliest `validUntil`, so during an outage HDFS/S3 tokens are also re-obtained every retry interval.
  - **Effect:** an outage shorter than the margin is invisible to the sink. A longer one makes writes fail with 516 until recovery, which comes within one retry interval.
- **TaskManager side of an outage:**
  - `DelegationTokenBridge.current()` keeps the same `ResolvedCredentials` instance when the JobManager re-sends the same JWT (same `jwt` and `validUntil`). So a re-send triggers no `updateBearerToken` and doesn't reset the auth-failure budget.
  - `await()` treats an expired token as missing. If only an expired one exists at the timeout, it fails with a message naming the expiry time and pointing to the JobManager log. A restart therefore never runs on a token known to be expired.
  - `onAuthFailure` never treats the rejection of an **expired** token as permanent: renewal is late, not misconfigured.
  - **Still open (review issue #1):** those retries are re-queued immediately, so a long outage needs the auth-retry backoff.
- **Bad payload:** the receiver throws, and the previous token stays in place.

## 10. Security considerations

- **Client secret:** only on the JobManager.
- **TaskManagers:** hold only short-lived JWTs.
- **Inside Flink:** JWTs travel over Flink RPC, so recommend `security.ssl.internal.enabled`.
- **To ClickHouse:** HTTPS is enforced.
- **Logs:** never show more than 4 characters of a token, and never the secret.

## 11. Test plan

### 11.1 Unit tests
- **Token module:**
  - codec;
  - OAuth source against a JDK `HttpServer` stub (form encoding with special characters, `expires_in` number/string, `exp` fallback, errors, compatibility check);
  - config (inactive, missing keys, trimming, URL validation, timeouts, retry interval);
  - provider and receiver (inactive, encoded result, failed init, holder, bad payload, ServiceLoader).
- **`-base`:** bridge semantics and messages; `Utils` 516/720 mapping.
- **Versioned modules:** `ClickHouseClientConfig` (HTTPS rule, no ping, token at build, in-place swap, await timeout, serialization with no secret in the bytes).

### 11.2 Local end-to-end (always run, both modules)
- **Setup:** a MiniCluster with the provider pointed at a **`FakeIdentityProvider`**. It's a JDK `HttpServer` implementing client-credentials, issuing RS256 JWTs with `kid`, `expires_in=20`.
- **Sink target:** a **`FakeJwtClickHouseServer`** that verifies each Bearer JWT's signature, `exp`, `iss` and `aud`, answers `200` and counts rows, and answers `403` with `X-ClickHouse-Exception-Code: 516` otherwise.
- **Pass criteria:** all rows arrive, at least 3 distinct JWTs are seen, and 0 requests are rejected.
- **Grounding:** client-v2 0.10.0 accepts a `200` with no summary and maps the exception header to `ServerException`. It sends nothing besides the `INSERT`.

### 11.3 Cloud end-to-end with a real identity provider (env-gated, both modules)
- **Test:** `ClickHouseDelegationTokenCloudRealIdpTests` (package `org.apache.flink.connector.clickhouse.sink.auth`). No fakes: the provider requests tokens from a real client-credentials identity provider, and the sink writes to a real ClickHouse Cloud service that validates them.
- **Setup:** the same Cloud configuration as every other Cloud test (`CLICKHOUSE_VERSION=cloud`, `CLICKHOUSE_CLOUD_HOST`, `CLICKHOUSE_CLOUD_PASSWORD`, via `ClickHouseServerForTests`). The admin login creates the per-run database `flink_connector_test_<timestamp>` with a `flink_dt` table, and drops it at the end. The sink and the row count use only the identity provider's JWT, so the service's JWT provider must grant the token `INSERT` and `SELECT` on `flink_connector_test_*.*`, through a `clickhouse:grants` claim or a role in `clickhouse:roles`.
- **Rotations:** the test requests one token up front to learn `expires_in`, then sets the renewal ratio so Flink re-obtains about every 40 s, whatever lifetime the identity provider issues.
- **Pass criteria:** the job finishes, the per-run table holds exactly the 400 rows written, and at least 2 distinct JWTs were observed.
- **Verified:** Auth0 + ClickHouse Cloud 26.6.1 on Flink 1.17.2 and 2.0.0 (24 h and 60 s token lifetimes).
- **Skipped** unless in Cloud mode and `CLICKHOUSE_CLOUD_OAUTH_TOKEN_ENDPOINT`, `_CLIENT_ID` and `_CLIENT_SECRET` are set. `_AUDIENCE` and `_SCOPE` are optional.
- A variant with a fake identity provider signing for a JWKS registered on Cloud was dropped on 2026-10-11: it needs a Cloud admin login plus a registered private key, and the real-identity-provider test covers Cloud more realistically.

### 11.4 Real Flink cluster with a real identity provider (env-gated, integration module)
- **Test:** `DelegationTokenClusterTests` in `flink-connector-clickhouse-integration`.
- **Cluster:** a Docker JobManager and 2 TaskManagers from the official `flink:<FLINK_VERSION>` image (`Cluster.Builder.withLibJar` / `withFlinkProperty`). The token jar goes into `/opt/flink/lib/`. The job is `examples/maven/flink-v{1.7,v2}/delegation-token`, a fat jar with the sink but **no** token classes, submitted through the REST API like the other example tests.
- **Why it exists:** a MiniCluster can't show `ServiceLoader` discovery from `lib/`, parent-first classloading between `lib/` and the user-code classloader, or RPC delivery to TaskManagers in separate JVMs. In a MiniCluster the JobManager-side receiver fills the shared static in the same JVM.
- **Pass criteria:**
  - the job finishes;
  - the per-run table holds all 240 rows;
  - the JobManager logs `Obtained ClickHouse JWT` at least twice;
  - **every TaskManager container** logs `Received ClickHouse JWT`.
- **Configuration and skipping:** same as §11.3. CI runs it in `integration-tests-cloud.yaml` across the Flink matrix once the `FLINK_DT_*` identity-provider secrets exist; it's skipped until then.
- **Verified (2026-10-15):** Auth0 + ClickHouse Cloud 26.6.1 on standalone Docker clusters with `flink:latest` (2.x, Java 17) and `flink:1.17.2` (Java 11). Against the per-run database, the first run failed only with `ACCESS_DENIED` on `flink_connector_test_*`: the JWT was delivered and **authenticated**, but the service's claim granted only `default.*`. A local run pointed at `default` passed every assertion. This also confirms that Flink's delegation-token manager runs on a standalone cluster.

### 11.5 Manual (release checklist)
- A real identity provider (Entra or Okta) client-credentials app against Cloud.
- Standalone Flink 1.20.x and 2.x clusters with the jar in `lib/`.

## 12. Rollout

1. **PR 1:** client-v2 → 0.10.0.
2. **PR 2:** the token module, sink changes, unit and local end-to-end tests, docs.
3. **PR 3** (optional): the Cloud-gated test.

## 13. Open questions

1. **Which error codes does Cloud return** for an expired JWT, a wrong `aud` and an unknown `kid`? Expected 516, possibly 720; the Cloud test will show.
2. ~~Can we reuse clickhouse-java's Cloud service and `JWT_PKEY` for the Cloud test?~~ **Resolved:** the fake-identity-provider Cloud test was dropped; the Cloud test uses a real identity provider (§11.3).
3. **Is Enterprise-only, OAuth-client-credentials-only acceptable** as the only token-mode path?
4. **Does Cloud tolerate a slightly-future `iat`** (identity provider clock ahead)? Identity providers normally handle this; it's in the docs as a troubleshooting hint.

## 14. File-level change summary

| File | Change |
|---|---|
| `settings.gradle.kts` | Include `flink-connector-clickhouse-token` |
| `flink-connector-clickhouse-token/**` | New module (§6) |
| `build.gradle.kts` | client-v2 → 0.10.0 |
| `-{base,1.17,2.0.0}/build.gradle.kts` | `compileOnly` + `testImplementation` on the token module |
| `-base/.../auth/{AuthMode,ResolvedCredentials,DelegationTokenBridge}.java` | New |
| `-base/.../utils/Utils.java` | `handleException(Throwable, boolean)` |
| `-{1.17,2.0.0}/.../ClickHouseClientConfig.java`, `ClickHouseAsyncWriter.java` | Token mode |
| `-1.17/.../EmbeddedFlinkClusterForTests.java` | `setUp(Configuration)` overload |
| `-{1.17,2.0.0}` tests | `ClickHouseClientConfigDelegationTokenTests`, `FakeIdentityProvider`, `FakeJwtClickHouseServer`, `ClickHouseDelegationTokenTests`, `ClickHouseDelegationTokenCloudRealIdpTests` (the last three in `sink.auth`) |
| `README.md`, `CLAUDE.md` | Docs |
