package org.apache.flink.connector.clickhouse.token;

import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.util.TimeUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/** Typed, validated view of {@code security.delegation.token.provider.clickhouse.*}. */
public final class ProviderConfig {

    public static final String PREFIX = "security.delegation.token.provider.clickhouse.";
    static final String RENEWAL_RATIO_KEY = "security.delegation.tokens.renewal.time-ratio";
    /** Hosts allowed over plain http:// without opting in. Literal match only: no DNS lookup. */
    private static final List<String> LOOPBACK_HOSTS = Arrays.asList("localhost", "127.0.0.1", "[::1]");

    private boolean active;
    private URI tokenEndpoint;
    private String clientId;
    private String clientSecret;
    private String scope;
    private String audience;
    private boolean allowInsecureHttp;
    private Duration connectTimeout;
    private Duration requestTimeout;
    private Duration retryInterval;
    private double renewalRatio;

    private ProviderConfig() {}

    public static ProviderConfig parse(Map<String, String> conf) {
        ProviderConfig c = new ProviderConfig();
        c.renewalRatio = conf.containsKey(RENEWAL_RATIO_KEY)
                ? Double.parseDouble(conf.get(RENEWAL_RATIO_KEY)) : 0.75;

        String endpoint = optional(conf, "oauth.token-endpoint");
        if (endpoint == null) {
            c.active = false;
            return c;
        }
        c.active = true;
        c.allowInsecureHttp = bool(conf, "oauth.allow-insecure-http");
        c.tokenEndpoint = tokenEndpointUri("oauth.token-endpoint", endpoint, c.allowInsecureHttp);
        c.clientId = required(conf, "oauth.client-id");
        c.clientSecret = required(conf, "oauth.client-secret");
        c.scope = optional(conf, "oauth.scope");
        c.audience = optional(conf, "oauth.audience");
        c.connectTimeout = positiveDuration(conf, "oauth.connect-timeout", Duration.ofSeconds(10));
        c.requestTimeout = positiveDuration(conf, "oauth.request-timeout", Duration.ofSeconds(30));
        c.retryInterval = positiveDuration(conf, "oauth.retry-interval", Duration.ofSeconds(60));
        return c;
    }

    /**
     * A WARN message, logged at every startup, if {@code oauth.allow-insecure-http} let a plain
     * http:// endpoint on a non-loopback host through; empty otherwise.
     */
    public Optional<String> insecureEndpointWarning() {
        if (!active || !"http".equalsIgnoreCase(tokenEndpoint.getScheme()) || isLoopback(tokenEndpoint)) {
            return Optional.empty();
        }
        return Optional.of("The OAuth token endpoint " + displayable(tokenEndpoint) + " uses plain http:// ("
                + PREFIX + "oauth.allow-insecure-http=true): the client secret and the issued ClickHouse JWTs "
                + "travel unencrypted. Use https:// outside of isolated test environments.");
    }

    /** scheme://host[:port]/path: safe for logs and exception messages (no userinfo, query or fragment). */
    public static String displayable(URI uri) {
        StringBuilder sb = new StringBuilder();
        sb.append(uri.getScheme()).append("://").append(uri.getHost());
        if (uri.getPort() != -1) {
            sb.append(':').append(uri.getPort());
        }
        if (uri.getRawPath() != null) {
            sb.append(uri.getRawPath());
        }
        return sb.toString();
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

    private static Duration positiveDuration(Map<String, String> conf, String key, Duration defaultValue) {
        String value = optional(conf, key);
        if (value == null) {
            return defaultValue;
        }
        Duration duration;
        try {
            duration = TimeUtils.parseDuration(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalConfigurationException(PREFIX + key + " is not a valid duration (e.g. 10s, 2min): '"
                    + value + "'", e);
        }
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalConfigurationException(PREFIX + key + " must be positive, got '" + value + "'");
        }
        return duration;
    }

    private static boolean bool(Map<String, String> conf, String key) {
        String value = optional(conf, key);
        if (value == null || value.equalsIgnoreCase("false")) {
            return false;
        }
        if (value.equalsIgnoreCase("true")) {
            return true;
        }
        throw new IllegalConfigurationException(PREFIX + key + " must be true or false, got '" + value + "'");
    }

    /**
     * Validates the token endpoint. The raw value is never echoed: it may carry credentials.
     * https:// is required, except plain http:// to a loopback host (local testing) or with
     * {@code oauth.allow-insecure-http=true}.
     */
    private static URI tokenEndpointUri(String key, String value, boolean allowInsecureHttp) {
        URI uri;
        try {
            uri = new URI(value);
        } catch (URISyntaxException e) {
            throw new IllegalConfigurationException(PREFIX + key + " is not a valid URI: " + e.getReason()
                    + " at index " + e.getIndex());
        }
        String scheme = uri.getScheme();
        if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
            throw new IllegalConfigurationException(PREFIX + key + " must be an https:// URL");
        }
        if (uri.getRawUserInfo() != null) {
            throw new IllegalConfigurationException(PREFIX + key + " must not contain credentials (user:password@); "
                    + "set them in " + PREFIX + "oauth.client-id and " + PREFIX + "oauth.client-secret");
        }
        if (uri.isOpaque() || uri.getHost() == null) {
            throw new IllegalConfigurationException(PREFIX + key
                    + " must be an absolute URL with a host, e.g. https://idp.example.com/oauth2/token");
        }
        if (uri.getRawFragment() != null) {
            throw new IllegalConfigurationException(PREFIX + key + " must not contain a fragment (#...)");
        }
        if ("http".equalsIgnoreCase(scheme) && !isLoopback(uri) && !allowInsecureHttp) {
            throw new IllegalConfigurationException(PREFIX + key + " must use https:// (got " + displayable(uri)
                    + "): the client secret is sent to it in the request body. Plain http:// is accepted only for "
                    + "localhost/127.0.0.1/[::1], or when " + PREFIX + "oauth.allow-insecure-http is set to true.");
        }
        return uri;
    }

    private static boolean isLoopback(URI uri) {
        String host = uri.getHost();
        return host != null && LOOPBACK_HOSTS.contains(host.toLowerCase(Locale.ROOT));
    }

    public boolean isActive() { return active; }
    public URI getTokenEndpoint() { return tokenEndpoint; }
    public String getClientId() { return clientId; }
    public String getClientSecret() { return clientSecret; }
    public String getScope() { return scope; }
    public String getAudience() { return audience; }
    public boolean isAllowInsecureHttp() { return allowInsecureHttp; }
    public Duration getConnectTimeout() { return connectTimeout; }
    public Duration getRequestTimeout() { return requestTimeout; }
    public Duration getRetryInterval() { return retryInterval; }
    public double getRenewalRatio() { return renewalRatio; }
}
