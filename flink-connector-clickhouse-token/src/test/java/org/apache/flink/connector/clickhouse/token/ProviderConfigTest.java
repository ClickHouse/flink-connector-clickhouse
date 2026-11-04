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

    private static ProviderConfig withEndpoint(String endpoint) {
        Map<String, String> m = oauth();
        m.put(PREFIX + "oauth.token-endpoint", endpoint);
        return ProviderConfig.parse(m);
    }

    private static IllegalConfigurationException rejected(String endpoint) {
        return assertThrows(IllegalConfigurationException.class, () -> withEndpoint(endpoint));
    }

    @Test
    void httpsEndpointsAreAccepted() {
        assertEquals("idp.example.com", withEndpoint("https://idp.example.com/oauth2/token").getTokenEndpoint().getHost());
        assertEquals(8443, withEndpoint("https://idp.example.com:8443/token?tenant=x").getTokenEndpoint().getPort());
        assertEquals("IDP.EXAMPLE.COM", withEndpoint("HTTPS://IDP.EXAMPLE.COM/token").getTokenEndpoint().getHost());
    }

    @Test
    void plainHttpToRemoteHostIsRejectedByDefault() {
        IllegalConfigurationException e = rejected("http://idp.example.com/oauth2/token");
        assertTrue(e.getMessage().contains("https://"), e.getMessage());
        assertTrue(e.getMessage().contains("client secret"), e.getMessage());
        assertTrue(e.getMessage().contains("oauth.allow-insecure-http"), e.getMessage());
    }

    @Test
    void plainHttpToLoopbackIsAccepted() {
        for (String endpoint : new String[] {"http://localhost:8080/token", "http://127.0.0.1/token", "http://[::1]:9000/token"}) {
            assertNotNull(withEndpoint(endpoint).getTokenEndpoint(), endpoint);
        }
    }

    @Test
    void lookalikeLoopbackHostsAreRejected() {
        rejected("http://localhost.evil.com/token");
        rejected("http://127.0.0.2/token");
    }

    @Test
    void plainHttpToRemoteHostIsAcceptedWhenExplicitlyAllowed() {
        Map<String, String> m = oauth();
        m.put(PREFIX + "oauth.token-endpoint", "http://idp.internal.example.com/token");
        m.put(PREFIX + "oauth.allow-insecure-http", " TRUE ");
        ProviderConfig c = ProviderConfig.parse(m);
        assertTrue(c.isAllowInsecureHttp());
        assertTrue(c.insecureEndpointWarning().orElseThrow(AssertionError::new).contains("plain http://"));
    }

    @Test
    void secureEndpointHasNoInsecureWarning() {
        assertFalse(withEndpoint("https://idp.example.com/token").insecureEndpointWarning().isPresent());
        assertFalse(withEndpoint("http://127.0.0.1/token").insecureEndpointWarning().isPresent());
    }

    @Test
    void allowInsecureHttpMustBeABoolean() {
        Map<String, String> m = oauth();
        m.put(PREFIX + "oauth.allow-insecure-http", "yes");
        IllegalConfigurationException e = assertThrows(IllegalConfigurationException.class, () -> ProviderConfig.parse(m));
        assertTrue(e.getMessage().contains(PREFIX + "oauth.allow-insecure-http"), e.getMessage());
    }

    @Test
    void endpointWithoutHostIsRejectedEvenWhenInsecureAllowed() {
        for (String endpoint : new String[] {"https:///token", "https:foo", "https://my_host/token"}) {
            IllegalConfigurationException e = rejected(endpoint);
            assertTrue(e.getMessage().contains("with a host"), endpoint + " → " + e.getMessage());
        }
        Map<String, String> m = oauth();
        m.put(PREFIX + "oauth.token-endpoint", "http:///token");
        m.put(PREFIX + "oauth.allow-insecure-http", "true");
        assertThrows(IllegalConfigurationException.class, () -> ProviderConfig.parse(m));
    }

    @Test
    void endpointWithCredentialsIsRejectedWithoutEchoingThem() {
        IllegalConfigurationException e = rejected("https://client:S3cretPw@idp.example.com/token");
        assertTrue(e.getMessage().contains("must not contain credentials"), e.getMessage());
        assertFalse(e.getMessage().contains("S3cretPw"), e.getMessage());
    }

    @Test
    void endpointWithFragmentIsRejected() {
        assertTrue(rejected("https://idp.example.com/token#frag").getMessage().contains("fragment"));
    }

    @Test
    void displayableEndpointDropsUserinfoQueryAndFragment() {
        assertEquals("https://idp.example.com:8443/oauth2/token",
                ProviderConfig.displayable(java.net.URI.create("https://u:p@idp.example.com:8443/oauth2/token?api_key=abc#x")));
        assertEquals("https://idp.example.com/token", ProviderConfig.displayable(java.net.URI.create("https://idp.example.com/token")));
    }

    @Test
    void timeoutsKeepTheirDefaults() {
        ProviderConfig c = ProviderConfig.parse(oauth());
        assertEquals(Duration.ofSeconds(10), c.getConnectTimeout());
        assertEquals(Duration.ofSeconds(30), c.getRequestTimeout());
    }

    @Test
    void timeoutsAreConfigurable() {
        Map<String, String> m = oauth();
        m.put(PREFIX + "oauth.connect-timeout", "3s");
        m.put(PREFIX + "oauth.request-timeout", " 2min ");
        ProviderConfig c = ProviderConfig.parse(m);
        assertEquals(Duration.ofSeconds(3), c.getConnectTimeout());
        assertEquals(Duration.ofMinutes(2), c.getRequestTimeout());
    }

    @Test
    void unparseableTimeoutNamesTheKey() {
        Map<String, String> m = oauth();
        m.put(PREFIX + "oauth.request-timeout", "soon");
        IllegalConfigurationException e = assertThrows(IllegalConfigurationException.class, () -> ProviderConfig.parse(m));
        assertTrue(e.getMessage().contains(PREFIX + "oauth.request-timeout"), e.getMessage());
    }

    @Test
    void nonPositiveTimeoutIsRejected() {
        Map<String, String> m = oauth();
        m.put(PREFIX + "oauth.connect-timeout", "0ms");
        IllegalConfigurationException e = assertThrows(IllegalConfigurationException.class, () -> ProviderConfig.parse(m));
        assertTrue(e.getMessage().contains(PREFIX + "oauth.connect-timeout"), e.getMessage());
        assertTrue(e.getMessage().contains("positive"), e.getMessage());
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
    void retryIntervalDefaultsToOneMinute() {
        assertEquals(Duration.ofMinutes(1), ProviderConfig.parse(oauth()).getRetryInterval());
    }

    @Test
    void retryIntervalIsConfigurable() {
        Map<String, String> m = oauth();
        m.put(PREFIX + "oauth.retry-interval", "20s");
        assertEquals(Duration.ofSeconds(20), ProviderConfig.parse(m).getRetryInterval());
    }

    @Test
    void retryIntervalMustBePositive() {
        Map<String, String> m = oauth();
        m.put(PREFIX + "oauth.retry-interval", "0s");
        IllegalConfigurationException e = assertThrows(IllegalConfigurationException.class, () -> ProviderConfig.parse(m));
        assertTrue(e.getMessage().contains(PREFIX + "oauth.retry-interval"), e.getMessage());
    }

    @Test
    void renewalRatioIsReadFromFlinkConfiguration() {
        assertEquals(0.75, ProviderConfig.parse(oauth()).getRenewalRatio());
        Map<String, String> m = oauth();
        m.put("security.delegation.tokens.renewal.time-ratio", "0.5");
        assertEquals(0.5, ProviderConfig.parse(m).getRenewalRatio());
    }
}
