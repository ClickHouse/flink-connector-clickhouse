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
