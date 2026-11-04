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
