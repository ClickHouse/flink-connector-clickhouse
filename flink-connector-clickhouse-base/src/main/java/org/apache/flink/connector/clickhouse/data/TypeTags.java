package org.apache.flink.connector.clickhouse.data;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Tag-and-payload serialization for {@code Map<String, Object>} values used
 * inside {@link ClickHousePayload#getData()}. Tag values are documented in
 * the design spec §10c and must remain stable across releases.
 */
public final class TypeTags {

    static final byte NULL = 0;
    static final byte BOOL = 1;
    static final byte BYTE = 2;
    static final byte SHORT = 3;
    static final byte INT = 4;
    static final byte LONG = 5;
    static final byte FLOAT = 6;
    static final byte DOUBLE = 7;
    static final byte BIG_INT = 8;
    static final byte BIG_DEC = 9;
    static final byte STRING = 10;
    static final byte BYTES = 11;
    static final byte UUID_T = 12;
    static final byte LOCAL_DATE = 13;
    static final byte LOCAL_DATETIME = 14;
    static final byte ZONED_DATETIME = 15;
    static final byte LIST = 16;
    static final byte MAP = 17;
    static final byte TUPLE = 18;

    private TypeTags() {}

    /** Writes one tagged value. Throws IOException with a clear key-naming hint if type isn't supported. */
    public static void write(Object value, DataOutputStream out) throws IOException {
        if (value == null) { out.writeByte(NULL); return; }
        if (value instanceof Boolean)        { out.writeByte(BOOL);    out.writeBoolean((Boolean) value); return; }
        if (value instanceof Byte)           { out.writeByte(BYTE);    out.writeByte((Byte) value); return; }
        if (value instanceof Short)          { out.writeByte(SHORT);   out.writeShort((Short) value); return; }
        if (value instanceof Integer)        { out.writeByte(INT);     out.writeInt((Integer) value); return; }
        if (value instanceof Long)           { out.writeByte(LONG);    out.writeLong((Long) value); return; }
        if (value instanceof Float)          { out.writeByte(FLOAT);   out.writeFloat((Float) value); return; }
        if (value instanceof Double)         { out.writeByte(DOUBLE);  out.writeDouble((Double) value); return; }
        if (value instanceof BigInteger) {
            byte[] b = ((BigInteger) value).toByteArray();
            out.writeByte(BIG_INT); out.writeInt(b.length); out.write(b); return;
        }
        if (value instanceof BigDecimal) {
            BigDecimal d = (BigDecimal) value;
            byte[] b = d.unscaledValue().toByteArray();
            out.writeByte(BIG_DEC); out.writeInt(d.scale());
            out.writeInt(b.length); out.write(b); return;
        }
        if (value instanceof String)         { out.writeByte(STRING);  out.writeUTF((String) value); return; }
        if (value instanceof byte[]) {
            byte[] b = (byte[]) value;
            out.writeByte(BYTES); out.writeInt(b.length); out.write(b); return;
        }
        if (value instanceof UUID) {
            UUID u = (UUID) value;
            out.writeByte(UUID_T); out.writeLong(u.getMostSignificantBits()); out.writeLong(u.getLeastSignificantBits()); return;
        }
        if (value instanceof LocalDate) {
            out.writeByte(LOCAL_DATE);
            // toEpochDay() returns long; cast to int is safe for the ClickHouse Date/Date32
            // range (year 1900 to 9999, epoch day ≤ ~2.9M). Dates outside this range will
            // silently wrap — not a concern in practice.
            out.writeInt((int) ((LocalDate) value).toEpochDay());
            return;
        }
        if (value instanceof LocalDateTime) {
            LocalDateTime ldt = (LocalDateTime) value;
            out.writeByte(LOCAL_DATETIME);
            out.writeLong(ldt.toEpochSecond(java.time.ZoneOffset.UTC));
            out.writeInt(ldt.getNano()); return;
        }
        if (value instanceof ZonedDateTime) {
            ZonedDateTime zdt = (ZonedDateTime) value;
            out.writeByte(ZONED_DATETIME);
            out.writeLong(zdt.toEpochSecond());
            out.writeInt(zdt.getNano());
            out.writeUTF(zdt.getZone().getId()); return;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            out.writeByte(LIST); out.writeInt(list.size());
            for (Object e : list) write(e, out); return;
        }
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            out.writeByte(MAP); out.writeInt(map.size());
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (!(e.getKey() instanceof String))
                    throw new IOException("Map keys must be String, got: " + e.getKey().getClass());
                out.writeUTF((String) e.getKey()); write(e.getValue(), out);
            } return;
        }
        if (value instanceof Object[]) {
            Object[] tup = (Object[]) value;
            out.writeByte(TUPLE); out.writeInt(tup.length);
            for (Object e : tup) write(e, out); return;
        }
        throw new IOException("Unsupported map value type: " + value.getClass().getName()
            + " (allowed types: see design spec §7)");
    }

    /** Reads one tagged value. */
    public static Object read(DataInputStream in) throws IOException {
        byte tag = in.readByte();
        switch (tag) {
            case NULL:   return null;
            case BOOL:   return in.readBoolean();
            case BYTE:   return in.readByte();
            case SHORT:  return in.readShort();
            case INT:    return in.readInt();
            case LONG:   return in.readLong();
            case FLOAT:  return in.readFloat();
            case DOUBLE: return in.readDouble();
            case BIG_INT: {
                int len = in.readInt();
                if (len < 0 || len > 1024) throw new IOException("Implausible BIG_INT length: " + len);
                byte[] b = new byte[len];
                in.readFully(b);
                return new BigInteger(b);
            }
            case BIG_DEC: {
                int scale = in.readInt();
                int len = in.readInt();
                if (len < 0 || len > 1024) throw new IOException("Implausible BIG_DEC length: " + len);
                byte[] b = new byte[len];
                in.readFully(b);
                return new BigDecimal(new BigInteger(b), scale);
            }
            case STRING: return in.readUTF();
            case BYTES: {
                int len = in.readInt();
                if (len < 0 || len > 256 * 1024 * 1024) throw new IOException("Implausible BYTES length: " + len);
                byte[] b = new byte[len];
                in.readFully(b);
                return b;
            }
            case UUID_T: return new UUID(in.readLong(), in.readLong());
            case LOCAL_DATE: return LocalDate.ofEpochDay(in.readInt());
            case LOCAL_DATETIME: {
                long sec = in.readLong(); int nanos = in.readInt();
                return LocalDateTime.ofEpochSecond(sec, nanos, java.time.ZoneOffset.UTC);
            }
            case ZONED_DATETIME: {
                long sec = in.readLong(); int nanos = in.readInt(); String zone = in.readUTF();
                return ZonedDateTime.ofInstant(java.time.Instant.ofEpochSecond(sec, nanos), ZoneId.of(zone));
            }
            case LIST: {
                int n = in.readInt();
                if (n < 0 || n > 1_000_000) throw new IOException("Implausible LIST size: " + n);
                List<Object> list = new ArrayList<>(n);
                for (int i = 0; i < n; i++) list.add(read(in));
                return list;
            }
            case MAP: {
                int n = in.readInt();
                if (n < 0 || n > 1_000_000) throw new IOException("Implausible MAP size: " + n);
                Map<String, Object> m = new LinkedHashMap<>(n);
                for (int i = 0; i < n; i++) m.put(in.readUTF(), read(in));
                return m;
            }
            case TUPLE: {
                int n = in.readInt();
                if (n < 0 || n > 1_000_000) throw new IOException("Implausible TUPLE size: " + n);
                Object[] arr = new Object[n];
                for (int i = 0; i < n; i++) arr[i] = read(in);
                return arr;
            }
            default: throw new IOException("Unknown type tag: " + tag);
        }
    }
}
