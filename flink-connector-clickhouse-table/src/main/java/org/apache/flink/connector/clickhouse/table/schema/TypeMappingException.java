package org.apache.flink.connector.clickhouse.table.schema;

/**
 * A single (Flink {@code LogicalType}, {@code ClickHouseColumn}) pair failed to map.
 * Carries only the reason; {@link SchemaResolver} wraps it into the committed,
 * column-naming {@code ValidationException} messages.
 */
public class TypeMappingException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /** How the resolver should phrase the failure. */
    public enum Kind {
        /** The Flink type and the ClickHouse type cannot be paired. */
        MISMATCH,
        /** The ClickHouse type has no write path at all, whatever the Flink type. */
        TARGET_UNSUPPORTED
    }

    private final Kind kind;

    private TypeMappingException(Kind kind, String reason) {
        super(reason);
        this.kind = kind;
    }

    public static TypeMappingException mismatch(String reason) {
        return new TypeMappingException(Kind.MISMATCH, reason);
    }

    public static TypeMappingException targetUnsupported(String reason) {
        return new TypeMappingException(Kind.TARGET_UNSUPPORTED, reason);
    }

    public Kind getKind() {
        return kind;
    }
}
