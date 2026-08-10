package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.data.ClickHouseColumn;

import org.apache.flink.connector.clickhouse.table.data.ValueConverter;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.EnumSet;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickHouseTypeMapperTest {

    private static final ZoneId UTC = ZoneId.of("UTC");

    private static ClickHouseColumn col(String type) {
        return ClickHouseColumn.of("c", type);
    }

    /**
     * Guard: every Flink type root is either mapped or explicitly rejected, so a root
     * added by a future Flink can never fall through silently.
     */
    @Test
    void everyLogicalTypeRootIsMappedOrExplicitlyRejected() {
        assertEquals(EnumSet.allOf(LogicalTypeRoot.class),
                EnumSet.copyOf(ClickHouseTypeMapper.registeredRoots()));
    }

    @Test
    void intWritesToInt32AndWidensToInt64() {
        ValueConverter toInt32 = ClickHouseTypeMapper.converter(new IntType(false), col("Int32"), UTC, "c");
        assertEquals(7, toInt32.convert(7));
        ValueConverter toInt64 = ClickHouseTypeMapper.converter(new IntType(false), col("Int64"), UTC, "c");
        assertEquals(7L, toInt64.convert(7));
    }

    @Test
    void narrowingIntToInt16IsRejected() {
        assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converter(new IntType(false), col("Int16"), UTC, "c"));
    }

    @Test
    void signedIntNeverTargetsUnsignedOfSameWidth() {
        assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converter(new IntType(false), col("UInt32"), UTC, "c"));
    }

    @Test
    void stringUnwrapsToUuidForUuidColumns() {
        ValueConverter converter = ClickHouseTypeMapper.converter(
                new VarCharType(false, VarCharType.MAX_LENGTH), col("UUID"), UTC, "c");
        UUID uuid = UUID.randomUUID();
        assertEquals(uuid, converter.convert(StringData.fromString(uuid.toString())));
    }

    @Test
    void enumTargetIsExplicitlyUnsupported() {
        TypeMappingException e = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converter(
                        new VarCharType(false, VarCharType.MAX_LENGTH),
                        col("Enum8('new' = 1, 'done' = 2)"), UTC, "c"));
        assertEquals(TypeMappingException.Kind.TARGET_UNSUPPORTED, e.getKind());
        assertTrue(e.getMessage().contains("issue #43"), e.getMessage());
    }

    @Test
    void timestampPrecisionMayNotExceedColumnScale() {
        TypeMappingException e = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converter(
                        new TimestampType(false, 9), col("DateTime64(3)"), UTC, "c"));
        assertEquals("precision 9 exceeds the column's scale 3", e.getMessage());
    }

    @Test
    void timestampIsInterpretedInTheSinkTimezone() {
        ZoneId tokyo = ZoneId.of("Asia/Tokyo");
        ValueConverter converter = ClickHouseTypeMapper.converter(
                new TimestampType(false, 3), col("DateTime64(3)"), tokyo, "c");
        LocalDateTime wallClock = LocalDateTime.of(2026, 1, 2, 3, 4, 5, 678_000_000);
        assertEquals(ZonedDateTime.of(wallClock, tokyo),
                converter.convert(TimestampData.fromLocalDateTime(wallClock)));
    }

    @Test
    void simpleAggregateFunctionIsTransparentForMatching() {
        ValueConverter converter = ClickHouseTypeMapper.converter(
                new IntType(false), col("SimpleAggregateFunction(max, Int32)"), UTC, "c");
        assertEquals(41, converter.convert(41));
    }
}
