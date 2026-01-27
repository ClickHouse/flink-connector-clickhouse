package org.apache.flink.connector.clickhouse.sink;

public class ClickHouseSinkTestUtils {
    public static final int MAX_BATCH_SIZE = 5000;
    public static final int MIN_BATCH_SIZE = 1;
    public static final int MAX_IN_FLIGHT_REQUESTS = 2;
    public static final int MAX_BUFFERED_REQUESTS = 20000;
    public static final long MAX_BATCH_SIZE_IN_BYTES = 1024 * 1024;
    public static final long MAX_TIME_IN_BUFFER_MS = 5 * 1000;
    public static final long MAX_RECORD_SIZE_IN_BYTES = 1000;

    public static String createSimplePOJOTableSQL(String database, String tableName, int parts_to_throw_insert) {
        String createTable = createSimplePOJOTableSQL(database, tableName);
        return createTable.trim().substring(0, createTable.trim().length() - 1) + " " + String.format("SETTINGS parts_to_throw_insert = %d;",  parts_to_throw_insert);
    }

    public static String createSimplePOJOTableSQL(String database, String tableName) {
        return "CREATE TABLE `" + database + "`.`" + tableName + "` (" +
                "bytePrimitive Int8," +
                "byteObject Int8," +
                "shortPrimitive Int16," +
                "shortObject Int16," +
                "intPrimitive Int32," +
                "integerObject Int32," +
                "longPrimitive Int64," +
                "longObject Int64," +
                "bigInteger128 Int128," +
                "bigInteger256 Int256," +
                "uint8Primitive  UInt8," +
                "uint8Object UInt8," +
                "uint16Primitive  UInt16," +
                "uint16Object UInt16," +
                "uint32Primitive  UInt32," +
                "uint32Object UInt32," +
                "uint64Primitive  UInt64," +
                "uint64Object UInt64," +
                "uint128Object UInt128," +
                "uint256Object UInt256," +
                "decimal Decimal(10,5)," +
                "decimal32 Decimal32(9)," +
                "decimal64 Decimal64(18)," +
                "decimal128 Decimal128(38)," +
                "decimal256 Decimal256(76)," +
                "floatPrimitive Float," +
                "floatObject Float," +
                "doublePrimitive Double," +
                "doubleObject Double," +
                "booleanPrimitive Boolean," +
                "booleanObject Boolean," +
                "str String," +
                "fixedStr FixedString(10)," +
                "v_date Date," +
                "v_date32 Date32," +
                "v_dateTime DateTime," +
                "v_dateTime64 DateTime64," +
                "uuid UUID," +
                "stringList Array(String)," +
                "longList Array(Int64)," +
                "mapOfStrings Map(String,String)," +
                "tupleOfObjects Tuple(String,Int64,Boolean)," +
                ") " +
                "ENGINE = MergeTree " +
                "ORDER BY (longPrimitive); ";
    }
}
