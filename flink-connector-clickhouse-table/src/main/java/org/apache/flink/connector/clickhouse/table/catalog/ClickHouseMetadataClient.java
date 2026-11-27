package org.apache.flink.connector.clickhouse.table.catalog;

import com.clickhouse.client.api.metadata.TableSchema;

import java.util.List;
import java.util.Map;

/**
 * The reads {@link ClickHouseCatalog} makes of ClickHouse. The version modules implement it over
 * the sink factory's own planning client, so the catalog and the sink read a table identically.
 */
public interface ClickHouseMetadataClient {

    /** The first column of each row of {@code sql}, with {@code parameters} bound server-side. */
    List<String> queryColumn(String sql, Map<String, Object> parameters);

    /** The columns of the table that {@code tableOptions} address, as the sink's planning reads them. */
    TableSchema describe(Map<String, String> tableOptions);
}
