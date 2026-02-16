package io.kestra.plugin.jdbc.clickhouse;

import com.clickhouse.jdbc.ClickHouseDriver;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Bulk insert rows into ClickHouse with JDBC batch"
)
@Plugin(
    examples = {
        @Example(
            title = "Insert rows from another table to ClickHouse with async inserts",
            full = true,
            code = """
                id: clickhouse_bulk_insert
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: bulk_insert
                    type: io.kestra.plugin.jdbc.clickhouse.BulkInsert
                    from: "{{ inputs.file }}"
                    url: jdbc:clickhouse://127.0.0.1:56982/
                    username: "{{ secret('CLICKHOUSE_USERNAME') }}"
                    password: "{{ secret('CLICKHOUSE_PASSWORD') }}"
                    sql: INSERT INTO YourTable SETTINGS async_insert=1, wait_for_async_insert=1 VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
                """
        ),
        @Example(
            title = "Insert into selected columns using async inserts",
            full = true,
            code = """
                id: clickhouse_bulk_insert
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: bulk_insert
                    type: io.kestra.plugin.jdbc.clickhouse.BulkInsert
                    from: "{{ inputs.file }}"
                    url: jdbc:clickhouse://127.0.0.1:56982/
                    username: "{{ secret('CLICKHOUSE_USERNAME') }}"
                    password: "{{ secret('CLICKHOUSE_PASSWORD') }}"
                    sql: INSERT INTO YourTable ( field1, field2, field3 ) SETTINGS async_insert=1, wait_for_async_insert=1 VALUES( ?, ?, ? )
                """
        ),
        @Example(
            title = "Auto-generate INSERT using table column discovery",
            full = true,
            code = """
                id: clickhouse_bulk_insert
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: bulk_insert
                    type: io.kestra.plugin.jdbc.clickhouse.BulkInsert
                    from: "{{ inputs.file }}"
                    url: jdbc:clickhouse://127.0.0.1:56982/
                    username: "{{ secret('CLICKHOUSE_USERNAME') }}"
                    password: "{{ secret('CLICKHOUSE_PASSWORD') }}"
                    table: YourTable
                """
        )
    },
    metrics = {
        @Metric(
            name = "records",
            type = Counter.TYPE,
            unit = "records",
            description = "The number of records processed."
        ),
        @Metric(
            name = "updated",
            type = Counter.TYPE,
            unit = "records",
            description = "The number of records updated."
        ),
        @Metric(
            name = "query",
            type = Counter.TYPE,
            unit = "queries",
            description = "The number of batch queries executed."
        )
    }
)
public class BulkInsert extends AbstractJdbcBatch implements ClickhouseConnectionInterface {

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new ClickHouseCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(ClickHouseDriver.class::isInstance)) {
            DriverManager.registerDriver(new ClickHouseDriver());
        }
    }
}
