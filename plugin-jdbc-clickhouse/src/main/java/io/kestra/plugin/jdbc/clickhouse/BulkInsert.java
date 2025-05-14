package io.kestra.plugin.jdbc.clickhouse;

import com.clickhouse.jdbc.ClickHouseDriver;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
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
    title = "Bulk Insert new rows into a ClickHouse database."
)
@Plugin(
    examples = {
        @Example(
            title = "Insert rows from another table to a Clickhouse database using asynchronous inserts.",
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
                    username: ch_user
                    password: ch_password
                    sql: INSERT INTO YourTable SETTINGS async_insert=1, wait_for_async_insert=1 values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
                """
        ),
        @Example(
            title = "Insert data into specific columns via a SQL query to a ClickHouse database using asynchronous inserts.",
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
                    username: ch_user
                    password: ch_password
                    sql: INSERT INTO YourTable ( field1, field2, field3 ) SETTINGS async_insert=1, wait_for_async_insert=1 values( ?, ?, ? )
                """
        ),
        @Example(
            title = "Insert data into specific columns via a SQL query to a ClickHouse database using asynchronous inserts.",
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
                    username: ch_user
                    password: ch_password
                    table: YourTable
                """
        )
    }
)
public class BulkInsert extends AbstractJdbcBatch implements RunnableTask<AbstractJdbcBatch.Output> {

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

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.run(runContext);
    }
}
