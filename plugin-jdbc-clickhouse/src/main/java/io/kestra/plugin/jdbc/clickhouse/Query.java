package io.kestra.plugin.jdbc.clickhouse;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a Clickhouse database."
)
@Plugin(
    examples = {
        @Example(
            title = "Query a Clickhouse database.",
            full = true,
            code = """
                id: clickhouse_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.clickhouse.Query
                    url: jdbc:clickhouse://127.0.0.1:56982/
                    username: ch_user
                    password: ch_password
                    sql: select * from clickhouse_types
                    fetchType: STORE
                """
        ),
        @Example(
            full = true,
            title = "Ingest data to and query data from ClickHouse",
            code = """
                id: query_clickhouse
                namespace: company.team

                tasks:
                  - id: create_database
                    type: io.kestra.plugin.jdbc.clickhouse.Query
                    sql: CREATE DATABASE IF NOT EXISTS helloworld

                  - id: create_table
                    type: io.kestra.plugin.jdbc.clickhouse.Query
                    sql: |
                      CREATE TABLE IF NOT EXISTS helloworld.my_first_table
                      (
                          user_id String,
                          message String,
                          timestamp DateTime,
                          metric Float32
                      )
                      ENGINE = MergeTree()
                      PRIMARY KEY (user_id, timestamp)

                  - id: insert_data
                    type: io.kestra.plugin.jdbc.clickhouse.Query
                    sql: |
                      INSERT INTO helloworld.my_first_table (user_id, message, timestamp,
                      metric) VALUES
                          (101, 'Hello, ClickHouse!',                                 now(),       -1.0    ),
                          (102, 'Insert a lot of rows per batch',                     yesterday(), 1.41421 ),
                          (102, 'Sort your data based on your commonly-used queries', today(),     2.718   ),
                          (101, 'Granules are the smallest chunks of data read',      now() + 5,   3.14159 )

                  - id: query_and_store_as_json
                    type: io.kestra.plugin.jdbc.clickhouse.Query
                    sql: SELECT user_id, message FROM helloworld.my_first_table
                    fetchType: STORE

                pluginDefaults:
                  - type: io.kestra.plugin.jdbc.clickhouse.Query
                    values:
                      url: jdbc:clickhouse://host.docker.internal:8123/
                      username: default
            """
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, ClickhouseConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new ClickHouseCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.clickhouse.jdbc.ClickHouseDriver());
    }
}
