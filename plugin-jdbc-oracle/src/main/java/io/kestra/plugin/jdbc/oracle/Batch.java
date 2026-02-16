package io.kestra.plugin.jdbc.oracle;

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
import oracle.jdbc.OracleDriver;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Bulk insert rows into Oracle Database using prepared statements",
    description = "Reads ION-formatted data from Kestra internal storage and performs high-performance batch inserts using JDBC batch operations. Data is processed in chunks (default 1,000 rows) to optimize memory and performance. Supports auto-commit for databases without transaction support. Handles NULL values explicitly to avoid ORA-17041 errors."
)
@Plugin(
    examples = {
        @Example(
            title = "Fetch rows from a table and bulk insert to another one",
            full = true,
            code = """
                id: oracle_batch
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.oracle.Query
                    url: jdbc:oracle:thin:@dev:49161:XE
                    username: oracle
                    password: oracle_password
                    sql: |
                      SELECT *
                      FROM xref
                      FETCH FIRST 1500 ROWS ONLY;
                    fetchType: STORE

                  - id: update
                    type: io.kestra.plugin.jdbc.oracle.Batch
                    from: "{{ outputs.query.uri }}"
                    url: jdbc:oracle:thin:@prod:49161:XE
                    username: oracle
                    password: oracle_password
                    sql: |
                      insert into xref values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
                """
        ),
        @Example(
            title = "Fetch rows from a table and bulk insert to another one, without using sql query",
            full = true,
            code = """
                id: oracle_batch
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.oracle.Query
                    url: jdbc:oracle:thin:@dev:49161:XE
                    username: oracle
                    password: oracle_password
                    sql: |
                      SELECT *
                      FROM xref
                      FETCH FIRST 1500 ROWS ONLY;
                    fetchType: STORE

                  - id: update
                    type: io.kestra.plugin.jdbc.oracle.Batch
                    from: "{{ outputs.query.uri }}"
                    url: jdbc:oracle:thin:@prod:49161:XE
                    username: oracle
                    password: oracle_password
                    table: XREF
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
public class Batch extends AbstractJdbcBatch implements OracleConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new OracleCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(OracleDriver.class::isInstance)) {
            DriverManager.registerDriver(new OracleDriver());
        }
    }
}
