package io.kestra.plugin.jdbc.druid;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.calcite.avatica.remote.Driver;

import java.sql.*;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Query a Apache Druid database."
)
@Plugin(
    examples = {
        @Example(
            title = "Query an Apache Druid database.",
            full = true,
            code = """
                id: druid_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.druid.Query
                    url: jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/;transparent_reconnection=true
                    sql: |
                      SELECT *
                      FROM wikiticker
                    fetchType: STORE
                """
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, DruidConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new DruidCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new Driver());
    }

    @Override
    protected Statement createStatement(Connection conn) throws SQLException {
        return conn.createStatement();
    }
}
