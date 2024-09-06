package io.kestra.plugin.jdbc.pinot;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AutoCommitInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.pinot.client.PinotDriver;

import java.sql.*;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a Apache Pinot server."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: pinot_query
                namespace: company.team
                
                tasks:
                  - id: query
                    type: o.kestra.plugin.jdbc.pinot.Query
                    url: jdbc:pinot://localhost:9000
                    sql: |
                      SELECT *
                      FROM airlineStats
                    fetch: true
                """
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output> {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new PinotCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new PinotDriver());
    }

    @Override
    protected Statement createStatement(Connection conn) throws SQLException {
        return conn.createStatement();
    }

}
