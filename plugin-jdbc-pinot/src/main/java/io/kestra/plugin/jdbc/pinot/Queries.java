package io.kestra.plugin.jdbc.pinot;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
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
    title = "Perform multiple queries on an Apache Pinot server."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: pinot_queries
                namespace: company.team

                tasks:
                  - id: queries
                    type: io.kestra.plugin.jdbc.pinot.Queries
                    url: jdbc:pinot://localhost:9000
                    username: "{{ secret('PINOT_USERNAME') }}"
                    password: "{{ secret('PINOT_PASSWORD') }}"
                    sql: |
                      SELECT * FROM airlineStats;
                      SELECT * FROM airlineStats;
                    fetchType: FETCH
                """
        )
    }
)
public class Queries extends AbstractJdbcQueries implements RunnableTask<AbstractJdbcQueries.MultiQueryOutput>, PinotConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new PinotCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new PinotDriver());
    }

    @Override
    protected PreparedStatement createPreparedStatement(Connection conn, String preparedSql) throws SQLException {
        return conn.prepareStatement(preparedSql);
    }
}
