package io.kestra.plugin.jdbc.druid;

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
import org.apache.calcite.avatica.remote.Driver;

import java.sql.*;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Run multiple Apache Druid queries."
)
@Plugin(
    examples = {
        @Example(
            title = "Multiple queries on an Apache Druid database.",
            full = true,
            code = """
                id: druid_queries
                namespace: company.team

                tasks:
                  - id: queries
                    type: io.kestra.plugin.jdbc.druid.Queries
                    url: jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/;transparent_reconnection=true
                    sql: |
                      SELECT * FROM wikiticker; SELECT * FROM product;
                    fetchType: STORE
                """
        )
    }
)
public class Queries extends AbstractJdbcQueries implements RunnableTask<AbstractJdbcQueries.MultiQueryOutput>, DruidConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new DruidCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }
}
