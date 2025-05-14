package io.kestra.plugin.jdbc.clickhouse;

import com.clickhouse.jdbc.ClickHouseDriver;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
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
    title = "Run multiple ClickHouse queries."
)
@Plugin(
    examples = {
        @Example(
            title = "Queries on a Clickhouse database.",
            full = true,
            code = """
                id: clickhouse_queries
                namespace: company.team

                tasks:
                  - id: queries
                    type: io.kestra.plugin.jdbc.clickhouse.Queries
                    url: jdbc:clickhouse://127.0.0.1:56982/
                    username: "{{ secret('CLICKHOUSE_USERNAME') }}"
                    password: "{{ secret('CLICKHOUSE_PASSWORD') }}"
                    sql: select * from employee; select * from laptop;
                    fetchType: STORE
                """
        )
    }
)
public class Queries extends AbstractJdbcQueries implements RunnableTask<AbstractJdbcQueries.MultiQueryOutput>, ClickhouseConnectionInterface {

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
