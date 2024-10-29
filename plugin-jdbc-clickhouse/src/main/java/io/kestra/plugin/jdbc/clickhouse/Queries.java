package io.kestra.plugin.jdbc.clickhouse;

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
    title = "Perform multiple queries on a Clickhouse database."
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
                    username: ch_user
                    password: ch_password
                    sql: select * from employee; select * from laptop;
                    fetchType: STORE
                """
        )
    }
)
public class Queries extends AbstractJdbcQueries implements RunnableTask<AbstractJdbcQueries.MultiQueryOutput> {

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new ClickHouseCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.clickhouse.jdbc.ClickHouseDriver());
    }
}
