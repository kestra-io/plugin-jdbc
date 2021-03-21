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
import io.kestra.core.runners.RunContext;
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
    title = "Clickhouse Query Task.",
    description = "Currently supported types are the following ones : \n" +
        " - Int8,\n" +
        " - Float32,\n" +
        " - Float64,\n" +
        " - Decimal(n, m),\n" +
        " - String,\n" +
        " - FixedString(n),\n" +
        " - UUID,\n" +
        " - Date,\n" +
        " - DateTime(n),\n" +
        " - DateTime64(n, m),\n" +
        " - Enum,\n" +
        " - LowCardinality(n),\n" +
        " - Array(n),\n" +
        " - Nested(),\n" +
        " - Tuple(n, m),\n" +
        " - Nullable(n),\n" +
        " - Ipv4,\n" +
        " - Ipv6"
)
@Plugin(
    examples = {
        @Example(
            title = "Request a Clickhouse Database and fetch a row as outputs",
            code = {
                "url: jdbc:clickhouse://127.0.0.1:56982/",
                "username: clickhouse",
                "password: ch_passwd",
                "sql: select * from clickhouse_types",
                "fetchOne: true",
            }
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output> {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new ClickHouseCellConverter(zoneId);
    }

    @Override
    protected void registerDriver() throws SQLException {
        DriverManager.registerDriver(new ru.yandex.clickhouse.ClickHouseDriver());
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.run(runContext);
    }
}
