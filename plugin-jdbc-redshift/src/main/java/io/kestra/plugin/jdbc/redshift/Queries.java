package io.kestra.plugin.jdbc.redshift;

import com.amazon.redshift.jdbc.Driver;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
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
    title = "Run multiple Redshift queries."
)
@Plugin(
    examples = {
        @Example(
            title = "Send SQL queries to a Redshift database and fetch the outputs.",
            full = true,
            code = """
                   id: redshift_queries
                   namespace: company.team

                   tasks:
                     - id: select
                       type: io.kestra.plugin.jdbc.redshift.Queries
                       url: jdbc:redshift://123456789.eu-central-1.redshift-serverless.amazonaws.com:5439/dev
                       username: "{{ secret('REDSHIFT_USERNAME') }}"
                       password: "{{ secret('REDSHIFT_PASSWORD') }}"
                       sql: select count(*) from employee; select count(*) from laptop;
                       fetchType: FETCH_ONE
                   """
        )
    },
    metrics = {
        @Metric(
            name = "fetch.size",
            type = Counter.TYPE,
            unit = "rows",
            description = "The number of fetched rows."
        )
    }
)
public class Queries extends AbstractJdbcQueries implements RedshiftConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new RedshiftCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }

}
