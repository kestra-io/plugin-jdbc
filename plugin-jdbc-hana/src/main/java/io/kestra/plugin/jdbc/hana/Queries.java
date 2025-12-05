package io.kestra.plugin.jdbc.hana;

import com.sap.db.jdbc.Driver;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Properties;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run multiple SAP HANA queries."
)
@Plugin(
    examples = {
        @Example(
            title = "Run multiple SQL queries on SAP HANA",
            full = true,
            code = """
                id: hana_queries
                namespace: company.team

                tasks:
                  - id: queries
                    type: io.kestra.plugin.jdbc.hana.Queries
                    url: jdbc:sap://127.0.0.1:39015/?databaseName=SYSTEMDB
                    username: "{{ secret('HANA_USERNAME') }}"
                    password: "{{ secret('HANA_PASSWORD') }}"
                    sql: select * from TABLE1; select * from TABLE2;
                    fetchType: FETCH
            """
        )
    },
    metrics = {
        @Metric(
            name = "fetch.size",
            type = Counter.TYPE,
            unit = "rows",
            description = "Number of fetched rows."
        )
    }
)
public class Queries extends AbstractJdbcQueries implements HanaConnectionInterface {

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new HanaCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        return super.connectionProperties(runContext, this.getScheme());
    }
}
