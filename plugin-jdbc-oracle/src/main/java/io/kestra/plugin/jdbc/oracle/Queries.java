package io.kestra.plugin.jdbc.oracle;

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

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Perform multiple queries on an Oracle database."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute multiple queries and get the results.",
            code = """
                id: oracle_query
                namespace: company.team

                tasks:
                  - id: select
                    type: io.kestra.plugin.jdbc.oracle.Queries
                    url: jdbc:oracle:thin:@localhost:49161:XE
                    username: oracle_user
                    password: oracle_password
                    sql: select * from employee; select * from laptop;
                    fetchType: FETCH
                """
        )
    }
)
public class Queries extends AbstractJdbcQueries implements RunnableTask<AbstractJdbcQueries.MultiQueryOutput> {

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new OracleCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
    }
}
