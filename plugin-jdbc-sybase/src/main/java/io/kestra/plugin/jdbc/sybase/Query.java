package io.kestra.plugin.jdbc.sybase;

import com.sybase.jdbc4.jdbc.SybDriver;
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

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a Sybase database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SQL query to a Sybase Database and fetch a row as output.",
            full = true,
            code = """
                   id: sybase_query
                   namespace: company.team

                   tasks:
                     - id: query
                       type: io.kestra.plugin.jdbc.sybase.Query
                       url: jdbc:sybase:Tds:127.0.0.1:5000/
                       username: "{{ secret('SYBASE_USERNAME') }}"
                       password: "{{ secret('SYBASE_PASSWORD') }}"
                       sql: select * from syb_types
                       fetchType: FETCH_ONE
                   """
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, SybaseConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new SybaseCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(SybDriver.class::isInstance)) {
            DriverManager.registerDriver(new SybDriver());
        }
    }
}
