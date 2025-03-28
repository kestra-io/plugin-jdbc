package io.kestra.plugin.jdbc.dremio;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
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
    title = "Query a Dremio database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SQL query to a Dremio database and fetch a row as output.",
            full = true,
            code = """
                id: dremio_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.dremio.Query
                    url: jdbc:dremio:direct=sql.dremio.cloud:443;ssl=true;PROJECT_ID=sampleProjectId;
                    username: dremio_token
                    password: samplePersonalAccessToken
                    sql: select * FROM source.database.table
                    fetchType: FETCH_ONE
                """
        )
    }
)
public class Query extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output>, DremioConnectionInterface {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new DremioCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.dremio.jdbc.Driver());
    }
}
