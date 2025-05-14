package io.kestra.plugin.jdbc.dremio;

import com.dremio.jdbc.Driver;
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
    title = "Perform multiple queries on a Dremio database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send multiple select queries to a Dremio database and fetch a row as output.",
            full = true,
            code = """
                id: dremio_queries
                namespace: company.team

                tasks:
                  - id: queries
                    type: io.kestra.plugin.jdbc.dremio.Queries
                    url: jdbc:dremio:direct=sql.dremio.cloud:443;ssl=true;PROJECT_ID=sampleProjectId;
                    username: dremio_token
                    password: samplePersonalAccessToken
                    sql: select * FROM source.database.table; select * FROM source.database.table; 
                    fetchType: FETCH_ONE
                """
        )
    }
)
public class Queries extends AbstractJdbcQueries implements RunnableTask<AbstractJdbcQueries.MultiQueryOutput> {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new DremioCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }
}
