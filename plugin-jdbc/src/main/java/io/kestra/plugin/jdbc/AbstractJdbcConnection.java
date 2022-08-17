package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcConnection extends Task {
    @Schema(
        title = "The jdbc url to connect to the database"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String url;

    @Schema(
        title = "The database user"
    )
    @PluginProperty(dynamic = true)
    protected String username;

    @Schema(
        title = "The database user's password"
    )
    @PluginProperty(dynamic = true)
    protected String password;

    /**
     * JDBC driver may be auto-registered. See <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html">DriverManager</a>
     *
     * @throws SQLException registerDrivers failed
     */
    protected abstract void registerDriver() throws SQLException;

    protected Properties connectionProperties(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        Properties props = new Properties();
        props.put("jdbc.url", runContext.render(this.url));

        if (this.username != null) {
            props.put("user", runContext.render(this.username));
        }

        if (this.password != null) {
            props.put("password", runContext.render(this.password));
        }

        return props;
    }

    protected Connection connection(RunContext runContext) throws IllegalVariableEvaluationException, SQLException, IOException {
        registerDriver();

        Properties props = this.connectionProperties(runContext);
        String jdbcUrl = props.getProperty("jdbc.url");
        props.remove("jdbc.url");

        return DriverManager.getConnection(jdbcUrl, props);
    }
}
