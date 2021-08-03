package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

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

    @Schema(
        title = "If autocommit is enabled",
        description = "Sets this connection's auto-commit mode to the given state. If a connection is in auto-commit " +
            "mode, then all its SQL statements will be executed and committed as individual transactions. Otherwise, " +
            "its SQL statements are grouped into transactions that are terminated by a call to either the method commit" +
            "or the method rollback. By default, new connections are in auto-commit mode except if you are using a " +
            "`store` properties that will disabled autocommit whenever this properties values."
    )
    @PluginProperty(dynamic = false)
    protected final Boolean autoCommit = true;

    @Getter(AccessLevel.NONE)
    private transient Path cleanupDirectory;

    /**
     * JDBC driver may be auto-registered. See https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html
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

        if (this.username != null) {
            props.put("password", runContext.render(this.password));
        }

        return props;
    }

    protected Connection connection(RunContext runContext) throws IllegalVariableEvaluationException, SQLException, IOException {
        registerDriver();

        Properties props = this.connectionProperties(runContext);

        return DriverManager.getConnection(props.getProperty("jdbc.url"), props);
    }

    private Path tempDir() throws IOException {
        if (this.cleanupDirectory == null) {
            this.cleanupDirectory = Files.createTempDirectory("working-dir");
        }

        return this.cleanupDirectory;
    }

    protected void cleanup() throws IOException {
        if (cleanupDirectory != null) {
            FileUtils.deleteDirectory(cleanupDirectory.toFile());
        }
    }

    public Path tempFile(String content) throws IOException {
        Path tempFile = Files.createTempFile(this.tempDir(), null, null);

        Files.write(tempFile, content.getBytes(StandardCharsets.UTF_8));

        return tempFile;
    }
}
