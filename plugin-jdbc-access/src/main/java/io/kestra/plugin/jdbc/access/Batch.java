package io.kestra.plugin.jdbc.access;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import net.ucanaccess.jdbc.UcanaccessDriver;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
    title = "Bulk insert rows into a Microsoft Access database using prepared statements",
    description = "Reads ION-formatted data from Kestra internal storage and performs high-performance batch inserts into a Microsoft Access database via the UCanAccess JDBC driver. Data is processed in chunks (default 1,000 rows) to optimize memory and performance."
)
@Plugin(
    examples = {
        @Example(
            title = "Fetch rows from a query and bulk insert them into an Access table.",
            full = true,
            code = """
                id: access_batch
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.jdbc.access.Query
                    url: jdbc:ucanaccess:///myfile.accdb
                    outputDbFile: true
                    sql: |
                      SELECT product_id, product_name, price
                      FROM products
                      LIMIT 1500
                    fetchType: STORE

                  - id: insert
                    type: io.kestra.plugin.jdbc.access.Batch
                    from: "{{ outputs.query.uri }}"
                    accessFile: "{{ outputs.query.databaseUri }}"
                    sql: INSERT INTO products_copy VALUES (?, ?, ?)
                """
        )
    },
    metrics = {
        @Metric(
            name = "records",
            type = Counter.TYPE,
            unit = "records",
            description = "The number of records processed."
        ),
        @Metric(
            name = "updated",
            type = Counter.TYPE,
            unit = "records",
            description = "The number of records updated."
        ),
        @Metric(
            name = "query",
            type = Counter.TYPE,
            unit = "queries",
            description = "The number of batch queries executed."
        )
    }
)
public class Batch extends AbstractJdbcBatch implements AccessQueryInterface {

    @PluginProperty(group = "source")
    @Schema(
        title = "Access database file (optional)",
        description = """
            Optional URI to an existing Access database file stored in Kestra internal storage.

            When provided, the file is downloaded into the task working directory and used
            as the Access database for the batch insert.
            """
    )
    protected Property<String> accessFile;

    @Builder.Default
    @PluginProperty(group = "source")
    @Schema(
        title = "Access database version to use when creating a new database file",
        description = """
            Specifies the Microsoft Access file format used when UCanAccess creates a new database.
            Only applies when the target database file does not already exist.
            See https://spannm.github.io/ucanaccess/20-getting-started.html for details.
            """,
        defaultValue = "V2003"
    )
    private Property<AccessVersion> newDatabaseVersion = Property.ofValue(AccessVersion.V2003);

    @Getter(AccessLevel.NONE)
    protected transient Path databaseFile;

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new AccessCellConverter(zoneId);
    }

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        var props = super.connectionProperties(runContext);
        var version = runContext.render(this.newDatabaseVersion).as(AccessVersion.class).orElse(AccessVersion.V2003);

        if (this.databaseFile != null) {
            props.put("jdbc.url", "jdbc:ucanaccess://" + this.databaseFile.toAbsolutePath());
        }

        return AccessQueryUtils.buildAccessProperties(props, runContext, version);
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rAccessFile = runContext.render(this.accessFile).as(String.class);

        if (rAccessFile.isPresent()) {
            Path workingDir = runContext.workingDir().path();
            this.databaseFile = workingDir.resolve("database.accdb");
            try (var input = runContext.storage().getFile(URI.create(rAccessFile.get()))) {
                Files.copy(input, this.databaseFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } else {
            this.databaseFile = null;
        }

        return super.run(runContext);
    }

    @Override
    public void registerDriver() throws SQLException {
        if (DriverManager.drivers().noneMatch(UcanaccessDriver.class::isInstance)) {
            DriverManager.registerDriver(new UcanaccessDriver());
        }
    }
}
