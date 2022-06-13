package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractCopy extends AbstractJdbcConnection implements PostgresConnectionInterface {
    @Builder.Default
    protected Boolean ssl = false;
    protected SslMode sslMode;
    protected String sslRootCert;
    protected String sslCert;
    protected String sslKey;
    protected String sslKeyPassword;

    @Schema(
        title = "The name (optionally schema-qualified) of an existing table."
    )
    @PluginProperty(dynamic = true)
    protected String table;

    @Schema(
        title = "An optional list of columns to be copied",
        description = "If no column list is specified, all columns of the table will be copied."
    )
    @PluginProperty(dynamic = false)
    protected List<String> columns;

    @Schema(
        title = "Selects the data format to be read or written"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    protected Format format = Format.TEXT;

    @Schema(
        title = "Specifies copying the OID for each row",
        description = "An error is raised if OIDS is specified for a table that does not have OIDs, or in the case of copying a query."
    )
    @PluginProperty(dynamic = false)
    protected Boolean oids;

    @Schema(
        title = "Requests copying the data with rows already frozen, just as they would be after running the VACUUM FREEZE command",
        description = "This is intended as a performance option for initial data loading. Rows will be frozen only if the table being loaded has been created or truncated in the current subtransaction, there are no cursors open and there are no older snapshots held by this transaction. It is currently not possible to perform a COPY FREEZE on a partitioned table.\n" +
            "\n" +
            "Note that all other sessions will immediately be able to see the data once it has been successfully loaded. This violates the normal rules of MVCC visibility and users specifying should be aware of the potential problems this might cause."
    )
    @PluginProperty(dynamic = false)
    protected Boolean freeze;

    @Schema(
        title = "Specifies the character that separates columns within each row (line) of the file",
        description = "The default is a tab character in text format, a comma in CSV format. This must be a single one-byte character. This option is not allowed when using binary"
    )
    @PluginProperty(dynamic = false)
    protected Character delimiter;

    @Schema(
        title = "Specifies the string that represents a null value",
        description = "The default is \\N (backslash-N) in text format, and an unquoted empty string in CSV format. You might prefer an empty string even in text format for cases where you don't want to distinguish nulls from empty strings. This option is not allowed when using binary format."
    )
    @PluginProperty(dynamic = false)
    protected String nullString;

    @Schema(
        title = "Specifies that the file contains a header line with the names of each column in the file",
        description = "On output, the first line contains the column names from the table, and on input, the first line is ignored. This option is allowed only when using CSV."
    )
    @PluginProperty(dynamic = false)
    protected Boolean header;

    @Schema(
        title = "Specifies the quoting character to be used when a data value is quoted.",
        description = "The default is double-quote. This must be a single one-byte character. This option is allowed only when using CSV format."
    )
    @PluginProperty(dynamic = false)
    protected Character quote;

    @Schema(
        title = "Specifies the character that should appear before a data character that matches the QUOTE value.",
        description = "The default is the same as the QUOTE value (so that the quoting character is doubled if it appears in the data). This must be a single one-byte character. This option is allowed only when using CSV format."
    )
    @PluginProperty(dynamic = false)
    protected Character escape;

    @Schema(
        title = "Forces quoting to be used for all non-NULL values in each specified column",
        description = "NULL output is never quoted. If * is specified, non-NULL values will be quoted in all columns. This option is allowed only in COPY TO, and only when using CSV format."
    )
    @PluginProperty(dynamic = false)
    protected List<String> forceQuote;

    @Schema(
        title = "Do not match the specified columns' values against the null string",
        description = "In the default case where the null string is empty, this means that empty values will be read as zero-length strings rather than nulls, even when they are not quoted. This option is allowed only in COPY FROM, and only when using CSV format."
    )
    @PluginProperty(dynamic = false)
    protected List<String> forceNotNull;

    @Schema(
        title = "Match the specified columns' values against the null string, even if it has been quoted, and if a match is found set the value to NULL",
        description = "In the default case where the null string is empty, this converts a quoted empty string into NULL. This option is allowed only in COPY FROM, and only when using CSV format."
    )
    @PluginProperty(dynamic = false)
    protected List<String> forceNull;

    @Schema(
        title = "Specifies that the file is encoded in the encoding_name",
        description = "If this option is omitted, the current client encoding is used. See the Notes below for more details."
    )
    @PluginProperty(dynamic = false)
    protected String encoding;

    public enum Format {
        TEXT,
        CSV,
        BINARY
    }

    @Override
    protected Properties connectionProperties(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        Properties properties = super.connectionProperties(runContext);
        PostgresService.handleSsl(properties, runContext, this);

        return properties;
    }

    @Override
    protected void registerDriver() throws SQLException {
        DriverManager.registerDriver(new org.postgresql.Driver());
    }

    protected String query(RunContext runContext, String query, String dest) throws IllegalVariableEvaluationException {
        List<String> sql = new ArrayList<>();
        List<String> options = new ArrayList<>();

        sql.add("COPY");

        if (query == null) {
            sql.add(runContext.render(this.table));

            if (this.columns != null) {
                sql.add("(" + String.join(", ", this.columns) + ")");
            }
        } else {
            sql.add("(" + runContext.render(query) + ")");
        }

        sql.add(dest);

        if (this.format != Format.TEXT) {
            options.add("FORMAT " + this.format.name());
        }

        if (oids != null && this.oids) {
            options.add("OIDS");
        }

        if (freeze != null && this.freeze) {
            options.add("FREEZE");
        }

        if (delimiter != null) {
            options.add("DELIMITER '" + this.delimiter + "'");
        }

        if (nullString != null) {
            options.add("NULL '" + this.nullString + "'");
        }

        if (header != null) {
            options.add("HEADER " + (this.header ? "TRUE" : "FALSE"));
        }

        if (quote != null) {
            options.add("QUOTE '" + this.quote + "'");
        }

        if (escape != null) {
            options.add("ESCAPE '" + this.escape + "'");
        }

        if (forceQuote != null) {
            options.add("FORCE_QUOTE " + (this.forceQuote.size() == 1 && this.forceQuote.get(0).equals("*") ? "*" :  "(" + String.join(", ", this.forceQuote) + ")"));
        }

        if (forceNotNull != null) {
            options.add("FORCE_NOT_NULL (" + String.join(", ", this.forceNotNull) + ")");
        }

        if (forceNull != null) {
            options.add("FORCE_NULL (" + String.join(", ", this.forceNull) + ")");
        }

        if (encoding != null) {
            options.add("ENCODING '" + this.encoding + "'");
        }

        if (options.size() > 0) {
            sql.add("WITH (" +  String.join(", ", options) + ")");
        }

        return String.join(" ", sql);
    }
}
