package io.kestra.plugin.jdbc.postgresql;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.JdbcConnectionInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

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
public abstract class AbstractCopy extends Task implements PostgresConnectionInterface {
    private Property<String> url;
    private Property<String> username;
    private Property<String> password;
    @Builder.Default
    protected Property<Boolean> ssl = Property.of(false);
    protected Property<SslMode> sslMode;
    protected Property<String> sslRootCert;
    protected Property<String> sslCert;
    protected Property<String> sslKey;
    protected Property<String> sslKeyPassword;

    @Schema(
        title = "The name (optionally schema-qualified) of an existing table."
    )
    protected Property<String> table;

    @Schema(
        title = "An optional list of columns to be copied.",
        description = "If no column list is specified, all columns of the table will be copied."
    )
    protected Property<List<String>> columns;

    @Schema(
        title = "Selects the data format to be read or written."
    )
    @Builder.Default
    protected Property<Format> format = Property.of(Format.TEXT);

    @Schema(
        title = "Specifies copying the OID for each row.",
        description = "An error is raised if OIDs is specified for a table that does not have OIDs, or in the case of copying a query."
    )
    protected Property<Boolean> oids;

    @Schema(
        title = "Requests copying the data with rows already frozen, just as they would be after running the VACUUM FREEZE command.",
        description = "This is intended as a performance option for initial data loading. Rows will be frozen only if the table being loaded has been created or truncated in the current sub-transaction, there are no cursors open and there are no older snapshots held by this transaction. It is currently not possible to perform a COPY FREEZE on a partitioned table.\n" +
            "\n" +
            "Note that all other sessions will immediately be able to see the data once it has been successfully loaded. This violates the normal rules of MVCC visibility and users specifying should be aware of the potential problems this might cause."
    )
    protected Property<Boolean> freeze;

    @Schema(
        title = "Specifies the character that separates columns within each row (line) of the file.",
        description = "The default is a tab character in text format, a comma in CSV format. This must be a single one-byte character. This option is not allowed when using binary."
    )
    protected Property<Character> delimiter;

    @Schema(
        title = "Specifies the string that represents a null value.",
        description = "The default is \\N (backslash-N) in text format, and an unquoted empty string in CSV format. You might prefer an empty string even in text format for cases where you don't want to distinguish nulls from empty strings. This option is not allowed when using binary format."
    )
    protected Property<String> nullString;

    @Schema(
        title = "Specifies that the file contains a header line with the names of each column in the file.",
        description = "On output, the first line contains the column names from the table, and on input, the first line is ignored. This option is allowed only when using CSV."
    )
    protected Property<Boolean> header;

    @Schema(
        title = "Specifies the quoting character to be used when a data value is quoted.",
        description = "The default is double-quote. This must be a single one-byte character. This option is allowed only when using CSV format."
    )
    protected Property<Character> quote;

    @Schema(
        title = "Specifies the character that should appear before a data character that matches the QUOTE value.",
        description = "The default is the same as the QUOTE value (so that the quoting character is doubled if it appears in the data). This must be a single one-byte character. This option is allowed only when using CSV format."
    )
    protected Property<Character> escape;

    @Schema(
        title = "Forces quoting to be used for all non-NULL values in each specified column.",
        description = "NULL output is never quoted. If * is specified, non-NULL values will be quoted in all columns. This option is allowed only in COPY TO, and only when using CSV format."
    )
    protected Property<List<String>> forceQuote;

    @Schema(
        title = "Do not match the specified columns' values against the null string.",
        description = "In the default case where the null string is empty, this means that empty values will be read as zero-length strings rather than nulls, even when they are not quoted. This option is allowed only in COPY FROM, and only when using CSV format."
    )
    protected Property<List<String>> forceNotNull;

    @Schema(
        title = "Match the specified columns' values against the null string, even if it has been quoted, and if a match is found set the value to NULL.",
        description = "In the default case where the null string is empty, this converts a quoted empty string into NULL. This option is allowed only in COPY FROM, and only when using CSV format."
    )
    protected Property<List<String>> forceNull;

    @Schema(
        title = "Specifies that the file is encoded in the encoding_name.",
        description = "If this option is omitted, the current client encoding is used. See the Notes below for more details."
    )
    protected Property<String> encoding;

    public enum Format {
        TEXT,
        CSV,
        BINARY
    }

    @Override
    public Properties connectionProperties(RunContext runContext) throws Exception {
        Properties properties = PostgresConnectionInterface.super.connectionProperties(runContext);
        PostgresService.handleSsl(properties, runContext, this);

        return properties;
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new org.postgresql.Driver());
    }

    protected String query(RunContext runContext, String query, String dest) throws IllegalVariableEvaluationException {
        List<String> sql = new ArrayList<>();
        List<String> options = new ArrayList<>();

        sql.add("COPY");

        if (query == null) {
            sql.add(runContext.render(this.table).as(String.class).orElse(null));

            var columnsValue = runContext.render(this.columns).asList(String.class);
            if (!columnsValue.isEmpty()) {
                sql.add("(" + String.join(", ", columnsValue) + ")");
            }
        } else {
            sql.add("(" + runContext.render(query) + ")");
        }

        sql.add(dest);

        var formatValue = runContext.render(this.format).as(Format.class).orElseThrow();
        if (formatValue != Format.TEXT) {
            options.add("FORMAT " + formatValue.name());
        }

        if (oids != null && runContext.render(this.oids).as(Boolean.class).orElseThrow()) {
            options.add("OIDS");
        }

        if (freeze != null && runContext.render(this.freeze).as(Boolean.class).orElseThrow()) {
            options.add("FREEZE");
        }

        if (delimiter != null) {
            options.add("DELIMITER '" + runContext.render(this.delimiter).as(Character.class).orElseThrow() + "'");
        }

        if (nullString != null) {
            options.add("NULL '" + runContext.render(this.nullString).as(String.class).orElseThrow() + "'");
        }

        if (header != null) {
            options.add("HEADER " + (runContext.render(this.header).as(Boolean.class).orElseThrow() ? "TRUE" : "FALSE"));
        }

        if (quote != null) {
            options.add("QUOTE '" + runContext.render(this.quote).as(Character.class).orElseThrow() + "'");
        }

        if (escape != null) {
            options.add("ESCAPE '" + runContext.render(this.escape).as(Character.class).orElseThrow() + "'");
        }

        var forceQuoteListValue = runContext.render(this.forceQuote).asList(String.class);
        if (!forceQuoteListValue.isEmpty()) {
            options.add("FORCE_QUOTE " + (forceQuoteListValue.size() == 1 && forceQuoteListValue.get(0).equals("*") ? "*" :  "(" + String.join(", ", forceQuoteListValue) + ")"));
        }

        var forceNotNullListValue = runContext.render(this.forceNotNull).asList(String.class);
        if (!forceNotNullListValue.isEmpty()) {
            options.add("FORCE_NOT_NULL (" + String.join(", ", forceNotNullListValue) + ")");
        }

        var forceNullListValue = runContext.render(this.forceNull).asList(String.class);
        if (!forceNullListValue.isEmpty()) {
            options.add("FORCE_NULL (" + String.join(", ", forceNullListValue) + ")");
        }

        if (encoding != null) {
            options.add("ENCODING '" + runContext.render(this.encoding).as(String.class).orElseThrow() + "'");
        }

        if (options.size() > 0) {
            sql.add("WITH (" +  String.join(", ", options) + ")");
        }

        return String.join(" ", sql);
    }
}
