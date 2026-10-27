package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.assets.Asset;
import io.kestra.core.models.assets.AssetIdentifier;
import io.kestra.core.models.assets.AssetsDeclaration;
import io.kestra.core.models.assets.Custom;
import io.kestra.core.models.tasks.AssetFailureBehavior;
import io.kestra.core.queues.QueueException;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.SqlTableExtractor.Access;
import io.kestra.plugin.jdbc.SqlTableExtractor.Confidence;
import io.kestra.plugin.jdbc.SqlTableExtractor.Kind;
import io.kestra.plugin.jdbc.SqlTableExtractor.TableReference;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.UnaryOperator;

// Builds the table assets and their lineage for a SQL statement.
final class JdbcAssets {
    static final String TABLE_TYPE = "io.kestra.plugin.ee.assets.Table";
    private static final Set<String> SYSTEM_SCHEMAS = Set.of(
        "information_schema", "pg_catalog", "pg_toast", "sys", "sysibm", "syscat", "sysstat", "mysql",
        "performance_schema", "system", "sysdiagrams", "pg_temp"
    );

    private JdbcAssets() {
    }

    record ExecutedStatement(String sql, Long rowsAffected) {
    }

    static void emit(RunContext runContext, AssetsDeclaration declaration, Connection connection, String statement, Long rowsAffected)
        throws IllegalVariableEvaluationException {
        emit(runContext, declaration, connection, List.of(new ExecutedStatement(statement, rowsAffected)));
    }

    // One emit per task run, so the database lookups are made once whatever the number of statements.
    static void emit(RunContext runContext, AssetsDeclaration declaration, Connection connection, List<ExecutedStatement> statements)
        throws IllegalVariableEvaluationException {
        var rEnableAuto = declaration != null && runContext.render(declaration.getEnableAuto()).as(Boolean.class).orElse(false);

        AssetEmit emit;
        try {
            emit = build(connection, statements, rEnableAuto);
        } catch (SQLException | RuntimeException e) {
            onFailure(runContext, declaration, e);
            return;
        }
        if (emit == null) {
            return;
        }

        try {
            runContext.assets().emit(emit);
            runContext.logger().info("Emitted {} input asset(s) and {} output asset(s)", emit.inputs().size(), emit.outputs().size());
        } catch (UnsupportedOperationException | NullPointerException ignored) {
            // OSS edition or tests where EE assets are not available: silently skip.
        } catch (QueueException | RuntimeException e) {
            onFailure(runContext, declaration, e);
        }
    }

    static void onFailure(RunContext runContext, AssetsDeclaration declaration, Exception e) throws IllegalVariableEvaluationException {
        var rAssetFailureBehavior = declaration == null
            ? null
            : runContext.render(declaration.getAssetFailureBehavior()).as(AssetFailureBehavior.class).orElse(null);
        if (rAssetFailureBehavior == AssetFailureBehavior.FAIL) {
            throw new IllegalStateException("Unable to emit assets: " + e.getMessage(), e);
        }
        runContext.logger().warn("Unable to emit assets: {}", e.getMessage());
    }

    // Returns null when the statement touches no table worth an asset.
    static AssetEmit build(Connection connection, String statement, Long rowsAffected, boolean enableAuto) throws SQLException {
        return build(connection, List.of(new ExecutedStatement(statement, rowsAffected)), enableAuto);
    }

    private record Written(TableReference reference, String kind, Long rowsAffected) {
    }

    static AssetEmit build(Connection connection, List<ExecutedStatement> statements, boolean enableAuto) throws SQLException {
        var dialect = JdbcExecutionSupport.dialectOf(connection);
        Naming naming = null;
        var temporaryTables = new HashSet<String>();
        var written = new LinkedHashMap<String, Written>();
        var read = new LinkedHashSet<String>();

        for (var executed : statements) {
            var kind = SqlTableExtractor.statementKind(executed.sql());
            for (var reference : SqlTableExtractor.extract(executed.sql(), dialect)) {
                if (reference.getAccess() == Access.CREATE && reference.getConfidence() == Confidence.LOW) {
                    // Temporary tables only live for the session, so neither they nor their later reads are assets.
                    temporaryTables.add(reference.getName().toLowerCase(Locale.ROOT));
                    continue;
                }
                if (!isAssetCandidate(reference) || (reference.getSchema() == null && temporaryTables.contains(reference.getName().toLowerCase(Locale.ROOT)))) {
                    continue;
                }
                var isCreate = reference.getAccess() == Access.CREATE;
                var isWrite = isCreate || reference.getAccess() == Access.WRITE;
                if (!isWrite && reference.getAccess() != Access.READ) {
                    continue;
                }
                if (!enableAuto && !isCreate) {
                    continue;
                }
                naming = naming == null ? Naming.of(connection) : naming;
                var id = naming.assetId(reference);
                if (isWrite) {
                    written.put(id, new Written(reference, kind, executed.rowsAffected()));
                } else if (enableAuto) {
                    read.add(id);
                }
            }
        }

        if (naming == null || written.isEmpty() && read.isEmpty()) {
            return null;
        }

        var outputs = new ArrayList<Asset>();
        for (var entry : written.entrySet()) {
            outputs.add(Custom.builder()
                .id(entry.getKey())
                .type(TABLE_TYPE)
                .metadata(metadata(connection, entry.getValue().reference(), naming, entry.getValue().kind(), entry.getValue().rowsAffected(), enableAuto))
                .build());
        }
        var inputs = new ArrayList<AssetIdentifier>();
        for (var id : read) {
            if (!written.containsKey(id)) {
                inputs.add(new AssetIdentifier(null, null, id, TABLE_TYPE));
            }
        }

        return inputs.isEmpty() && outputs.isEmpty() ? null : new AssetEmit(inputs, outputs);
    }

    // The id is built from what the database resolves, not from how the SQL is written, so lineage links up:
    // a missing schema is the current one and unquoted names are folded the way the database folds them.
    private record Naming(String system, String database, String schema, UnaryOperator<String> fold) {
        static Naming of(Connection connection) throws SQLException {
            var metaData = connection.getMetaData();
            UnaryOperator<String> fold;
            if (metaData.storesUpperCaseIdentifiers()) {
                fold = name -> name.toUpperCase(Locale.ROOT);
            } else if (metaData.storesLowerCaseIdentifiers() || !metaData.supportsMixedCaseIdentifiers()) {
                fold = name -> name.toLowerCase(Locale.ROOT);
            } else {
                fold = name -> name;
            }
            String schema = null;
            try {
                schema = connection.getSchema();
            } catch (SQLException | RuntimeException | AbstractMethodError ignored) {
                // Drivers without schema support yield ids without one.
            }
            return new Naming(metaData.getDatabaseProductName(), AbstractJdbcBaseQuery.getDatabaseName(connection), schema, fold);
        }

        String database(TableReference reference) {
            if (reference.getDatabase() == null) {
                return database;
            }
            return reference.isQuotedDatabase() ? reference.getDatabase() : fold.apply(reference.getDatabase());
        }

        String schema(TableReference reference) {
            if (reference.getSchema() != null) {
                return reference.isQuotedSchema() ? reference.getSchema() : fold.apply(reference.getSchema());
            }
            return reference.getDatabase() == null ? schema : null;
        }

        String name(TableReference reference) {
            return reference.isQuotedName() ? reference.getName() : fold.apply(reference.getName());
        }

        String assetId(TableReference reference) {
            var schema = schema(reference);
            return schema != null
                ? database(reference) + "." + schema + "." + name(reference)
                : database(reference) + "." + name(reference);
        }
    }

    private static boolean isAssetCandidate(TableReference reference) {
        if (reference.getAccess() == Access.DROP) {
            return false;
        }
        if (reference.getKind() == Kind.FILE || reference.getKind() == Kind.URL) {
            return false;
        }
        var name = reference.getName();
        if (name.startsWith("#") || name.startsWith("@") || name.toLowerCase(Locale.ROOT).startsWith("sqlite_")) {
            return false;
        }
        return !isSystemSchema(reference.getSchema()) && !isSystemSchema(reference.getDatabase());
    }

    private static boolean isSystemSchema(String schema) {
        return schema != null && SYSTEM_SCHEMAS.contains(schema.toLowerCase(Locale.ROOT));
    }

    private static LinkedHashMap<String, Object> metadata(
        Connection connection,
        TableReference reference,
        Naming naming,
        String kind,
        Long rowsAffected,
        boolean withColumns
    ) {
        var metadata = new LinkedHashMap<String, Object>();
        metadata.put("system", naming.system());
        metadata.put("database", naming.database(reference));
        var schema = naming.schema(reference);
        if (schema != null) {
            metadata.put("schema", schema);
        }
        metadata.put("name", naming.name(reference));
        metadata.put("lastStatementKind", kind);
        if (rowsAffected != null) {
            metadata.put("rowsAffected", rowsAffected);
        }
        if (withColumns) {
            var columns = columns(connection, reference);
            if (!columns.isEmpty()) {
                metadata.put("columns", columns);
            }
        }
        return metadata;
    }

    // Best effort: drivers without metadata support just yield no columns.
    private static List<LinkedHashMap<String, Object>> columns(Connection connection, TableReference reference) {
        try {
            var metaData = connection.getMetaData();
            for (var name : List.of(reference.getName(), reference.getName().toUpperCase(Locale.ROOT), reference.getName().toLowerCase(Locale.ROOT))) {
                var columns = new ArrayList<LinkedHashMap<String, Object>>();
                try (var rs = metaData.getColumns(reference.getDatabase(), reference.getSchema(), name, null)) {
                    while (rs.next()) {
                        var column = new LinkedHashMap<String, Object>();
                        column.put("name", rs.getString("COLUMN_NAME"));
                        column.put("jdbcType", JdbcExecutionSupport.jdbcTypeName(rs.getInt("DATA_TYPE")));
                        column.put("nullable", rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);
                        columns.add(column);
                    }
                }
                if (!columns.isEmpty()) {
                    return columns;
                }
            }
        } catch (SQLException | RuntimeException e) {
            return List.of();
        }
        return List.of();
    }
}
