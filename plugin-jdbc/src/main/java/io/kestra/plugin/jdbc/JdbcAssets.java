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
import io.kestra.plugin.jdbc.SqlTableExtractor.Kind;
import io.kestra.plugin.jdbc.SqlTableExtractor.TableReference;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Builds the table assets (and their lineage) of a SQL statement.
 */
final class JdbcAssets {
    static final String TABLE_TYPE = "io.kestra.plugin.ee.assets.Table";
    private static final Set<String> SYSTEM_SCHEMAS = Set.of(
        "information_schema", "pg_catalog", "pg_toast", "sys", "sysibm", "syscat", "sysstat", "mysql",
        "performance_schema", "system", "sysdiagrams", "pg_temp"
    );

    private JdbcAssets() {
    }

    static void emit(RunContext runContext, AssetsDeclaration declaration, Connection connection, String statement, Long rowsAffected)
        throws IllegalVariableEvaluationException {
        var rEnableAuto = declaration != null && runContext.render(declaration.getEnableAuto()).as(Boolean.class).orElse(false);

        AssetEmit emit;
        try {
            emit = build(connection, statement, rowsAffected, rEnableAuto);
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
        var dialect = JdbcExecutionSupport.dialectOf(connection);
        var references = SqlTableExtractor.extract(statement, dialect).stream()
            .filter(JdbcAssets::isAssetCandidate)
            .toList();
        var hasCreate = references.stream().anyMatch(r -> r.getAccess() == Access.CREATE);
        if (references.isEmpty() || (!enableAuto && !hasCreate)) {
            return null;
        }

        var system = connection.getMetaData().getDatabaseProductName();
        var currentDatabase = AbstractJdbcBaseQuery.getDatabaseName(connection);
        var kind = SqlTableExtractor.statementKind(statement);

        var outputs = new ArrayList<Asset>();
        var inputs = new ArrayList<AssetIdentifier>();
        var outputIds = new HashSet<String>();

        for (var reference : references) {
            var isWrite = reference.getAccess() == Access.WRITE || reference.getAccess() == Access.CREATE;
            if (isWrite && (enableAuto || reference.getAccess() == Access.CREATE)) {
                var id = assetId(reference, currentDatabase);
                outputIds.add(id);
                outputs.add(Custom.builder()
                    .id(id)
                    .type(TABLE_TYPE)
                    .metadata(metadata(connection, reference, system, currentDatabase, kind, rowsAffected, enableAuto))
                    .build());
            }
        }

        if (enableAuto) {
            var inputIds = new LinkedHashSet<String>();
            for (var reference : references) {
                if (reference.getAccess() == Access.READ) {
                    var id = assetId(reference, currentDatabase);
                    if (!outputIds.contains(id) && inputIds.add(id)) {
                        inputs.add(new AssetIdentifier(null, null, id, TABLE_TYPE));
                    }
                }
            }
        }

        return inputs.isEmpty() && outputs.isEmpty() ? null : new AssetEmit(inputs, outputs);
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

    static String assetId(TableReference reference, String currentDatabase) {
        var database = reference.getDatabase() != null ? reference.getDatabase() : currentDatabase;
        return reference.getSchema() != null
            ? database + "." + reference.getSchema() + "." + reference.getName()
            : database + "." + reference.getName();
    }

    private static LinkedHashMap<String, Object> metadata(
        Connection connection,
        TableReference reference,
        String system,
        String currentDatabase,
        String kind,
        Long rowsAffected,
        boolean withColumns
    ) {
        var metadata = new LinkedHashMap<String, Object>();
        metadata.put("system", system);
        metadata.put("database", reference.getDatabase() != null ? reference.getDatabase() : currentDatabase);
        if (reference.getSchema() != null) {
            metadata.put("schema", reference.getSchema());
        }
        metadata.put("name", reference.getName());
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
                        column.put("jdbcType", jdbcTypeName(rs.getInt("DATA_TYPE")));
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

    private static String jdbcTypeName(int type) {
        try {
            return JDBCType.valueOf(type).getName();
        } catch (IllegalArgumentException e) {
            return String.valueOf(type);
        }
    }
}
