package io.kestra.plugin.jdbc.snowflake;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

final class SnowflakeCompatibility {
    private static final String API_CONNECTION_CLASS = "net.snowflake.client.api.connection.SnowflakeConnection";
    private static final String LEGACY_CONNECTION_CLASS = "net.snowflake.client.jdbc.SnowflakeConnection";
    private static final String UPLOAD_STREAM_CONFIG_CLASS = "net.snowflake.client.api.connection.UploadStreamConfig";
    private static final String DOWNLOAD_STREAM_CONFIG_CLASS = "net.snowflake.client.api.connection.DownloadStreamConfig";

    private SnowflakeCompatibility() {
    }

    static void uploadStream(
        Connection connection,
        String stageName,
        String prefix,
        InputStream inputStream,
        String fileName,
        boolean compress
    ) throws SQLException {
        var snowflakeConnection = unwrapSnowflakeConnection(connection);

        if (tryUploadWithConfig(snowflakeConnection, stageName, prefix, inputStream, fileName, compress)) {
            return;
        }

        if (tryLegacyUpload(snowflakeConnection, stageName, prefix, inputStream, fileName, compress)) {
            return;
        }

        throw new IllegalStateException("Unsupported Snowflake JDBC driver API: unable to call uploadStream");
    }

    static InputStream downloadStream(
        Connection connection,
        String stageName,
        String fileName,
        boolean decompress
    ) throws SQLException {
        var snowflakeConnection = unwrapSnowflakeConnection(connection);

        var stream = tryDownloadWithConfig(snowflakeConnection, stageName, fileName, decompress);
        if (stream != null) {
            return stream;
        }

        stream = tryLegacyDownload(snowflakeConnection, stageName, fileName, decompress);
        if (stream != null) {
            return stream;
        }

        stream = tryDownloadWithoutConfig(snowflakeConnection, stageName, fileName);
        if (stream != null) {
            return stream;
        }

        throw new IllegalStateException("Unsupported Snowflake JDBC driver API: unable to call downloadStream");
    }

    private static Object unwrapSnowflakeConnection(Connection connection) throws SQLException {
        for (var className : List.of(API_CONNECTION_CLASS, LEGACY_CONNECTION_CLASS)) {
            try {
                var snowflakeClass = Class.forName(className);
                if (connection.isWrapperFor(snowflakeClass)) {
                    return connection.unwrap(snowflakeClass);
                }
            } catch (ClassNotFoundException ignored) {
            } catch (SQLException ignored) {
                // Try next known connection API.
            }
        }

        return connection;
    }

    private static boolean tryUploadWithConfig(
        Object snowflakeConnection,
        String stageName,
        String prefix,
        InputStream inputStream,
        String fileName,
        boolean compress
    ) throws SQLException {
        try {
            var uploadConfigClass = Class.forName(UPLOAD_STREAM_CONFIG_CLASS);
            var builder = uploadConfigClass.getMethod("builder").invoke(null);
            var builderClass = builder.getClass();
            builderClass.getMethod("setDestPrefix", String.class).invoke(builder, prefix);
            builderClass.getMethod("setCompressData", boolean.class).invoke(builder, compress);
            var uploadConfig = builderClass.getMethod("build").invoke(builder);

            snowflakeConnection
                .getClass()
                .getMethod("uploadStream", String.class, String.class, InputStream.class, uploadConfigClass)
                .invoke(snowflakeConnection, stageName, fileName, inputStream, uploadConfig);

            return true;
        } catch (ClassNotFoundException | NoSuchMethodException ignored) {
            return false;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw asSqlOrIllegalState(e);
        }
    }

    private static boolean tryLegacyUpload(
        Object snowflakeConnection,
        String stageName,
        String prefix,
        InputStream inputStream,
        String fileName,
        boolean compress
    ) throws SQLException {
        try {
            snowflakeConnection
                .getClass()
                .getMethod("uploadStream", String.class, String.class, InputStream.class, String.class, boolean.class)
                .invoke(snowflakeConnection, stageName, prefix, inputStream, fileName, compress);

            return true;
        } catch (NoSuchMethodException ignored) {
            return false;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw asSqlOrIllegalState(e);
        }
    }

    private static InputStream tryDownloadWithConfig(
        Object snowflakeConnection,
        String stageName,
        String fileName,
        boolean decompress
    ) throws SQLException {
        try {
            var downloadConfigClass = Class.forName(DOWNLOAD_STREAM_CONFIG_CLASS);
            var builder = downloadConfigClass.getMethod("builder").invoke(null);
            var builderClass = builder.getClass();
            builderClass.getMethod("setDecompress", boolean.class).invoke(builder, decompress);
            var downloadConfig = builderClass.getMethod("build").invoke(builder);

            return (InputStream) snowflakeConnection
                .getClass()
                .getMethod("downloadStream", String.class, String.class, downloadConfigClass)
                .invoke(snowflakeConnection, stageName, fileName, downloadConfig);
        } catch (ClassNotFoundException | NoSuchMethodException ignored) {
            return null;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw asSqlOrIllegalState(e);
        }
    }

    private static InputStream tryLegacyDownload(
        Object snowflakeConnection,
        String stageName,
        String fileName,
        boolean decompress
    ) throws SQLException {
        try {
            return (InputStream) snowflakeConnection
                .getClass()
                .getMethod("downloadStream", String.class, String.class, boolean.class)
                .invoke(snowflakeConnection, stageName, fileName, decompress);
        } catch (NoSuchMethodException ignored) {
            return null;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw asSqlOrIllegalState(e);
        }
    }

    private static InputStream tryDownloadWithoutConfig(
        Object snowflakeConnection,
        String stageName,
        String fileName
    ) throws SQLException {
        try {
            return (InputStream) snowflakeConnection
                .getClass()
                .getMethod("downloadStream", String.class, String.class)
                .invoke(snowflakeConnection, stageName, fileName);
        } catch (NoSuchMethodException ignored) {
            return null;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw asSqlOrIllegalState(e);
        }
    }

    private static SQLException asSqlOrIllegalState(Exception exception) throws SQLException {
        var cause = exception instanceof InvocationTargetException invocationTargetException ? invocationTargetException.getCause() : exception;
        if (cause instanceof SQLException sqlException) {
            throw sqlException;
        }

        throw new IllegalStateException("Unable to execute Snowflake JDBC API compatibility call", cause);
    }
}
