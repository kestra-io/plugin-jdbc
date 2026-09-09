package io.kestra.plugin.jdbc;

/**
 * Splits a SQL string containing one or more statements into individual statements.
 */
public interface SqlStatementSplitter {

    String[] split(String sql);
}
