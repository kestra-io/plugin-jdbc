package io.kestra.plugin.jdbc;

/**
 * Production entry point used by {@code AbstractJdbcQueries} and {@code AbstractJdbcQuery} to split
 * a multi-statement SQL string.
 * <p>
 * jOOQ's parser was evaluated as a replacement for {@link SqlSplitter}
 * (https://github.com/kestra-io/plugin-jdbc/issues/782) and run against the full corpus of real
 * inputs. That run confirmed jOOQ's OSS edition re-renders every statement from its parsed AST
 * instead of slicing the original source text, which:
 * <ul>
 *     <li>re-cases every unquoted identifier to uppercase, which breaks execution against
 *     case-sensitive MySQL installations (the Linux default);</li>
 *     <li>re-renders backtick- (MySQL/MariaDB) and bracket-quoted (SQL Server) identifiers as
 *     double-quoted ones;</li>
 *     <li>can silently drop no-op statements (e.g. a plain {@code NULL;}) nested inside a
 *     {@code BEGIN...END} block;</li>
 *     <li>doesn't parse Postgres dollar-quoted string literals/{@code DO} blocks, or most Oracle
 *     DDL with a labelled {@code END}, at all in the OSS edition.</li>
 * </ul>
 * None of this is safe to execute against a real database, so this class doesn't route to jOOQ at
 * all: it delegates to the source-preserving {@link SqlSplitter}, which can only ever return
 * substrings of the original input and therefore can't corrupt or drop anything.
 */
public final class JdbcStatementSplitter {

    private JdbcStatementSplitter() {
    }

    public static String[] split(String sql) {
        return SqlSplitter.getQueries(sql);
    }
}
