package io.kestra.plugin.jdbc;

import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import java.util.Arrays;

/**
 * Splits SQL by parsing it into an AST with jOOQ and re-rendering each parsed statement.
 * <p>
 * Unlike {@link SqlSplitter}, this doesn't slice the original source text: it re-renders each
 * statement from its parsed AST, which lower-cases keywords, may re-quote identifiers and may
 * substitute types. jOOQ's OSS edition also doesn't parse several dialect-specific constructs
 * (e.g. Oracle PL/SQL blocks, Postgres dollar-quoted {@code DO} blocks). See {@link JdbcStatementSplitter}
 * for the fallback used to keep those cases safe.
 */
public class JooqSqlSplitter implements SqlStatementSplitter {

    @Override
    public String[] split(String sql) {
        Query[] queries = DSL.using(SQLDialect.DEFAULT).parser().parse(sql).queries();

        return Arrays.stream(queries)
            .map(Query::getSQL)
            .toArray(String[]::new);
    }
}
