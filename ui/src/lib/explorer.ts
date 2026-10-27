export type Engine = "duckdb" | "sqlite";
export type ColumnKind = "numeric" | "date" | "text";

export const MAX_LIMIT = 1000;
export const BUCKETS = 10;

export type QueryResult = {
    columns: { name: string; type: string }[];
    rows: unknown[][];
    hasMore: boolean;
    durationMs: number;
};

export type Catalog = {
    tables: { schema: string; name: string; type: string; columns: { name: string; type: string }[] }[];
};

export function engineOf(taskType?: string): Engine | undefined {
    if (taskType?.startsWith("io.kestra.plugin.jdbc.duckdb.")) return "duckdb";
    if (taskType?.startsWith("io.kestra.plugin.jdbc.sqlite.")) return "sqlite";
    return undefined;
}

export class EndpointError extends Error {}

export const quoteIdentifier = (name: string) => `"${name.replace(/"/g, '""')}"`;

export function stripTrailingSemicolon(sql: string): string {
    return sql.trim().replace(/;+\s*$/, "");
}

export function columnKind(type: string): ColumnKind {
    const t = type.toUpperCase();
    if (/(INT|DEC|NUM|FLOA|DOUBLE|REAL)/.test(t)) return "numeric";
    if (/(DATE|TIME)/.test(t)) return "date";
    return "text";
}

export const previewSql = (table: string) => `SELECT * FROM ${quoteIdentifier(table)} LIMIT 100`;

export type Sample = { key: string; sql: string };

export function samples(engine: Engine, table?: string): Sample[] {
    const t = table ? quoteIdentifier(table) : "my_table";
    const count: Sample = { key: "count", sql: `SELECT count(*) AS total FROM ${t}` };
    const specific: Sample[] =
        engine === "duckdb"
            ? [
                  { key: "describe", sql: `SELECT * FROM (DESCRIBE ${t})` },
                  { key: "summarize", sql: `SELECT * FROM (SUMMARIZE ${t})` },
              ]
            : [{ key: "tableInfo", sql: `PRAGMA table_info(${table ? `'${table.replace(/'/g, "''")}'` : "'my_table'"})` }];
    return [...specific, count];
}

const source = (sql: string) => `(${stripTrailingSemicolon(sql)}) q`;

export const countSql = (sql: string) => `SELECT count(*) AS total FROM ${source(sql)}`;

// DuckDB summarizes natively; SQLite has no SUMMARIZE so the same figures come from plain aggregates.
export function summarySql(engine: Engine, sql: string, column: string, kind: ColumnKind): string {
    const c = quoteIdentifier(column);
    if (engine === "duckdb") {
        return `SELECT * FROM (SUMMARIZE SELECT ${c} FROM ${source(sql)})`;
    }
    const avg = kind === "numeric" ? `, avg(${c}) AS avg` : "";
    return `SELECT count(*) AS count, count(${c}) AS non_null, count(DISTINCT ${c}) AS distinct_values, min(${c}) AS min, max(${c}) AS max${avg} FROM ${source(sql)}`;
}

export function histogramSql(engine: Engine, sql: string, column: string, kind: ColumnKind): string | undefined {
    const c = quoteIdentifier(column);
    if (kind === "text") {
        return `SELECT ${c} AS value, count(*) AS n FROM ${source(sql)} WHERE ${c} IS NOT NULL GROUP BY ${c} ORDER BY n DESC LIMIT 3`;
    }
    const x =
        kind === "numeric"
            ? engine === "duckdb"
                ? `CAST(${c} AS DOUBLE)`
                : `CAST(${c} AS REAL)`
            : engine === "duckdb"
              ? `epoch(CAST(${c} AS TIMESTAMP))`
              : `julianday(${c})`;
    const scaled = `CASE WHEN s.mx = s.mn THEN 0 ELSE (v.x - s.mn) * ${BUCKETS}.0 / (s.mx - s.mn) END`;
    const bucket = engine === "duckdb" ? `CAST(floor(${scaled}) AS INTEGER)` : `CAST(${scaled} AS INTEGER)`;
    return (
        `SELECT ${bucket} AS bucket, count(*) AS n ` +
        `FROM (SELECT ${x} AS x FROM ${source(sql)}) v, ` +
        `(SELECT min(x) AS mn, max(x) AS mx FROM (SELECT ${x} AS x FROM ${source(sql)})) s ` +
        `WHERE v.x IS NOT NULL GROUP BY bucket ORDER BY bucket`
    );
}

export function bucketCounts(rows: unknown[][]): number[] {
    const counts = new Array<number>(BUCKETS).fill(0);
    for (const [bucket, n] of rows) {
        const index = Math.min(BUCKETS - 1, Math.max(0, Number(bucket)));
        counts[index] += Number(n);
    }
    return counts;
}

export type ColumnStats = {
    count?: number;
    nullPercent?: number;
    nonNull?: number;
    distinct?: number;
    min?: unknown;
    max?: unknown;
    avg?: unknown;
    bars: number[];
    top: { value: string; percent: number; other?: boolean }[];
};

// Maps the first summary row (by column label, per engine) and the histogram rows to what a card shows.
export function toColumnStats(
    engine: Engine,
    kind: ColumnKind,
    summary: QueryResult | undefined,
    histogram: QueryResult | undefined,
): ColumnStats {
    const row: Record<string, unknown> = {};
    summary?.columns.forEach((c, i) => (row[c.name] = summary.rows[0]?.[i]));
    const count = Number(row.count);
    let nullPercent: number | undefined;
    let distinct: number | undefined;
    if (engine === "duckdb") {
        nullPercent = row.null_percentage === undefined ? undefined : Number(row.null_percentage);
        distinct = row.approx_unique === undefined ? undefined : Number(row.approx_unique);
    } else {
        nullPercent = count > 0 ? ((count - Number(row.non_null)) / count) * 100 : 0;
        distinct = row.distinct_values === undefined ? undefined : Number(row.distinct_values);
    }
    const total = Math.round(count - (nullPercent ? (count * nullPercent) / 100 : 0));
    // DuckDB's distinct count is an estimate and can exceed the row count: never show more distinct than non-null values.
    if (distinct !== undefined && !Number.isNaN(total)) distinct = Math.min(distinct, total);
    const top: ColumnStats["top"] =
        kind === "text" && histogram
            ? histogram.rows.map(([value, n]) => ({
                  value: String(value),
                  percent: total > 0 ? Math.round((Number(n) / total) * 100) : 0,
              }))
            : [];
    const other = 100 - top.reduce((sum, t) => sum + t.percent, 0);
    if (top.length && distinct !== undefined && distinct > top.length && other > 0) {
        top.push({ value: "other", percent: other, other: true });
    }
    return {
        count: Number.isNaN(count) ? undefined : count,
        nullPercent,
        nonNull: Number.isNaN(total) ? undefined : total,
        distinct,
        min: row.min,
        max: row.max,
        avg: row.avg,
        bars: kind === "text" || !histogram ? [] : bucketCounts(histogram.rows),
        top,
    };
}

export function pageLabel(offset: number, rowCount: number, total?: number): string {
    if (rowCount === 0) return "0";
    const range = `${offset + 1} to ${offset + rowCount}`;
    return total === undefined ? range : `${range} of ${total.toLocaleString("en-US")}`;
}

export function formatCell(value: unknown): { text: string; kind: "null" | "number" | "text" } {
    if (value === null || value === undefined) return { text: "NULL", kind: "null" };
    if (typeof value === "number") return { text: value.toLocaleString("en-US", { maximumFractionDigits: 6 }), kind: "number" };
    return { text: String(value), kind: "text" };
}
