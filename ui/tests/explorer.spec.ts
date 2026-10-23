import { describe, expect, it } from "vitest";
import {
    bucketCounts,
    columnKind,
    endpointPath,
    engineOf,
    formatCell,
    histogramSql,
    pageLabel,
    post,
    samples,
    summarySql,
    toColumnStats,
} from "../src/lib/explorer";

describe("engine and endpoint", () => {
    it("derives the engine from the task type", () => {
        expect(engineOf("io.kestra.plugin.jdbc.duckdb.Query")).toBe("duckdb");
        expect(engineOf("io.kestra.plugin.jdbc.sqlite.Queries")).toBe("sqlite");
        expect(engineOf("io.kestra.plugin.jdbc.postgresql.Query")).toBeUndefined();
    });

    it("builds the core endpoint path", () => {
        expect(
            endpointPath({ tenant: "main", taskType: "io.kestra.plugin.jdbc.duckdb.Query", engine: "duckdb", operation: "query", executionId: "e1", taskRunId: "t1" }),
        ).toBe("/api/v1/main/plugins/io.kestra.plugin.jdbc.duckdb.Query/endpoints/duckdb-query/e1/t1");
    });
});

describe("post", () => {
    it("shows the endpoint error as is", async () => {
        globalThis.fetch = (async () => new Response(JSON.stringify({ error: "Only a single read-only SELECT statement is allowed in the explorer." }), { status: 200 })) as any;
        await expect(post("/x", {})).rejects.toThrow("Only a single read-only SELECT statement is allowed in the explorer.");
    });

    it("returns the payload on success", async () => {
        globalThis.fetch = (async () => new Response(JSON.stringify({ rows: [] }), { status: 200 })) as any;
        await expect(post("/x", {})).resolves.toEqual({ rows: [] });
    });
});

describe("paging", () => {
    it("labels the range", () => {
        expect(pageLabel(0, 100, 1234)).toBe("1 to 100 of 1,234");
        expect(pageLabel(100, 50)).toBe("101 to 150");
        expect(pageLabel(0, 0)).toBe("0");
    });
});

describe("samples", () => {
    it("offers DESCRIBE and SUMMARIZE as subqueries for DuckDB", () => {
        const sql = samples("duckdb", "people").map((s) => s.sql);
        expect(sql).toContain('SELECT * FROM (DESCRIBE "people")');
        expect(sql).toContain('SELECT * FROM (SUMMARIZE "people")');
    });

    it("offers table info for SQLite and no write or file samples", () => {
        const list = samples("sqlite", "it's");
        expect(list.find((s) => s.key === "tableInfo")?.sql).toBe("PRAGMA table_info('it''s')");
        expect(list.map((s) => s.key)).toEqual(["tableInfo", "count"]);
    });
});

describe("stats queries", () => {
    it("uses SUMMARIZE on DuckDB and aggregates on SQLite", () => {
        expect(summarySql("duckdb", "SELECT * FROM t;", 'a"b', "numeric")).toBe('SELECT * FROM (SUMMARIZE SELECT "a""b" FROM (SELECT * FROM t) q)');
        const sqlite = summarySql("sqlite", "SELECT * FROM t", "a", "numeric");
        expect(sqlite).toContain('count(DISTINCT "a")');
        expect(sqlite).toContain('avg("a")');
        expect(summarySql("sqlite", "SELECT * FROM t", "a", "text")).not.toContain("avg(");
    });

    it("builds top values for text and buckets for numbers", () => {
        expect(histogramSql("sqlite", "SELECT * FROM t", "name", "text")).toContain("LIMIT 3");
        expect(histogramSql("duckdb", "SELECT * FROM t", "n", "numeric")).toContain("floor(");
        expect(histogramSql("sqlite", "SELECT * FROM t", "d", "date")).toContain("julianday(");
    });

    it("classifies column types", () => {
        expect(columnKind("BIGINT")).toBe("numeric");
        expect(columnKind("TIMESTAMP")).toBe("date");
        expect(columnKind("VARCHAR")).toBe("text");
    });

    it("clamps bucket counts", () => {
        expect(bucketCounts([[0, 2], [10, 1], [9, 3]])).toEqual([2, 0, 0, 0, 0, 0, 0, 0, 0, 4]);
    });

    it("maps a DuckDB summary and a SQLite aggregate row", () => {
        const duck = toColumnStats(
            "duckdb",
            "numeric",
            { columns: [{ name: "count", type: "" }, { name: "null_percentage", type: "" }, { name: "approx_unique", type: "" }, { name: "min", type: "" }], rows: [[10, "20.00", 4, "1"]], hasMore: false, durationMs: 1 },
            { columns: [], rows: [[0, 5]], hasMore: false, durationMs: 1 },
        );
        expect(duck.nullPercent).toBe(20);
        expect(duck.distinct).toBe(4);
        expect(duck.bars[0]).toBe(5);

        const lite = toColumnStats(
            "sqlite",
            "text",
            { columns: [{ name: "count", type: "" }, { name: "non_null", type: "" }, { name: "distinct_values", type: "" }], rows: [[10, 5, 2]], hasMore: false, durationMs: 1 },
            { columns: [], rows: [["a", 4]], hasMore: false, durationMs: 1 },
        );
        expect(lite.nullPercent).toBe(50);
        expect(lite.top).toEqual([{ value: "a", percent: 80 }]);
    });
});

describe("formatCell", () => {
    it("marks NULL and numbers", () => {
        expect(formatCell(null)).toEqual({ text: "NULL", kind: "null" });
        expect(formatCell(1234.5)).toEqual({ text: "1,234.5", kind: "number" });
        expect(formatCell("x")).toEqual({ text: "x", kind: "text" });
    });
});
