import { describe, expect, it } from "vitest";
import { databaseFromUrl, safeHref, summaryRows } from "../src/lib/details";

const labels = {
    database: "Database",
    fetchType: "Fetch type",
    statements: "Statements",
    rowsAffected: "Rows affected",
    rowsFetched: "Rows fetched",
    duration: "Duration",
};

describe("databaseFromUrl", () => {
    it("drops credentials, host and query parameters", () => {
        expect(databaseFromUrl("jdbc:postgresql://user:secret@db.example.com:5432/sales?password=secret")).toBe("sales");
        expect(databaseFromUrl("jdbc:mysql://127.0.0.1:3306/kestra")).toBe("kestra");
        expect(databaseFromUrl("jdbc:sqlserver://localhost:1433;databaseName=orders;password=x")).toBe("orders");
        expect(databaseFromUrl("jdbc:sqlite:temp")).toBe("temp");
    });

    it("ignores unrendered templates", () => {
        expect(databaseFromUrl("{{ secret('PG_URL') }}")).toBeUndefined();
    });
});

describe("summaryRows", () => {
    it("has two rows before execution", () => {
        const rows = summaryRows({ task: { url: "jdbc:mysql://h/db", fetchType: "STORE" }, taskType: "x.Query", hasExecution: false, labels });
        expect(rows).toEqual([
            { label: "Database", value: "db" },
            { label: "Fetch type", value: "STORE" },
        ]);
    });

    it("uses Statements for Queries and adds rows and duration after execution", () => {
        const rows = summaryRows({
            task: { url: "jdbc:mysql://h/db" },
            taskType: "io.kestra.plugin.jdbc.mysql.Queries",
            outputs: { rowsAffected: 1200, durationMs: 1500 },
            hasExecution: true,
            labels,
        });
        expect(rows).toHaveLength(4);
        expect(rows[1].value).toBe("Statements");
        expect(rows[2]).toEqual({ label: "Rows affected", value: "1,200" });
        expect(rows[3]).toEqual({ label: "Duration", value: "1.50 s" });
    });

    it("shows rows fetched for result sets", () => {
        const rows = summaryRows({ task: {}, taskType: "x.Query", outputs: { size: 3, durationMs: 12 }, hasExecution: true, labels });
        expect(rows[2]).toEqual({ label: "Rows fetched", value: "3" });
    });
});

describe("safeHref", () => {
    it("only accepts http(s)", () => {
        expect(safeHref("https://example.com/a")).toBe("https://example.com/a");
        expect(safeHref("javascript:alert(1)")).toBeUndefined();
        expect(safeHref("kestra:///file.ion")).toBeUndefined();
    });
});
