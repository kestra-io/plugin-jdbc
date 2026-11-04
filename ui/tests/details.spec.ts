import { afterEach, describe, expect, it, vi } from "vitest";
import { apiRoot, databaseFromUrl, fileHref, safeHref, summaryRows } from "../src/lib/details";

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

describe("fileHref", () => {
    it("routes kestra:// files through the execution file endpoint", () => {
        expect(fileHref("kestra:///a/b.ion", "main", "E1")).toBe("/api/v1/main/executions/E1/file?path=kestra%3A%2F%2F%2Fa%2Fb.ion");
    });

    it("keeps http(s) and drops anything else", () => {
        expect(fileHref("https://example.com/a", "main", "E1")).toBe("https://example.com/a");
        expect(fileHref("javascript:alert(1)", "main", "E1")).toBeUndefined();
        expect(fileHref("kestra:///a/b.ion", "main", undefined)).toBeUndefined();
    });
});

describe("apiRoot", () => {
    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it("prefixes the file route with the base path, without a trailing slash", () => {
        vi.stubGlobal("window", { KESTRA_BASE_PATH: "/kestra/" });
        expect(apiRoot()).toBe("/kestra");
        expect(fileHref("kestra:///a.ion", "main", "E1")).toBe("/kestra/api/v1/main/executions/E1/file?path=kestra%3A%2F%2F%2Fa.ion");
    });

    it("ignores VITE_APP_API_URL, which comes from the plugin build and not the host", () => {
        vi.stubEnv("VITE_APP_API_URL", "https://evil.example");
        vi.stubGlobal("window", { KESTRA_BASE_PATH: "/kestra" });
        expect(apiRoot()).toBe("/kestra");
        vi.unstubAllEnvs();
    });

    it("is empty at the root", () => {
        vi.stubGlobal("window", { KESTRA_BASE_PATH: "/" });
        expect(apiRoot()).toBe("");
    });
});
