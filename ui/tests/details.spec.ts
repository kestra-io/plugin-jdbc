import { afterEach, describe, expect, it, vi } from "vitest";
import { apiRoot, fileHref, safeHref } from "../src/lib/details";

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
