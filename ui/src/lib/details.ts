export const DASH = "-";

export function formatDuration(ms?: number): string {
    if (typeof ms !== "number") return DASH;
    return ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(2)} s`;
}

export function formatBytes(bytes?: number): string {
    if (typeof bytes !== "number") return DASH;
    const units = ["B", "KB", "MB", "GB", "TB"];
    let i = 0;
    let v = bytes;
    while (v >= 1024 && i < units.length - 1) {
        v /= 1024;
        i++;
    }
    return `${v.toFixed(i === 0 ? 0 : 2)} ${units[i]}`;
}

export function formatNumber(value?: number): string {
    return typeof value === "number" ? value.toLocaleString("en-US") : DASH;
}

// Output values are untrusted: only explicit http(s) URLs are ever bound to an href.
export function safeHref(value: unknown): string | undefined {
    return typeof value === "string" && /^https?:\/\//i.test(value) ? value : undefined;
}

// The host sets KESTRA_BASE_PATH, so the file route also works behind a base path.
export function apiRoot(): string {
    const basePath = typeof window === "undefined" ? "" : ((window as { KESTRA_BASE_PATH?: string }).KESTRA_BASE_PATH ?? "");
    return basePath.trim().replace(/\/$/, "");
}

// Internal storage files go through the execution file route (same origin, session auth).
export function fileHref(value: unknown, tenant: string, executionId?: string): string | undefined {
    if (typeof value !== "string" || !value.startsWith("kestra://") || !executionId) return safeHref(value);
    return `${apiRoot()}/api/v1/${encodeURIComponent(tenant)}/executions/${encodeURIComponent(executionId)}/file?path=${encodeURIComponent(value)}`;
}
