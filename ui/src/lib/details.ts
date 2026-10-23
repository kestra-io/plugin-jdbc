export type Row = { label: string; value: string };

export const DASH = "-";

// Only the database name is kept, so credentials never reach the UI; unrendered templates are ignored.
export function databaseFromUrl(url?: unknown): string | undefined {
    if (typeof url !== "string" || url.includes("{{")) return undefined;
    const sqlServer = /;databaseName=([^;]+)/i.exec(url);
    if (sqlServer) return sqlServer[1];
    const withoutQuery = url.split(/[?;]/)[0];
    const afterHost = /^jdbc:[^:]+:(?:\/\/)?[^/]*\/([^/]+)$/.exec(withoutQuery);
    if (afterHost) return afterHost[1];
    const file = /^jdbc:(?:sqlite|duckdb):(.+)$/.exec(withoutQuery);
    return file ? file[1] : undefined;
}

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

export function isQueries(taskType?: string): boolean {
    return !!taskType && taskType.endsWith(".Queries");
}

export function summaryRows(opts: {
    task?: Record<string, any>;
    taskType?: string;
    outputs?: Record<string, any> | null;
    hasExecution: boolean;
    labels: Record<"database" | "fetchType" | "statements" | "rowsAffected" | "rowsFetched" | "duration", string>;
}): Row[] {
    const { task, taskType, outputs, hasExecution, labels } = opts;
    const fetchType = isQueries(taskType)
        ? labels.statements
        : typeof task?.fetchType === "string" && !task.fetchType.includes("{{")
          ? task.fetchType
          : "NONE";
    const rows: Row[] = [
        { label: labels.database, value: databaseFromUrl(task?.url) ?? DASH },
        { label: labels.fetchType, value: fetchType },
    ];
    if (hasExecution) {
        const affected = outputs?.rowsAffected as number | undefined;
        const fetched = outputs?.size as number | undefined;
        rows.push(
            affected !== undefined && affected !== null
                ? { label: labels.rowsAffected, value: formatNumber(affected) }
                : { label: labels.rowsFetched, value: formatNumber(fetched) },
        );
        rows.push({ label: labels.duration, value: formatDuration(outputs?.durationMs as number | undefined) });
    }
    return rows;
}

// Output values are untrusted: only explicit http(s) URLs are ever bound to an href.
export function safeHref(value: unknown): string | undefined {
    return typeof value === "string" && /^https?:\/\//i.test(value) ? value : undefined;
}

// Internal storage files go through the execution file route (same origin, session auth).
export function fileHref(value: unknown, tenant: string, executionId?: string): string | undefined {
    if (typeof value !== "string" || !value.startsWith("kestra://") || !executionId) return safeHref(value);
    return `/api/v1/${encodeURIComponent(tenant)}/executions/${encodeURIComponent(executionId)}/file?path=${encodeURIComponent(value)}`;
}
