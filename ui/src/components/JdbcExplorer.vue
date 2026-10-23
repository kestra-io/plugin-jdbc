<script setup lang="ts">
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import { computed, ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import {
    type Catalog,
    type ColumnStats,
    type QueryResult,
    MAX_LIMIT,
    columnKind,
    countSql,
    endpointPath,
    engineOf,
    formatCell,
    histogramSql,
    pageLabel,
    post,
    quoteIdentifier,
    samples,
    summarySql,
    toColumnStats,
} from "../lib/explorer";
import { formatBytes } from "../lib/details";

const { t } = useI18n({
    inheritLocale: true,
    useScope: "local",
    messages: {
        en: {
            title: "{engine} explorer",
            readOnly: "Read-only",
            database: "Database",
            sql: "SQL",
            run: "Run",
            ready: "Ready",
            running: "Running",
            rowsInMs: "{rows} rows · {ms} ms",
            refused: "Refused by the endpoint",
            previous: "Previous",
            next: "Next",
            rows: "Rows {range}",
            columnExplorer: "Column explorer ({rows} rows)",
            nulls: "{percent}% null",
            distinct: "distinct {n}",
            values: "{n} values",
            min: "min",
            max: "max",
            avg: "avg",
            view: "view",
            unsupported: "No explorer for task type {type}.",
            noDatabase: "This task run has no database file: set outputDbFile to true on the task.",
            hintDuckdb: "Click a table or view to preview it. Then try DESCRIBE or SUMMARIZE.",
            hintSqlite: "SQLite has no DESCRIBE or SUMMARIZE: try Table info (PRAGMA table_info).",
            guardDuckdb:
                "One statement checked with json_serialize_sql, file attached READ_ONLY, external access off, settings locked, 1,000 rows per page, 30 s timeout.",
            guardSqlite:
                "First keyword allowlist, mode=ro with query_only, ATTACH and load_extension refused, 1,000 rows per page, 30 s timeout.",
            preview: "Preview",
            count: "Count",
            describe: "DESCRIBE",
            summarize: "SUMMARIZE",
            tableInfo: "Table info",
            write: "Try a write",
            fileRead: "Try a file read",
            attach: "Try ATTACH",
        },
    },
});

const props = defineProps<KnownSlotProps["execution-artifact"]>();
const emit = defineEmits<{ close: [] }>();

// The host leaves taskType undefined in the execution-artifact slot, the task carries its type.
const taskType = computed(() => props.taskType ?? (props.task as { type?: string } | undefined)?.type);
const engine = computed(() => engineOf(taskType.value));
const engineLabel = computed(() => (engine.value === "duckdb" ? "DuckDB" : "SQLite"));
const tenant = computed(() => props.tenant ?? window.localStorage?.getItem("selectedTenant") ?? "main");
const taskRun = computed(() => {
    const list = props.execution?.taskRunList as any[] | undefined;
    return list?.filter((tr: any) => tr.taskId === props.task?.id).at(-1);
});

const outputs = ref<Record<string, any> | null>(null);
const hasDatabase = computed(() => typeof outputs.value?.databaseUri === "string");
const fileName = computed(() => String(outputs.value?.databaseUri ?? "").split("/").pop() ?? "");

const catalog = ref<Catalog>({ tables: [] });
const selected = ref<string | undefined>();
const sql = ref("");
const result = ref<QueryResult | undefined>();
const error = ref<string | undefined>();
const status = ref(t("ready"));
const offset = ref(0);
const total = ref<number | undefined>();
const stats = ref<Record<string, ColumnStats | undefined>>({});
const expanded = ref<string | undefined>();
let generation = 0;

// The host's fetchEndpoint carries the session auth a raw fetch lacks (the core answers 403 to it).
const call = async <T,>(operation: "catalog" | "query", body: Record<string, unknown>): Promise<T> => {
    if (props.fetchEndpoint) {
        const params = Object.fromEntries(Object.entries(body).map(([k, v]) => [k, String(v)]));
        const out = (await props.fetchEndpoint(`${engine.value}-${operation}`, params, { taskRunId: taskRun.value?.id })) as any;
        if (out?.error) throw new Error(String(out.error));
        return out as T;
    }
    return post<T>(
        endpointPath({
            tenant: tenant.value,
            taskType: taskType.value!,
            engine: engine.value!,
            operation,
            executionId: props.execution!.id as string,
            taskRunId: taskRun.value?.id,
        }),
        body,
    );
};

const query = (statement: string, limit = MAX_LIMIT, from = 0) => call<QueryResult>("query", { sql: statement, limit, offset: from });

async function load() {
    try {
        outputs.value = (await props.fetchOutputs?.({ taskRunId: taskRun.value?.id })) ?? null;
    } catch {
        return;
    }
    if (!hasDatabase.value || !engine.value) return;
    try {
        catalog.value = await call<Catalog>("catalog", {});
        const first = catalog.value.tables[0];
        if (first) select(first.name);
    } catch (e) {
        error.value = (e as Error).message;
    }
}

watch(() => props.execution?.id, (id) => id && load(), { immediate: true });

function select(table: string) {
    selected.value = table;
    sql.value = samples(engine.value!, table)[0].sql;
    run(0);
}

async function run(from = 0) {
    const current = ++generation;
    offset.value = from;
    error.value = undefined;
    status.value = t("running");
    try {
        const answer = await query(sql.value, MAX_LIMIT, from);
        if (current !== generation) return;
        result.value = answer;
        status.value = t("rowsInMs", { rows: answer.rows.length.toLocaleString("en-US"), ms: answer.durationMs });
        if (from === 0) loadStats(current);
    } catch (e) {
        if (current !== generation) return;
        error.value = (e as Error).message;
        status.value = t("refused");
    }
}

// Column cards come from real queries over the current statement, three at a time, never from the displayed page.
async function loadStats(current: number) {
    stats.value = {};
    total.value = undefined;
    expanded.value = undefined;
    const columns = result.value?.columns ?? [];
    try {
        total.value = Number((await query(countSql(sql.value), 1)).rows[0]?.[0]);
    } catch {
        total.value = undefined;
    }
    const queue = [...columns];
    const worker = async () => {
        for (let column = queue.shift(); column; column = queue.shift()) {
            const kind = columnKind(column.type);
            try {
                const summary = await query(summarySql(engine.value!, sql.value, column.name, kind), 1, 0);
                const histogramQuery = histogramSql(engine.value!, sql.value, column.name, kind);
                const histogram = histogramQuery ? await query(histogramQuery, 10, 0).catch(() => undefined) : undefined;
                if (current === generation) stats.value[column.name] = toColumnStats(engine.value!, kind, summary, histogram);
            } catch {
                /* a card without statistics is shown for statements that cannot be wrapped, such as PRAGMA */
            }
        }
    };
    await Promise.all([worker(), worker(), worker()]);
}

const canPrevious = computed(() => offset.value > 0);
const canNext = computed(() => !!result.value?.hasMore);
const range = computed(() => pageLabel(offset.value, result.value?.rows.length ?? 0, total.value));
const sampleList = computed(() => (engine.value ? samples(engine.value, selected.value) : []));

function barHeights(bars: number[]) {
    const max = Math.max(1, ...bars);
    return bars.map((n) => Math.max(2, Math.round((n / max) * 28)));
}

const fmt = (value: unknown) => (value === undefined || value === null ? "-" : String(value));
const typeClass = (type: string) => columnKind(type);
</script>

<template>
    <p v-if="!engine" class="xp__empty">{{ t("unsupported", { type: taskType ?? "unknown" }) }}</p>
    <div v-else class="xp" role="dialog" :aria-label="t('title', { engine: engineLabel })">
        <header class="xp__header">
            <h3 class="xp__title">{{ t("title", { engine: engineLabel }) }}</h3>
            <span class="xp__pill">{{ t("readOnly") }}</span>
            <span class="xp__dim xp__mono">
                {{ task?.id }} · {{ fileName }}
                <template v-if="typeof outputs?.databaseSize === 'number'"> · {{ formatBytes(outputs.databaseSize) }}</template>
            </span>
            <button class="xp__close" type="button" aria-label="Close" @click="emit('close')">×</button>
        </header>

        <p v-if="outputs && !hasDatabase" class="xp__empty">{{ t("noDatabase") }}</p>

        <div v-else class="xp__grid">
            <aside class="xp__col">
                <h4 class="xp__label">{{ t("database") }}</h4>
                <ul class="xp__tables">
                    <li v-for="table in catalog.tables" :key="table.schema + table.name">
                        <button
                            type="button"
                            class="xp__table"
                            :class="{ 'xp__table--selected': selected === table.name }"
                            @click="select(table.name)"
                        >
                            <span class="xp__mono">{{ table.name }}</span>
                            <span class="xp__dim">{{ table.type === "VIEW" ? t("view") : table.type.toLowerCase() }}</span>
                        </button>
                    </li>
                </ul>
                <p class="xp__dim xp__note">{{ engine === "duckdb" ? t("hintDuckdb") : t("hintSqlite") }}</p>
            </aside>

            <section class="xp__col">
                <h4 class="xp__label">{{ t("sql") }}</h4>
                <textarea
                    v-model="sql"
                    class="xp__editor xp__mono"
                    rows="5"
                    spellcheck="false"
                    @keydown.enter.ctrl.prevent="run(0)"
                    @keydown.enter.meta.prevent="run(0)"
                />
                <div class="xp__bar">
                    <button type="button" class="xp__run" @click="run(0)">{{ t("run") }}</button>
                    <span class="xp__dim">{{ status }}</span>
                </div>
                <div class="xp__chips">
                    <button
                        v-for="sample in sampleList"
                        :key="sample.key"
                        type="button"
                        class="xp__chip"
                        @click="sql = sample.sql; run(0)"
                    >
                        {{ t(sample.key) }}
                    </button>
                </div>

                <p v-if="error" class="xp__error" role="alert">{{ error }}</p>

                <div v-if="result" class="xp__result">
                    <table class="xp__gridtable">
                        <thead>
                            <tr>
                                <th v-for="column in result.columns" :key="column.name">{{ column.name }}</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr v-for="(row, index) in result.rows" :key="index">
                                <td
                                    v-for="(cell, c) in row"
                                    :key="c"
                                    :class="{ 'xp__dim': formatCell(cell).kind === 'null', 'xp__num': formatCell(cell).kind === 'number' }"
                                >
                                    {{ formatCell(cell).text }}
                                </td>
                            </tr>
                        </tbody>
                    </table>
                    <div class="xp__pager">
                        <button type="button" :disabled="!canPrevious" @click="run(Math.max(0, offset - MAX_LIMIT))">{{ t("previous") }}</button>
                        <span class="xp__dim">{{ t("rows", { range }) }}</span>
                        <button type="button" :disabled="!canNext" @click="run(offset + MAX_LIMIT)">{{ t("next") }}</button>
                    </div>
                </div>
                <p class="xp__dim xp__note">{{ engine === "duckdb" ? t("guardDuckdb") : t("guardSqlite") }}</p>
            </section>

            <aside class="xp__col">
                <h4 class="xp__label">{{ t("columnExplorer", { rows: total === undefined ? "-" : total.toLocaleString("en-US") }) }}</h4>
                <button
                    v-for="column in result?.columns ?? []"
                    :key="column.name"
                    type="button"
                    class="xp__card"
                    :aria-expanded="expanded === column.name"
                    @click="expanded = expanded === column.name ? undefined : column.name"
                >
                    <span class="xp__cardhead">
                        <span class="xp__tag xp__mono" :class="`xp__tag--${typeClass(column.type)}`">{{ column.type.toLowerCase() }}</span>
                        <span class="xp__mono xp__grow">{{ column.name }}</span>
                        <span class="xp__dim">{{ t("nulls", { percent: Math.round(stats[column.name]?.nullPercent ?? 0) }) }}</span>
                    </span>
                    <span v-if="stats[column.name]?.bars.length" class="xp__bars" aria-hidden="true">
                        <i v-for="(h, i) in barHeights(stats[column.name]!.bars)" :key="i" :style="{ height: `${h}px` }" />
                    </span>
                    <span class="xp__cardfoot xp__dim">
                        <span>{{ t("distinct", { n: fmt(stats[column.name]?.distinct) }) }}</span>
                        <span>{{ t("values", { n: fmt(stats[column.name]?.count) }) }}</span>
                    </span>
                    <span v-if="expanded === column.name && stats[column.name]" class="xp__detail">
                        <template v-if="stats[column.name]!.top.length">
                            <span v-for="top in stats[column.name]!.top" :key="top.value" class="xp__top">
                                <span class="xp__mono">{{ top.value }}</span>
                                <span class="xp__topbar"><i :style="{ width: `${top.percent}%` }" /></span>
                                <span class="xp__dim">{{ top.percent }}%</span>
                            </span>
                        </template>
                        <template v-else>
                            <span>{{ t("min") }} {{ fmt(stats[column.name]!.min) }}</span>
                            <span>{{ t("max") }} {{ fmt(stats[column.name]!.max) }}</span>
                            <span v-if="stats[column.name]!.avg !== undefined">{{ t("avg") }} {{ fmt(stats[column.name]!.avg) }}</span>
                        </template>
                    </span>
                </button>
            </aside>
        </div>
    </div>
</template>

<style scoped>
/* Tokens follow the explorer mockup. Host variables are not guaranteed in this slot, so the palette is local. */
.xp {
    --xp-surface: #ffffff;
    --xp-canvas: #f1f2f7;
    --xp-border: #dcdee8;
    --xp-text: #1b1d2a;
    --xp-dim: #5d6178;
    --xp-accent: #8405ff;
    --xp-accent-soft: #efe3ff;
    --xp-ok: #17874f;
    --xp-ok-bg: #dff5e8;
    --xp-warn: #a8630a;
    --xp-warn-bg: #fdecd0;
    container-type: inline-size;
    display: flex;
    flex-direction: column;
    gap: 16px;
    padding: 20px;
    background: var(--xp-surface);
    color: var(--xp-text);
    font-size: 13px;
    line-height: 1.5;
}

:global(html.dark) .xp,
:global(.dark) .xp {
    --xp-surface: #151722;
    --xp-canvas: #10121a;
    --xp-border: #2a2d3c;
    --xp-text: #f2f3f8;
    --xp-dim: #a3a7bd;
    --xp-accent: #a45cff;
    --xp-accent-soft: #2b1d45;
    --xp-ok: #46d28b;
    --xp-ok-bg: #12301f;
    --xp-warn: #f5b25c;
    --xp-warn-bg: #3a2810;
}

.xp button {
    font: inherit;
}

.xp button:focus-visible,
.xp textarea:focus-visible {
    outline: 2px solid var(--xp-accent);
    outline-offset: 1px;
}

.xp__header {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 10px;
}

.xp__title {
    margin: 0;
    font-size: 20px;
    font-weight: 700;
}

.xp__pill {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 3px 8px;
    border-radius: 6px;
    background: var(--xp-ok-bg);
    color: var(--xp-ok);
    font-size: 12px;
    font-weight: 600;
}

.xp__pill::before {
    content: "";
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: currentColor;
}

.xp__close {
    margin-left: auto;
    border: 0;
    background: none;
    font-size: 20px;
    line-height: 1;
    cursor: pointer;
    color: var(--xp-dim);
}

.xp__grid {
    display: grid;
    grid-template-columns: minmax(0, 200px) minmax(0, 1fr) minmax(0, 260px);
    gap: 20px;
}

@container (max-width: 820px) {
    .xp__grid {
        grid-template-columns: minmax(0, 1fr);
    }
}

.xp__col {
    display: flex;
    flex-direction: column;
    gap: 8px;
    min-width: 0;
}

.xp__label {
    margin: 0;
    font-size: 12px;
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--xp-dim);
}

.xp__mono {
    font-family: ui-monospace, "JetBrains Mono", "SFMono-Regular", Menlo, monospace;
    font-size: 12.5px;
}

.xp__dim {
    color: var(--xp-dim);
    font-size: 12px;
}

.xp__note,
.xp__empty {
    margin: 0;
}

.xp__tables {
    margin: 0;
    padding: 0;
    list-style: none;
}

.xp__table {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 8px;
    width: 100%;
    padding: 6px 8px;
    border: 0;
    border-radius: 6px;
    background: none;
    cursor: pointer;
    color: inherit;
    text-align: left;
}

.xp__table:hover {
    background: var(--xp-canvas);
}

.xp__table--selected,
.xp__table--selected:hover {
    background: var(--xp-accent-soft);
    color: var(--xp-accent);
    font-weight: 600;
}

.xp__editor {
    width: 100%;
    box-sizing: border-box;
    resize: vertical;
    padding: 10px 12px;
    line-height: 1.5;
    border: 1px solid var(--xp-border);
    border-radius: 6px;
    background: var(--xp-canvas);
    color: var(--xp-text);
}

.xp__bar,
.xp__pager {
    display: flex;
    align-items: center;
    gap: 10px;
}

.xp__pager {
    justify-content: space-between;
    margin-top: 8px;
}

.xp__pager button {
    padding: 3px 10px;
    border: 1px solid var(--xp-border);
    border-radius: 6px;
    background: var(--xp-canvas);
    color: var(--xp-text);
    font-size: 12px;
    cursor: pointer;
}

.xp__pager button:disabled {
    opacity: 0.4;
    cursor: default;
}

.xp__run {
    padding: 5px 16px;
    border: 0;
    border-radius: 6px;
    background: var(--xp-accent);
    color: #fff;
    font-weight: 600;
    font-size: 12.5px;
    cursor: pointer;
}

.xp__chips {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
}

.xp__chip {
    padding: 3px 9px;
    border: 0;
    border-radius: 12px;
    background: var(--xp-accent-soft);
    color: var(--xp-accent);
    font-size: 11.5px;
    font-weight: 600;
    cursor: pointer;
}

.xp__error {
    margin: 0;
    padding: 8px 12px;
    border-radius: 6px;
    background: var(--xp-warn-bg);
    color: var(--xp-warn);
    font-size: 12.5px;
    white-space: pre-wrap;
}

.xp__result {
    overflow-x: auto;
}

.xp__gridtable {
    border-collapse: collapse;
    width: 100%;
    font-size: 13px;
    font-variant-numeric: tabular-nums;
}

.xp__gridtable th {
    padding: 6px 12px;
    border-bottom: 1px solid var(--xp-border);
    text-align: left;
    font-weight: 500;
    color: var(--xp-dim);
    white-space: nowrap;
}

.xp__gridtable td {
    padding: 8px 12px;
    border-bottom: 1px solid var(--xp-border);
    text-align: left;
    white-space: nowrap;
}

.xp__gridtable tr:last-child td {
    border-bottom: 0;
}

.xp__gridtable td.xp__num {
    text-align: left;
}

.xp__card {
    display: flex;
    flex-direction: column;
    gap: 6px;
    width: 100%;
    box-sizing: border-box;
    padding: 8px 10px;
    border: 1px solid var(--xp-border);
    border-radius: 6px;
    background: var(--xp-canvas);
    cursor: pointer;
    color: inherit;
    text-align: left;
}

.xp__card[aria-expanded="true"] {
    border-color: var(--xp-accent);
}

.xp__cardhead,
.xp__cardfoot,
.xp__top {
    display: flex;
    align-items: baseline;
    gap: 8px;
}

.xp__cardfoot {
    justify-content: space-between;
    flex-wrap: wrap;
    font-variant-numeric: tabular-nums;
}

.xp__cardhead .xp__dim {
    margin-left: auto;
}

.xp__grow {
    flex: 0 1 auto;
    min-width: 0;
    overflow-wrap: anywhere;
    font-weight: 600;
}

.xp__tag {
    padding: 1px 5px;
    border-radius: 3px;
    background: var(--xp-accent-soft);
    color: var(--xp-accent);
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
}

.xp__bars {
    display: flex;
    align-items: flex-end;
    gap: 2px;
    height: 34px;
}

.xp__bars i {
    flex: 1;
    border-radius: 2px 2px 0 0;
    background: var(--xp-accent);
    opacity: 0.75;
}

.xp__detail {
    display: flex;
    flex-direction: column;
    gap: 3px;
    font-size: 11.5px;
    color: var(--xp-dim);
}

.xp__top {
    display: grid;
    grid-template-columns: minmax(0, 1fr) 70px 36px;
    align-items: center;
    gap: 6px;
}

.xp__topbar {
    height: 6px;
    border-radius: 3px;
    background: var(--xp-border);
    overflow: hidden;
}

.xp__topbar i {
    display: block;
    height: 100%;
    background: var(--xp-accent);
}
</style>
