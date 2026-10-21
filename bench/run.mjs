// Concurrent-writer benchmark: SQLite (WAL) vs Turso (WAL) vs Turso (MVCC + BEGIN CONCURRENT).
// Each writer is a worker thread with its own connection to the same database file.
//
// env or args: SYNC (FULL | NORMAL | OFF, default FULL), WORKLOADS (default disjoint,hot,compute), COMPUTE_MS (default 1), SECONDS (default 3), REPEAT (default 3),
//      WRITERS (default "1,2,4,8"), ENGINES, WORKLOADS, OUT
import { Worker } from "node:worker_threads";
import { DatabaseSync } from "node:sqlite";
import { connect } from "@tursodatabase/database";
import os from "node:os";
import fs from "node:fs";

// Settings can also be passed as arguments (node bench/run.mjs SYNC=OFF), which works on every shell.
for (const arg of process.argv.slice(2)) {
  const [key, value] = arg.split("=");
  if (value !== undefined) process.env[key] = value;
}

const SYNC = process.env.SYNC ?? "FULL";
const SECONDS = Number(process.env.SECONDS ?? 3);
const REPEAT = Number(process.env.REPEAT ?? 3);
const WRITERS = (process.env.WRITERS ?? "1,2,4,8").split(",").map(Number);
const ENGINES = (process.env.ENGINES ?? "sqlite,turso-wal,turso-mvcc").split(",");
const WORKLOADS = (process.env.WORKLOADS ?? "disjoint,hot,compute").split(",");
const OUT = process.env.OUT ?? `results/bench-${SYNC.toLowerCase()}.json`;
const ROWS_PER_TX = 10;
const COMPUTE_MS = Number(process.env.COMPUTE_MS ?? 1);
const dir = "results/db";

const SCHEMA = [
  "CREATE TABLE items (id INTEGER PRIMARY KEY, writer INTEGER NOT NULL, v TEXT NOT NULL)",
  "CREATE TABLE counter (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)",
  "INSERT INTO counter VALUES (1, 0)",
];

async function setup(engine, path) {
  if (engine === "sqlite") {
    const db = new DatabaseSync(path);
    db.exec("PRAGMA journal_mode = WAL");
    for (const s of SCHEMA) db.exec(s);
    db.close();
    return;
  }
  const db = await connect(path);
  if (engine === "turso-mvcc") await db.exec("PRAGMA journal_mode = 'mvcc'");
  for (const s of SCHEMA) await db.exec(s);
  await db.close();
}

async function verify(engine, path) {
  const q = ["SELECT count(*) AS n FROM items", "SELECT n FROM counter"];
  if (engine === "sqlite") {
    const db = new DatabaseSync(path, { readOnly: true });
    const [items, counter] = q.map((s) => db.prepare(s).get().n);
    db.close();
    return { items, counter };
  }
  const db = await connect(path);
  const items = (await db.prepare(q[0]).get()).n;
  const counter = (await db.prepare(q[1]).get()).n;
  await db.close();
  return { items, counter };
}

const median = (xs) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];
const percentile = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];

async function runOnce(engine, workload, writers) {
  const path = `${dir}/${engine}-${workload}-${writers}.db`;
  for (const f of fs.readdirSync(dir).filter((f) => f.startsWith(`${engine}-${workload}-${writers}.db`))) fs.rmSync(`${dir}/${f}`);
  await setup(engine, path);

  const deadline = Date.now() + 500 + SECONDS * 1000; // 500 ms for workers to start
  let firstError;
  const results = await Promise.all(
    Array.from({ length: writers }, (_, writerId) =>
      new Promise((resolve, reject) => {
        const w = new Worker(new URL("./writer.mjs", import.meta.url), {
          workerData: { engine, workload, path, writerId, deadline, rowsPerTx: ROWS_PER_TX, sync: SYNC, computeMs: COMPUTE_MS },
        });
        w.on("message", (m) => (m.firstError ? (firstError ??= m.firstError) : resolve(m)));
        w.on("error", reject);
      }),
    ),
  );

  const commits = results.reduce((s, r) => s + r.commits, 0);
  const check = await verify(engine, path);
  return {
    txPerSec: commits / SECONDS,
    retriesPerCommit: commits ? results.reduce((s, r) => s + r.retries, 0) / commits : null,
    errors: results.reduce((s, r) => s + r.errors, 0),
    p99: percentile(results.flatMap((r) => r.latencies).sort((a, b) => a - b), 99),
    dataCorrect:
      check.items === (workload === "hot" ? commits : commits * ROWS_PER_TX) &&
      check.counter === (workload === "hot" ? commits : 0),
    syncReadBack: [...new Set(results.map((r) => r.syncReadBack))].join(","),
    firstError,
  };
}

fs.mkdirSync(dir, { recursive: true });
const rows = [];
for (const workload of WORKLOADS)
  for (const engine of ENGINES)
    for (const writers of WRITERS) {
      const runs = [];
      for (let i = 0; i < REPEAT; i++) runs.push(await runOnce(engine, workload, writers));
      const row = {
        workload,
        engine,
        writers,
        "tx/s (median)": Math.round(median(runs.map((r) => r.txPerSec))),
        "tx/s (min-max)": `${Math.round(Math.min(...runs.map((r) => r.txPerSec)))}-${Math.round(Math.max(...runs.map((r) => r.txPerSec)))}`,
        "retries/commit": +median(runs.map((r) => r.retriesPerCommit)).toFixed(2),
        "p99 ms": +median(runs.map((r) => r.p99)).toFixed(1),
        errors: runs.reduce((s, r) => s + r.errors, 0),
        dataCorrect: runs.every((r) => r.dataCorrect),
        sync: [...new Set(runs.map((r) => r.syncReadBack))].join(","),
        runs,
      };
      rows.push(row);
      console.log(JSON.stringify({ ...row, runs: undefined }));
    }

const meta = {
  sync: SYNC, computeMs: COMPUTE_MS, seconds: SECONDS, repeat: REPEAT, rowsPerTx: ROWS_PER_TX, node: process.version,
  turso: JSON.parse(fs.readFileSync("node_modules/@tursodatabase/database/package.json", "utf8")).version,
  sqlite: new DatabaseSync(":memory:").prepare("SELECT sqlite_version() AS v").get().v,
  platform: `${os.type()} ${os.release()} ${process.arch}`, cpus: os.cpus().length, cpu: os.cpus()[0].model,
};
fs.writeFileSync(OUT, JSON.stringify({ meta, rows }, null, 2));
console.log(meta);
console.table(rows.map(({ runs, ...r }) => r));
