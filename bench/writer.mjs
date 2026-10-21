// One writer thread. Opens its own connection and commits transactions in a loop
// until the deadline, retrying on busy/conflict errors.
import { workerData, parentPort } from "node:worker_threads";

const { engine, workload, path, writerId, deadline, rowsPerTx, sync, computeMs } = workerData;

// synchronous is per connection, so every writer sets it and reports what it reads back.
let db, exec, syncReadBack;
if (engine === "sqlite") {
  const { DatabaseSync } = await import("node:sqlite");
  db = new DatabaseSync(path);
  db.exec("PRAGMA busy_timeout = 0"); // retry ourselves, same as the Turso modes
  db.exec(`PRAGMA synchronous = ${sync}`);
  syncReadBack = db.prepare("PRAGMA synchronous").get().synchronous;
  exec = async (sql) => db.exec(sql);
} else {
  const { connect } = await import("@tursodatabase/database");
  db = await connect(path);
  await db.exec(`PRAGMA synchronous = ${sync}`);
  syncReadBack = Object.values(await db.prepare("PRAGMA synchronous").get())[0];
  exec = (sql) => db.exec(sql);
}
// SQLite and Turso WAL both take the write lock up front; MVCC uses optimistic concurrency.
const begin = engine === "turso-mvcc" ? "BEGIN CONCURRENT" : "BEGIN IMMEDIATE";

const isRetryable = (e) => /busy|conflict|locked|stale|retry/i.test(e.message);

let commits = 0;
let retries = 0;
let errors = 0;
let n = 0;
const latencies = [];

while (Date.now() < deadline) {
  const started = performance.now();
  for (;;) {
    try {
      await exec(begin);
      if (workload === "compute") {
        // Read, do ~1 ms of CPU work while the transaction is open, then write
        // (the shape of Turso's "4x faster" benchmark).
        await exec("SELECT n FROM counter WHERE id = 1");
        const until = performance.now() + computeMs;
        while (performance.now() < until);
      }
      if (workload === "disjoint" || workload === "compute") {
        // Every writer inserts its own rows: no logical conflict between writers.
        const values = [];
        for (let i = 0; i < rowsPerTx; i++) values.push(`(${writerId * 1e9 + n * rowsPerTx + i}, ${writerId}, 'payload-${n}-${i}')`);
        await exec(`INSERT INTO items (id, writer, v) VALUES ${values.join(",")}`);
      } else {
        // Every writer bumps the same counter row: maximum contention.
        await exec("UPDATE counter SET n = n + 1 WHERE id = 1");
        await exec(`INSERT INTO items (id, writer, v) VALUES (${writerId * 1e9 + n}, ${writerId}, 'hot-${n}')`);
      }
      await exec("COMMIT");
      commits++;
      n++;
      latencies.push(performance.now() - started);
      break;
    } catch (e) {
      try { await exec("ROLLBACK"); } catch {}
      if (!isRetryable(e)) {
        errors++;
        if (errors === 1) parentPort.postMessage({ firstError: e.message });
        break;
      }
      retries++;
      if (Date.now() >= deadline) break;
      await new Promise((r) => setTimeout(r, Math.random() * 2)); // small jittered backoff
    }
  }
}

if (engine === "sqlite") db.close(); else await db.close();
parentPort.postMessage({ commits, retries, errors, latencies, syncReadBack });
