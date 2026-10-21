// Print SQLite vs Turso MVCC side by side from a saved run (default: results/bench-full.json).
import fs from "node:fs";

const file = process.argv[2] ?? "results/bench-full.json";
const { meta, rows } = JSON.parse(fs.readFileSync(file, "utf8"));
const workload = process.env.WORKLOAD ?? "compute";
const get = (engine, writers) => rows.find((r) => r.workload === workload && r.engine === engine && r.writers === writers);

console.log(`workload=${workload}${workload === "compute" ? ` (${meta.computeMs} ms of work inside each transaction)` : ""}, synchronous=${meta.sync}, median of ${meta.repeat} runs`);
console.table(
  [...new Set(rows.map((r) => r.writers))].map((writers) => {
    const s = get("sqlite", writers);
    const t = get("turso-mvcc", writers);
    return {
      writers,
      "SQLite tx/s": s["tx/s (median)"],
      "Turso MVCC tx/s": t["tx/s (median)"],
      "SQLite p99 ms": s["p99 ms"],
      "MVCC p99 ms": t["p99 ms"],
      "data correct": s.dataCorrect && t.dataCorrect,
    };
  }),
);
