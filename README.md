# Does Turso's `BEGIN CONCURRENT` beat SQLite? Testing it on a laptop

SQLite allows one writer at a time. Turso, a Rust rewrite of SQLite, added an MVCC mode in 2026 (`PRAGMA journal_mode = 'mvcc'` + `BEGIN CONCURRENT`). In this mode, transactions write in parallel and only fail if they touch the same rows. Turso's [engineering post](https://turso.tech/blog/beyond-the-single-writer-limitation-with-tursos-concurrent-writes) reports "up to 4x the write throughput of SQLite". That's measured at 8 threads with 1 ms of compute inside each transaction. With no compute they report about 16%, and with a single thread they say SQLite is faster.

I wanted to see whether that holds outside their setup, on an ordinary laptop, from Node.

**Setup:** `@tursodatabase/database` 0.8.3, SQLite 3.53.0 (Node's built-in `node:sqlite`), Node 24.16, Windows 11, AMD Ryzen 7 5700U (8 cores / 16 threads), NVMe SSD. Each writer is a `worker_thread` with its own connection to the same database file. Each test runs for 3 seconds and repeats 3 times, and the tables show the median. Raw runs are in [`results/`](results).

## Result: 5x with work inside the transaction, nothing for a hot row

Full durability (`synchronous = FULL`, every commit flushed). Each transaction reads a row, spends 1 ms on CPU work, then inserts 10 rows:

| Writers | SQLite tx/s | Turso MVCC tx/s | SQLite p99 | Turso MVCC p99 |
|---|---|---|---|---|
| 1 | 347 | 311 | 4.3 ms | 4.5 ms |
| 2 | 332 | 540 | 5.5 ms | 4.9 ms |
| 4 | 339 | 1,011 | 7.8 ms | 5.5 ms |
| 8 | 328 | **1,753** | 258 ms | **6.1 ms** |

SQLite stays flat because each writer holds the write lock for the whole millisecond of work, so the others queue. MVCC transactions overlap and only serialize at commit. At 8 writers that's 5.3x SQLite's throughput, with zero retries. Turso's 4x claim held up here; if anything it was conservative.

All three workloads at full durability, 8 writers:

| Workload (8 writers, FULL) | SQLite | Turso (normal WAL) | Turso MVCC | MVCC retries per commit |
|---|---|---|---|---|
| `compute`: read, 1 ms work, insert 10 rows | 328 tx/s | 288 tx/s | **1,753 tx/s** | 0 |
| `disjoint`: insert 10 rows, each writer its own rows | 192 tx/s | 253 tx/s | **1,722 tx/s** | 0 |
| `hot`: every writer increments the same counter row | **497 tx/s** | 140 tx/s | 417 tx/s | 1.23 |

The `disjoint` result surprised me more than `compute`. There's no CPU work at all, but at full durability every commit waits on a disk flush. SQLite writers queue for the lock and flush one at a time. Turso's MVCC commit path batches commits from concurrent writers into one flush (group commit, see `core/mvcc/database/group_commit.rs`). This laptop's disk does about 700 flushes per second, which caps SQLite around 500 tx/s. MVCC got past that by sharing flushes.

`hot` is the case Turso's own docs warn about: if everyone writes the same row, MVCC just turns waiting into conflicts and retries.

## Without disk flushes, SQLite is still much faster

The same benchmark with `synchronous = OFF` on every connection isolates the engines' CPU cost and concurrency control from the disk:

| Workload, 8 writers, OFF | SQLite | Turso (normal WAL) | Turso MVCC |
|---|---|---|---|
| `compute` | 737 tx/s | 425 tx/s | **5,206 tx/s** |
| `disjoint` | 9,767 tx/s | 825 tx/s | 9,663 tx/s (range 4,175 to 17,731) |
| `hot` | **13,518 tx/s** | 473 tx/s | 1,074 tx/s |

With a single writer and short transactions, SQLite did 14,740 tx/s in `disjoint` against MVCC's 4,397, and 37,274 against 2,883 in `hot`. Turso's engine is still slower per statement than SQLite through these bindings, so its MVCC wins come from writers not waiting on each other, not from faster writes. If your transactions are short and you don't need per-commit durability, SQLite wins.

## When it's worth it, from these numbers

- **Yes:** many writers, transactions that hold work open (read, compute, write), writes to different rows, and full durability.
- **No:** a single writer, short transactions without per-commit fsync, or everyone updating the same row.

## Things that tripped me up (and would skew a benchmark)

- **`synchronous` is per connection.** My first version set it on the setup connection only. The writer threads then ran on their own defaults, and one early MVCC run committed faster than this disk can flush. Every writer now sets `synchronous` explicitly and reports what it reads back (the `sync` column in the JSON).
- **Disk flush speed on this laptop is noisy.** Single runs swung by 3-4x, which is why every number here is a median of 3 with the min-max range saved.
- **Use the same lock mode on both sides.** SQLite and Turso's normal mode both use `BEGIN IMMEDIATE`. With Turso's plain `BEGIN`, a read-then-write transaction fails with `Database snapshot is stale. You must rollback and retry the whole transaction.`, which is retryable but easy to misclassify as a real error.
- **Conflicts surface early.** In MVCC mode a same-row conflict fails on the `UPDATE` itself (`Write-write conflict`), not at `COMMIT`, and the transaction is already rolled back.
- **No `SAVEPOINT` yet** in the Turso engine ("SAVEPOINT not supported yet").

## Caveats

- One machine, Windows, Node bindings, short runs. Linux or Turso's Rust API could look different.
- All engines use the same retry loop: no built-in `busy_timeout`, 0-2 ms random backoff. SQLite's tail latency at 8 writers (p99 up to 880 ms) is partly a product of that loop. A different backoff strategy would change the p99 columns more than the throughput.
- MVCC is an early preview in Turso. I verified the row counts after every run (`dataCorrect` in the JSON), but I didn't test crash recovery in MVCC mode.

## Run it yourself

```bash
npm install
npm run bench-full                  # synchronous=FULL, ~7 min
npm run bench-off                   # synchronous=OFF
npm run summary                     # SQLite vs MVCC table from results/bench-full.json
node bench/run.mjs SYNC=FULL WORKLOADS=compute WRITERS=8 REPEAT=1
```
