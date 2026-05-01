# Automatic Creation of Indices in PostgreSQL

## Overview
This project modifies the internal access layer of PostgreSQL to autonomously create indices based on workload analysis. It tracks sequential scans that would have benefited from an index, measures the potential performance benefit, accounts for the cost of index creation and maintenance, and dynamically delegates the actual index creation to an asynchronous background worker.

## Objective
To prevent application performance degradation caused by missing indices on frequently queried columns, without requiring manual database administration. By continuously analyzing the cost vs. benefit of potential indices, PostgreSQL becomes self-tuning.

## Architecture and Implementation Details

The implementation spans several core components of the PostgreSQL backend:

### 1. Shared Memory Infrastructure (`src/backend/storage/ipc/ipci.c`, `src/include/executor/nodeSeqscan.h`)
To track query statistics across multiple concurrent backend processes, custom data structures were introduced into PostgreSQL's shared memory:
- **`AutoIndexStat`**: Tracks relation IDs, column attribute numbers, cumulative query counts, accumulated benefit scores, and write (mutation) counts.
- **`AutoIndexDB`**: Keeps track of which databases currently have an active background indexing worker.
- **`AutoIndexShmemInit()` & `AutoIndexShmemSize()`**: Hooks added during server startup (`ipci.c`) to allocate our tracking structures in shared memory, protected by `AddinShmemInitLock`.

### 2. Query Instrumentation & Benefit Calculation (`src/backend/executor/nodeSeqscan.c`)
The sequential scan executor node was modified to act as the primary sensor for missing indices.
- Inside **`ExecEndSeqScan`**, the planner's qualifiers (`node->ss.ps.plan->qual`) are inspected. If the query filters on a column using an equality operator (`WHERE col = const`), it is considered an index candidate.
- **Selectivity Filtering:** The system rejects candidates where the estimated number of rows fetched (`plan_rows / reltuples`) exceeds 5%, as an index scan would likely be less efficient than a sequential scan anyway.
- **Cost Simulation:** The system mathematically simulates the planner's B-Tree cost model (incorporating `random_page_cost`, `cpu_operator_cost`, and index descent costs) to calculate the theoretical `index_scan_cost`.
- **Benefit Tracking:** The `benefit` is calculated as `seq_scan_cost - index_scan_cost`. This benefit, along with the `query_count`, is accumulated in the shared memory stats array.

### 3. Index Maintenance Cost Tracking (`src/backend/executor/nodeModifyTable.c`)
Indices speed up reads but slow down writes. To make intelligent decisions, the system also tracks write activity.
- The `track_autoindex_write(Oid relid)` hook increments the `write_count` for a relation during inserts, updates, and deletes.
- This allows the background worker to penalize relations that are highly volatile, preventing the creation of indices that would cost more to maintain than they save in read performance.

### 4. Autonomous Background Worker (`src/backend/postmaster/bgworker.c` & `nodeSeqscan.c`)
Index creation is an expensive operation that requires taking locks and performing I/O. Doing this synchronously during a user's `SELECT` query would cause massive latency spikes.
- A **Dynamic Background Worker** (`AutoIndexMain`) is registered and dynamically launched for the database.
- The worker periodically wakes up, scans the shared memory statistics, and evaluates the decision formula:
  
  ```text
  (Benefit * Number of Queries) > (Maintenance Cost)
  ```
- If the threshold is satisfied, the worker connects to the database via SPI (Server Programming Interface) and asynchronously executes a `CREATE INDEX` command on the identified column.

## 🚀 How to Run

```bash
export PATH=/home/saatvika/pg_install/bin:$PATH
export PGDATA=/home/saatvika/pgdata
export PGPORT=5435

make -j$(nproc)
make install

pg_ctl -D /home/saatvika/pgdata stop -m immediate
pg_ctl -l logfile start

psql postgres

## Summary
By combining query execution instrumentation, shared memory statistics, cost-based planner simulation, and asynchronous background workers, this project transforms PostgreSQL into an intelligent, self-tuning database system capable of automatically mitigating missing-index performance bottlenecks.
