# Parallel Database System

Distributed Database Systems · Project 8 · Parallel Database Systems (Group Project)

A shared-nothing parallel database for OLAP workloads. One coordinator process and four worker-node processes each hold a slice of a TPC-H-style dataset, run query fragments at the same time, and have their partial results merged. The project is measured against a traditional sequential database and is driven from a browser dashboard.

## Group members

| No. | Name | Index Number |
|---|---|---|
| 1 | Nutakor Gideon Edudzi | 51100523 |
| 2 | Gertrude-Hillary Aba Ocran | 51201923 |
| 3 | Godfred Andoh Blay | 51200523 |

## What it does

| Deliverable | What is implemented |
|---|---|
| System architecture | Shared-nothing, master-worker: 1 coordinator + 4 worker processes, each with its own SQLite file, talking over pipes |
| Data distribution | `orders` and `lineitem` hash-partitioned on the order key into 16 virtual partitions; `customers` replicated to every node; every partition has a replica on another node |
| Parallel query processing | Each query is split into one fragment per node, run on all nodes at once, and merged by the coordinator. The orders-lineitem join is co-located, so no data crosses the network |
| Load balancing | A rebalancer measures real partition sizes and moves whole partitions (greedy LPT), using role swaps where possible so no rows are copied. Results are checked before and after |
| Fault tolerance | A node can be killed (a real process kill) mid-query. The coordinator re-plans on the replicas, writes continue, and the node is resynced on restart |
| Performance evaluation | Benchmark tab compares the parallel system with a sequential baseline |

## Requirements

- Python 3
- Flask (`pip install flask`)
- SQLite (included with Python)

## Files

| File | Purpose |
|---|---|
| `cluster.py` | Coordinator (`Cluster` class), worker process (`worker_main`), query merge functions, rebalancer, failover and resync |
| `partitioner.py` | Hash function and the partition catalog (saved to `data/catalog.json`) |
| `load_data.py` | Generates the dataset and distributes it across the nodes |
| `server.py` | Flask API behind the dashboard |
| `index.html` | The single-page dashboard |

## How to run

1. Install the requirement:

   ```bash
   pip install flask
   ```

2. Generate and distribute the data. The run should end with `OK: data distributed correctly`:

   ```bash
   python3 load_data.py
   ```

3. Start the dashboard:

   ```bash
   python3 server.py
   ```

4. Open <http://127.0.0.1:5001> in a browser.

## Using the dashboard

| Tab | What to try |
|---|---|
| Cluster | See the four nodes, their primary and replica partitions, and the partition placement table |
| Query Lab | Pick one of the five queries and run it in parallel. See time per node and the merged result |
| Load Balance | Generate skewed data (about 55% of orders on Node 0), measure query speed, click **Rebalance now**, then measure again |
| Fault Tolerance | Click **Run failure drill** to crash a node mid-query and check recovery, or use Kill / Restart and the write button by hand |
| Benchmark | Compare the parallel system with the sequential baseline |

## The five queries

1. Parallel scan: filter on `lineitem`
2. Orders by status (partial GROUP BY)
3. Orders joined with lineitem, revenue per year (co-located join)
4. Customers, orders and lineitem, revenue per nation (three-way local join)
5. Top 10 customers by spend

## Known limitations

- The coordinator is a single point of failure.
- All nodes run as processes on one machine, so speedup is limited by CPU cores and does not include real network cost.
- A single partition larger than the average node load cannot be balanced by moving whole partitions.
- Resync copies whole partitions rather than replaying missed writes.
- Losing both copies of a partition makes that data unavailable.

## Report

The full project report is submitted separately.