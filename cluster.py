import os
import json
import time
import sqlite3
import statistics
import threading
import multiprocessing as mp

from config import NUM_NODES, NUM_PARTITIONS, REPLICATION_FACTOR, DATA_DIR, CATALOG_FILE, MODE_FILE
from partitioner import Catalog
import load_data


# ---------------------------------------------------------------- merge helpers
def merge_groups(results, nsum):
    """Merge partial GROUP BY results: row = (key, sum1, sum2, ...)."""
    acc = {}
    for rows in results:
        for r in rows:
            a = acc.setdefault(r[0], [0] * nsum)
            for i in range(nsum):
                a[i] += r[i + 1] or 0
    return acc


def merge_scan(results):
    cnt, tot, mx = 0, 0.0, None
    for rows in results:
        if not rows:
            continue
        c, t, m = rows[0]
        cnt += c
        tot += t or 0
        if m is not None and (mx is None or m > mx):
            mx = m
    return [[cnt, round(tot, 2), mx]]


def merge_status(results):
    acc = merge_groups(results, 2)
    return [[k, v[0], round(v[1], 2), round(v[1] / v[0], 2)] for k, v in sorted(acc.items())]


def merge_year(results):
    acc = merge_groups(results, 2)
    return [[k, v[0], round(v[1], 2)] for k, v in sorted(acc.items())]


def merge_nation(results):
    acc = merge_groups(results, 2)
    rows = [[k, v[0], round(v[1], 2)] for k, v in acc.items()]
    return sorted(rows, key=lambda r: -r[2])


def merge_topcust(results):
    acc = merge_groups(results, 2)
    rows = [[k, v[0], round(v[1], 2)] for k, v in acc.items()]
    return sorted(rows, key=lambda r: -r[2])[:10]


# ---------------------------------------------------------------- query catalogue
QUERIES = {
    "scan": {
        "title": "Parallel scan: filter lineitem",
        "description": "Count and total the lineitems with quantity >= qty and discount >= disc.",
        "defaults": {"qty": 45, "disc": 0.08},
        "columns": ["matching_rows", "total_value", "max_price"],
        "plan": [
            "Coordinator: assign each partition to one live node",
            "Every node: scan its lineitem partitions, filter, compute COUNT / SUM / MAX",
            "Coordinator: add the counts and sums, take the max of the maxes",
        ],
        "sql": lambda ph, p: (
            f"SELECT COUNT(*), COALESCE(SUM(l_price*l_quantity),0), MAX(l_price) "
            f"FROM lineitem WHERE part IN ({ph}) AND l_quantity>=? AND l_discount>=?",
            (p["qty"], p["disc"]),
        ),
        "merge": merge_scan,
    },
    "agg_status": {
        "title": "Parallel aggregate: orders by status",
        "description": "Order count, total and average price per order status.",
        "defaults": {},
        "columns": ["status", "orders", "total_price", "avg_price"],
        "plan": [
            "Coordinator: assign partitions to nodes",
            "Every node: GROUP BY o_status on its orders partitions (partial COUNT and SUM)",
            "Coordinator: add partials per status, recompute AVG = SUM / COUNT",
        ],
        "sql": lambda ph, p: (
            f"SELECT o_status, COUNT(*), SUM(o_totalprice) FROM orders "
            f"WHERE part IN ({ph}) GROUP BY o_status", ()),
        "merge": merge_status,
    },
    "join_year": {
        "title": "Parallel join: orders JOIN lineitem (co-located)",
        "description": "Revenue and line count per order year. Both tables are hashed on the order key, so the join is local on every node with no data movement.",
        "defaults": {},
        "columns": ["year", "lineitems", "revenue"],
        "plan": [
            "Coordinator: assign partitions to nodes",
            "Every node: local hash join orders x lineitem (same partitions, same node), group by year",
            "Coordinator: add partial revenues per year",
        ],
        "sql": lambda ph, p: (
            f"SELECT substr(o.o_orderdate,1,4), COUNT(*), SUM(l.l_price*l.l_quantity*(1-l.l_discount)) "
            f"FROM orders o JOIN lineitem l ON l.l_orderkey=o.o_orderkey "
            f"WHERE o.part IN ({ph}) GROUP BY 1", ()),
        "merge": merge_year,
    },
    "join_nation": {
        "title": "Parallel 3-way join: customers x orders x lineitem",
        "description": "Revenue per customer nation. customers is replicated on every node, so the whole join runs locally.",
        "defaults": {},
        "columns": ["nation", "orders", "revenue"],
        "plan": [
            "Coordinator: assign partitions to nodes",
            "Every node: join its orders and lineitem partitions with its local copy of customers",
            "Coordinator: add partial orders and revenue per nation, sort by revenue",
        ],
        "sql": lambda ph, p: (
            f"SELECT c.c_nation, COUNT(DISTINCT o.o_orderkey), SUM(l.l_price*l.l_quantity*(1-l.l_discount)) "
            f"FROM orders o JOIN lineitem l ON l.l_orderkey=o.o_orderkey "
            f"JOIN customers c ON c.c_custkey=o.o_custkey "
            f"WHERE o.part IN ({ph}) GROUP BY c.c_nation", ()),
        "merge": merge_nation,
    },
    "topcust": {
        "title": "Top 10 customers by spend",
        "description": "A customer's orders are spread over all nodes, so the coordinator must merge per-customer partials before ranking.",
        "defaults": {},
        "columns": ["custkey", "orders", "total_spent"],
        "plan": [
            "Coordinator: assign partitions to nodes",
            "Every node: GROUP BY o_custkey on its partitions (partial per-customer totals)",
            "Coordinator: merge partials by customer, sort, keep the top 10",
        ],
        "sql": lambda ph, p: (
            f"SELECT o_custkey, COUNT(*), SUM(o_totalprice) FROM orders "
            f"WHERE part IN ({ph}) GROUP BY o_custkey", ()),
        "merge": merge_topcust,
    },
}


# ---------------------------------------------------------------- worker process
def worker_main(node_id, conn):
    """One shared-nothing node: its own process, its own database file."""
    db = sqlite3.connect(os.path.join(DATA_DIR, f"node_{node_id}.db"))
    while True:
        try:
            msg = conn.recv()
        except EOFError:
            break
        if msg is None:
            break
        qname, parts, params = msg
        try:
            t0 = time.perf_counter()
            ph = ",".join(str(int(p)) for p in parts)
            if qname == "__stats__":
                rows = {
                    "orders_primary": db.execute(f"SELECT COUNT(*) FROM orders WHERE part IN ({ph})").fetchone()[0] if parts else 0,
                    "lineitem_primary": db.execute(f"SELECT COUNT(*) FROM lineitem WHERE part IN ({ph})").fetchone()[0] if parts else 0,
                    "orders_stored": db.execute("SELECT COUNT(*) FROM orders").fetchone()[0],
                    "lineitem_stored": db.execute("SELECT COUNT(*) FROM lineitem").fetchone()[0],
                    "customers": db.execute("SELECT COUNT(*) FROM customers").fetchone()[0],
                }
            elif qname == "__partstats__":
                # row counts per stored partition: {partition: [orders, lineitems]}
                o = dict(db.execute("SELECT part, COUNT(*) FROM orders GROUP BY part").fetchall())
                l = dict(db.execute("SELECT part, COUNT(*) FROM lineitem GROUP BY part").fetchall())
                rows = {p: [o.get(p, 0), l.get(p, 0)] for p in set(o) | set(l)}
            elif qname == "__export__":
                p = int(params["part"])
                rows = {
                    "orders": db.execute("SELECT * FROM orders WHERE part=?", (p,)).fetchall(),
                    "lineitem": db.execute("SELECT * FROM lineitem WHERE part=?", (p,)).fetchall(),
                }
            elif qname == "__import__":
                db.executemany("INSERT OR REPLACE INTO orders VALUES (?,?,?,?,?,?)", params["orders"])
                db.executemany("INSERT OR REPLACE INTO lineitem VALUES (?,?,?,?,?,?,?,?)", params["lineitem"])
                db.commit()
                rows = "ok"
            elif qname == "__drop__":
                p = int(params["part"])
                db.execute("DELETE FROM orders WHERE part=?", (p,))
                db.execute("DELETE FROM lineitem WHERE part=?", (p,))
                db.commit()
                rows = "ok"
            elif not parts:
                rows = []
            else:
                sql, args = QUERIES[qname]["sql"](ph, params)
                rows = db.execute(sql, args).fetchall()
            ms = (time.perf_counter() - t0) * 1000
            conn.send({"node": node_id, "rows": rows, "ms": round(ms, 2)})
        except Exception as e:
            conn.send({"node": node_id, "error": str(e)})
    db.close()


# ---------------------------------------------------------------- coordinator
class Cluster:
    def __init__(self):
        self.catalog = Catalog()
        self.rebalances = 0
        self._load_catalog()
        self.alive = set(range(NUM_NODES))
        self.conns = {}
        self.procs = {}
        self.lock = threading.Lock()     # one operation at a time keeps pipe messages in order
        self.ctx = mp.get_context("spawn")
        self.data_mode = self._read_mode()

    # ---- persistence of placement and data mode
    def _load_catalog(self):
        if os.path.exists(CATALOG_FILE):
            with open(CATALOG_FILE) as f:
                d = json.load(f)
            self.catalog.map = {int(p): nodes for p, nodes in d["map"].items()}
            self.rebalances = d.get("rebalances", 0)

    def _save_catalog(self):
        with open(CATALOG_FILE, "w") as f:
            json.dump({"map": {str(p): n for p, n in self.catalog.map.items()},
                       "rebalances": self.rebalances}, f)

    def _read_mode(self):
        try:
            with open(MODE_FILE) as f:
                return f.read().strip() or "uniform"
        except OSError:
            return "uniform"

    def _state(self):
        return {"data_mode": self.data_mode, "rebalances": self.rebalances}

    # ---- process management
    def _spawn(self):
        self.conns = {}
        self.procs = {}
        for n in range(NUM_NODES):
            parent, child = self.ctx.Pipe()
            p = self.ctx.Process(target=worker_main, args=(n, child), daemon=True)
            p.start()
            child.close()
            self.conns[n] = parent
            self.procs[n] = p

    def start(self):
        self._spawn()
        self.info()                      # warm-up

    def stop(self):
        for c in self.conns.values():
            try:
                c.send(None)
            except Exception:
                pass

    def reload(self, skewed):
        """Regenerate all data (uniform or skewed) and restart the workers."""
        with self.lock:
            self.stop()
            for p in self.procs.values():
                p.join(timeout=10)
            load_data.generate(skewed=skewed, log=lambda *a, **k: None)
            self.catalog = Catalog()
            self.alive = set(range(NUM_NODES))
            self.rebalances = 0
            self.data_mode = self._read_mode()
            self._spawn()
        return self.info()

    # ---- helpers (call only while holding the lock)
    def _call(self, node, qname, parts=(), params=None):
        self.conns[node].send((qname, list(parts), params or {}))
        r = self.conns[node].recv()
        if "error" in r:
            raise RuntimeError(f"Node {node}: {r['error']}")
        return r["rows"]

    def assign(self):
        """Query decomposition: give each partition to its primary, or a replica if the primary is down."""
        plan = {n: [] for n in self.alive}
        for p in range(NUM_PARTITIONS):
            for n in self.catalog.nodes_for(p):
                if n in self.alive:
                    plan[n].append(p)
                    break
            else:
                raise RuntimeError(f"Partition {p} has no live copy")
        return plan

    def _weights(self):
        """Size of every partition (orders, lineitems) as reported by the nodes that store it."""
        for n in sorted(self.alive):
            self.conns[n].send(("__partstats__", [], {}))
        per = {}
        for n in sorted(self.alive):
            r = self.conns[n].recv()
            if "error" in r:
                raise RuntimeError(r["error"])
            per[n] = r["rows"]
        weights = {}
        for p in range(NUM_PARTITIONS):
            weights[p] = {"orders": 0, "lineitem": 0, "total": 0}
            for n in self.catalog.nodes_for(p):
                if n in per and p in per[n]:
                    o, l = per[n][p]
                    weights[p] = {"orders": o, "lineitem": l, "total": o + l}
                    break
        return weights

    def _balance_stats(self):
        weights = self._weights()
        plan = self.assign()
        total = sum(w["total"] for w in weights.values())
        nodes, loads = [], []
        for n in range(NUM_NODES):
            parts = plan.get(n, [])
            load = sum(weights[p]["total"] for p in parts)
            nodes.append({
                "node": n,
                "alive": n in self.alive,
                "load": load,
                "share": round(100 * load / total, 1) if total else 0,
                "partitions": [{"partition": p, **weights[p]} for p in parts],
            })
            if n in self.alive:
                loads.append(load)
        mean = total / len(loads) if loads else 0
        return {
            "nodes": nodes,
            "total": total,
            "mean": round(mean, 1),
            "max_over_mean": round(max(loads) / mean, 3) if mean else 0,
            "cv": round(statistics.pstdev(loads) / mean, 3) if mean else 0,
            "state": self._state(),
        }

    def _run(self, qname, params=None):
        q = QUERIES[qname]
        p = {k: float(v) for k, v in {**q["defaults"], **(params or {})}.items()}
        t0 = time.perf_counter()
        plan = self.assign()
        for n, parts in plan.items():                       # dispatch to all nodes first ...
            self.conns[n].send((qname, parts, p))
        replies = {n: self.conns[n].recv() for n in plan}   # ... then collect
        t1 = time.perf_counter()
        for r in replies.values():
            if "error" in r:
                raise RuntimeError(f"Node {r['node']}: {r['error']}")
        merged = q["merge"]([replies[n]["rows"] for n in sorted(replies)])
        t2 = time.perf_counter()
        return {
            "query": qname,
            "title": q["title"],
            "columns": q["columns"],
            "rows": merged,
            "total_ms": round((t2 - t0) * 1000, 2),
            "merge_ms": round((t2 - t1) * 1000, 2),
            "per_node": [
                {"node": n, "partitions": plan[n], "ms": replies[n]["ms"], "rows_returned": len(replies[n]["rows"])}
                for n in sorted(replies)
            ],
        }

    # ---- public API
    def info(self):
        with self.lock:
            for n in range(NUM_NODES):
                self.conns[n].send(("__stats__", self.catalog.primary_partitions(n), {}))
            nodes = []
            for n in range(NUM_NODES):
                r = self.conns[n].recv()
                if "error" in r:
                    raise RuntimeError(r["error"])
                st = r["rows"]
                st.update({
                    "node": n,
                    "alive": n in self.alive,
                    "primary_partitions": self.catalog.primary_partitions(n),
                    "stored_partitions": self.catalog.all_partitions(n),
                })
                nodes.append(st)
        return {
            "num_nodes": NUM_NODES,
            "num_partitions": NUM_PARTITIONS,
            "placement": [{"partition": p, "nodes": self.catalog.nodes_for(p)} for p in range(NUM_PARTITIONS)],
            "nodes": nodes,
            "state": self._state(),
        }

    def run(self, qname, params=None):
        with self.lock:
            return self._run(qname, params)

    def balance_stats(self):
        with self.lock:
            return self._balance_stats()

    def measure(self, qname="join_year", runs=5):
        """Run a query repeatedly (after one warm-up) and report the median response time."""
        with self.lock:
            self._run(qname)
            results = [self._run(qname) for _ in range(runs)]
            totals = sorted(r["total_ms"] for r in results)
            per_node = []
            for n in sorted(self.alive):
                vals = [pn["ms"] for r in results for pn in r["per_node"] if pn["node"] == n]
                per_node.append({"node": n, "avg_ms": round(sum(vals) / len(vals), 2)})
            return {
                "query": qname,
                "runs": runs,
                "median_ms": totals[len(totals) // 2],
                "min_ms": totals[0],
                "max_ms": totals[-1],
                "per_node": per_node,
                "balance": self._balance_stats(),
            }

    def rebalance(self):
        """Migration-aware LPT: biggest partitions first, each to the least-loaded node."""
        with self.lock:
            if len(self.alive) != NUM_NODES:
                raise RuntimeError("Rebalancing needs all nodes alive")
            before = self._balance_stats()
            ref_before = self._run("agg_status")

            weights = self._weights()
            nodes = sorted(self.alive)
            mean = sum(w["total"] for w in weights.values()) / len(nodes)
            tolerance = 0.05 * mean
            load = {n: 0 for n in nodes}
            target = {}
            for p in sorted(weights, key=lambda x: -weights[x]["total"]):
                best = min(nodes, key=lambda n: (load[n], n))
                holders = self.catalog.nodes_for(p)
                best_holder = min(holders, key=lambda n: (load[n], n))
                # prefer a node that already has the data when it is nearly as good
                pick = best_holder if load[best_holder] <= load[best] + tolerance else best
                target[p] = pick
                load[pick] += weights[p]["total"]

            moves = []
            for p in sorted(target):
                t = target[p]
                old = list(self.catalog.map[p])
                if t == old[0]:
                    continue
                if t in old:
                    # the target already holds a copy: just swap primary and replica roles
                    self.catalog.map[p] = [t] + [x for x in old if x != t]
                    moves.append({"partition": p, "from": old[0], "to": t,
                                  "type": "role swap (no data copied)", "rows": 0})
                else:
                    data = self._call(old[0], "__export__", params={"part": p})
                    self._call(t, "__import__", params={"part": p, "orders": data["orders"],
                                                        "lineitem": data["lineitem"]})
                    new = ([t] + old)[:REPLICATION_FACTOR]
                    for x in old:
                        if x not in new:
                            self._call(x, "__drop__", params={"part": p})
                    self.catalog.map[p] = new
                    moves.append({"partition": p, "from": old[0], "to": t,
                                  "type": "copied to new node",
                                  "rows": len(data["orders"]) + len(data["lineitem"])})

            self.rebalances += 1
            self._save_catalog()
            after = self._balance_stats()
            ref_after = self._run("agg_status")

            same = (len(ref_before["rows"]) == len(ref_after["rows"]) and all(
                a[0] == b[0] and a[1] == b[1] and abs(a[2] - b[2]) < 0.05
                for a, b in zip(ref_before["rows"], ref_after["rows"])))
            return {
                "before": {"max_over_mean": before["max_over_mean"], "cv": before["cv"],
                           "shares": [n["share"] for n in before["nodes"]]},
                "after": {"max_over_mean": after["max_over_mean"], "cv": after["cv"],
                          "shares": [n["share"] for n in after["nodes"]]},
                "moves": moves,
                "rows_moved": sum(m["rows"] for m in moves),
                "verified": same,
                "balance": after,
            }