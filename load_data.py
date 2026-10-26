import os
import random
import sqlite3
import datetime

from config import *
from partitioner import Catalog, partition_of

NATIONS = ["Ghana", "Nigeria", "Kenya", "Egypt", "Morocco", "Senegal", "Ethiopia", "Tanzania"]
SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"]
STATUSES = ["O", "F", "P"]

SCHEMA = [
    """CREATE TABLE customers (
        c_custkey INTEGER PRIMARY KEY, c_name TEXT, c_nation TEXT, c_segment TEXT)""",
    """CREATE TABLE orders (
        o_orderkey INTEGER PRIMARY KEY, o_custkey INTEGER, o_orderdate TEXT,
        o_totalprice REAL, o_status TEXT, part INTEGER)""",
    """CREATE TABLE lineitem (
        l_orderkey INTEGER, l_linenumber INTEGER, l_partkey INTEGER, l_quantity INTEGER,
        l_price REAL, l_discount REAL, l_shipdate TEXT, part INTEGER,
        PRIMARY KEY (l_orderkey, l_linenumber))""",
]
INDEXES = [
    "CREATE INDEX idx_orders_part ON orders(part)",
    "CREATE INDEX idx_orders_cust ON orders(o_custkey)",
    "CREATE INDEX idx_lineitem_part ON lineitem(part)",
]

BATCH = 50_000


def node_path(n):
    return os.path.join(DATA_DIR, f"node_{n}.db")


def rand_date(rng):
    base = datetime.date(2018, 1, 1).toordinal()
    return datetime.date.fromordinal(base + rng.randint(0, 365 * 7)).isoformat()


def pick_customer(rng):
    """Uniform when SKEW == 0; otherwise a few customers get most of the orders."""
    if SKEW <= 0:
        return rng.randint(1, NUM_CUSTOMERS)
    x = int(rng.paretovariate(1.0 + (1.0 / SKEW)))
    return min(x, NUM_CUSTOMERS)


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    rng = random.Random(RANDOM_SEED)
    catalog = Catalog()

    conns = {}
    for n in range(NUM_NODES):
        if os.path.exists(node_path(n)):
            os.remove(node_path(n))
        conn = sqlite3.connect(node_path(n))
        for ddl in SCHEMA:
            conn.execute(ddl)
        conns[n] = conn

    buffers = {n: {"orders": [], "lineitem": []} for n in range(NUM_NODES)}

    def flush(n, table, force=False):
        buf = buffers[n][table]
        if buf and (force or len(buf) >= BATCH):
            q = "?," * len(buf[0])
            conns[n].executemany(f"INSERT INTO {table} VALUES ({q[:-1]})", buf)
            buf.clear()

    # 1) Replicate the small dimension table to EVERY node
    customers = [(i, f"Customer#{i:06d}", rng.choice(NATIONS), rng.choice(SEGMENTS))
                 for i in range(1, NUM_CUSTOMERS + 1)]
    for n in range(NUM_NODES):
        conns[n].executemany("INSERT INTO customers VALUES (?,?,?,?)", customers)

    # 2) Hash-partition orders and lineitem on orderkey (co-located), with replication
    for okey in range(1, NUM_ORDERS + 1):
        p = partition_of(okey)
        ocust = pick_customer(rng)
        odate = rand_date(rng)
        lines = []
        total = 0.0
        for ln in range(1, rng.randint(1, MAX_LINES_PER_ORDER) + 1):
            qty = rng.randint(1, 50)
            price = round(rng.uniform(10, 1000), 2)
            disc = round(rng.uniform(0, 0.10), 2)
            total += qty * price * (1 - disc)
            lines.append((okey, ln, rng.randint(1, 20000), qty, price, disc, rand_date(rng), p))
        order_row = (okey, ocust, odate, round(total, 2), rng.choice(STATUSES), p)

        for n in catalog.nodes_for(p):          # primary AND replica node(s)
            buffers[n]["orders"].append(order_row)
            buffers[n]["lineitem"].extend(lines)
            flush(n, "orders")
            flush(n, "lineitem")

    for n in range(NUM_NODES):
        flush(n, "orders", force=True)
        flush(n, "lineitem", force=True)
        for ix in INDEXES:
            conns[n].execute(ix)
        conns[n].commit()

    # 3) Verify: primary-only counts must add up to the full dataset (replicas excluded)
    print(f"{'Node':<6}{'Primary parts':<16}{'Orders(primary)':<18}{'Lineitem(primary)':<19}{'Orders(stored)'}")
    tot_o = tot_l = 0
    for n in range(NUM_NODES):
        parts = catalog.primary_partitions(n)
        ph = ",".join(map(str, parts))
        o = conns[n].execute(f"SELECT COUNT(*) FROM orders WHERE part IN ({ph})").fetchone()[0]
        l = conns[n].execute(f"SELECT COUNT(*) FROM lineitem WHERE part IN ({ph})").fetchone()[0]
        stored = conns[n].execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        tot_o += o
        tot_l += l
        print(f"{n:<6}{len(parts):<16}{o:<18}{l:<19}{stored}")
    print(f"\nTotal orders (primary): {tot_o}  (expected {NUM_ORDERS})")
    print(f"Total lineitem (primary): {tot_l}")
    assert tot_o == NUM_ORDERS, "Partitioning lost or duplicated rows!"
    print("OK: data distributed correctly.")

    for c in conns.values():
        c.close()


if __name__ == "__main__":
    main()