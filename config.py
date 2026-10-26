import os

NUM_NODES = 4
NUM_PARTITIONS = 16          # virtual partitions (more than nodes, for rebalancing)
REPLICATION_FACTOR = 2       # 1 primary + 1 replica
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CATALOG_FILE = os.path.join(DATA_DIR, "catalog.json")   # persisted partition placement
MODE_FILE = os.path.join(DATA_DIR, "mode.txt")          # "uniform" or "skewed"

# Dataset size (increase later for scalability experiments)
NUM_CUSTOMERS = 10_000
NUM_ORDERS = 100_000
MAX_LINES_PER_ORDER = 7      # each order gets 1..7 lineitems (~400k lineitems)

# Skewed data: this share of all orders lands on Node 0's four partitions
HOT_SHARE = 0.55

RANDOM_SEED = 42