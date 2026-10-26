from config import NUM_NODES, NUM_PARTITIONS, REPLICATION_FACTOR


def partition_of(key: int) -> int:
    """Stable hash partitioning: key -> virtual partition id."""
    h = (key * 2654435761) % (2 ** 32)   # multiplicative hash scrambles sequential keys
    return (h >> 16) % NUM_PARTITIONS


class Catalog:
    """Maps each virtual partition to an ordered list of nodes: [primary, replica, ...]."""

    def __init__(self, num_nodes=NUM_NODES, num_partitions=NUM_PARTITIONS,
                 replication_factor=REPLICATION_FACTOR):
        self.num_nodes = num_nodes
        self.num_partitions = num_partitions
        self.map = {}
        for p in range(num_partitions):
            primary = p % num_nodes
            # replicas go on the following nodes, so a copy never shares a node with its primary
            self.map[p] = [(primary + r) % num_nodes for r in range(replication_factor)]

    def nodes_for(self, partition: int):
        """All nodes storing this partition (primary first)."""
        return self.map[partition]

    def primary(self, partition: int) -> int:
        return self.map[partition][0]

    def primary_partitions(self, node: int):
        """Partitions this node is responsible for when answering queries."""
        return [p for p, nodes in self.map.items() if nodes[0] == node]

    def all_partitions(self, node: int):
        """Partitions physically stored on this node (primary + replica copies)."""
        return [p for p, nodes in self.map.items() if node in nodes]


if __name__ == "__main__":
    c = Catalog()
    for n in range(NUM_NODES):
        print(f"Node {n}: primary={c.primary_partitions(n)}  stored={c.all_partitions(n)}")