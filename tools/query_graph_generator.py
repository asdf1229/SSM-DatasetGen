#!/usr/bin/env python3
"""Generate query graphs by sampling connected subgraphs from a data graph."""

import argparse
import itertools
import random
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "SSM-GraphGen"))

from graph_utils import canonicalize_undirected_edges, read_standard_graph, write_standard_graph


def edge_key(u, v):
    """Return a canonical undirected edge key."""
    return tuple(sorted((int(u), int(v))))


def build_adjacency(graph):
    """Build adjacency lists and edge-label lookup for the data graph."""
    adjacency = {vertex_id: [] for vertex_id in graph.vertices}
    edge_labels = {}

    for u, v, label in canonicalize_undirected_edges(graph.edges):
        if u not in graph.vertices or v not in graph.vertices:
            continue
        key = edge_key(u, v)
        edge_labels[key] = label
        adjacency.setdefault(u, []).append(v)
        adjacency.setdefault(v, []).append(u)

    for neighbors in adjacency.values():
        neighbors.sort()

    return adjacency, edge_labels


def connected_component_sizes(adjacency):
    """Return component size for every vertex."""
    sizes = {}
    visited = set()

    for start in adjacency:
        if start in visited:
            continue

        stack = [start]
        component = []
        visited.add(start)
        while stack:
            vertex = stack.pop()
            component.append(vertex)
            for neighbor in adjacency.get(vertex, []):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                stack.append(neighbor)

        component_size = len(component)
        for vertex in component:
            sizes[vertex] = component_size

    return sizes


def target_edge_count(vertices_num, avg_degree, density):
    """Convert avg-degree/density parameters into a target edge count."""
    max_edges = vertices_num * (vertices_num - 1) // 2
    min_connected_edges = max(0, vertices_num - 1)
    targets = []

    if avg_degree is not None:
        targets.append(int(round(float(avg_degree) * vertices_num / 2.0)))
    if density is not None:
        targets.append(int(round(float(density) * max_edges)))
    if not targets:
        raise ValueError("either --avg-degree or --density must be provided")

    target = targets[0]
    if len(targets) > 1 and abs(targets[0] - targets[1]) > 1:
        raise ValueError(
            "inconsistent --avg-degree and --density targets: {} vs {}".format(
                targets[0], targets[1]
            )
        )

    if target < min_connected_edges:
        raise ValueError(
            "target edge count {} cannot contain a connected tree with {} vertices".format(
                target, vertices_num
            )
        )
    if target > max_edges:
        raise ValueError(
            "target edge count {} exceeds complete graph edge count {}".format(
                target, max_edges
            )
        )

    return target


def eligible_start_vertices(adjacency, component_sizes, vertices_num):
    """Return vertices that belong to a large enough connected component."""
    if vertices_num == 1:
        return list(adjacency.keys())

    return [
        vertex
        for vertex, neighbors in adjacency.items()
        if neighbors and component_sizes.get(vertex, 0) >= vertices_num
    ]


def random_walk_tree(adjacency, starts, vertices_num, max_walk_steps, rng):
    """Sample a tree-sized connected vertex set with a random walk."""
    if not starts:
        return None

    start = rng.choice(starts)
    selected = [start]
    selected_set = {start}
    tree_edges = []
    current = start

    for _step in range(max_walk_steps):
        if len(selected) >= vertices_num:
            return selected, tree_edges

        neighbors = adjacency.get(current, [])
        if not neighbors:
            return None

        next_vertex = rng.choice(neighbors)
        if next_vertex not in selected_set:
            selected.append(next_vertex)
            selected_set.add(next_vertex)
            tree_edges.append(edge_key(current, next_vertex))
        current = next_vertex

    if len(selected) >= vertices_num:
        return selected, tree_edges
    return None


def add_random_edges(
    selected_vertices,
    tree_edges,
    edge_labels,
    target_edges,
    missing_edge_threshold,
    rng,
    strict_data_edges=False,
):
    """Randomly add edges among selected vertices until the target is met."""
    query_edges = set(tree_edges)
    if len(query_edges) > target_edges:
        return None

    pair_pool = [
        edge_key(u, v)
        for u, v in itertools.combinations(selected_vertices, 2)
        if edge_key(u, v) not in query_edges
    ]
    rng.shuffle(pair_pool)

    if not strict_data_edges:
        while len(query_edges) < target_edges and pair_pool:
            query_edges.add(pair_pool.pop())
        if len(query_edges) != target_edges:
            return None
        return query_edges

    real_extra_edges = [key for key in pair_pool if key in edge_labels]
    if len(query_edges) + len(real_extra_edges) < target_edges:
        return None

    missing_edges_seen = 0
    while len(query_edges) < target_edges and pair_pool:
        key = pair_pool.pop()
        if key not in edge_labels:
            missing_edges_seen += 1
            if missing_edges_seen > missing_edge_threshold:
                return None
            continue
        query_edges.add(key)

    if len(query_edges) != target_edges:
        return None
    return query_edges


def remap_query_graph(data_graph, selected_vertices, query_edge_keys, edge_labels):
    """Remap sampled data-graph vertices to contiguous query vertex ids."""
    vertex_map = {
        data_vertex_id: query_vertex_id
        for query_vertex_id, data_vertex_id in enumerate(selected_vertices)
    }
    vertices = {
        query_vertex_id: data_graph.vertices[data_vertex_id]
        for data_vertex_id, query_vertex_id in vertex_map.items()
    }
    edges = [
        (vertex_map[u], vertex_map[v], edge_labels.get(edge_key(u, v)))
        for u, v in sorted(query_edge_keys)
    ]
    return vertices, edges


def generate_query_graph(
    data_graph,
    adjacency,
    edge_labels,
    starts,
    vertices_num,
    target_edges,
    missing_edge_threshold,
    max_attempts,
    max_walk_steps,
    rng,
    strict_data_edges=False,
):
    """Generate one query graph, retrying failed random walks."""
    for _attempt in range(max_attempts):
        walk_result = random_walk_tree(
            adjacency=adjacency,
            starts=starts,
            vertices_num=vertices_num,
            max_walk_steps=max_walk_steps,
            rng=rng,
        )
        if walk_result is None:
            continue

        selected_vertices, tree_edges = walk_result
        query_edge_keys = add_random_edges(
            selected_vertices=selected_vertices,
            tree_edges=tree_edges,
            edge_labels=edge_labels,
            target_edges=target_edges,
            missing_edge_threshold=missing_edge_threshold,
            rng=rng,
            strict_data_edges=strict_data_edges,
        )
        if query_edge_keys is None:
            continue

        return remap_query_graph(
            data_graph=data_graph,
            selected_vertices=selected_vertices,
            query_edge_keys=query_edge_keys,
            edge_labels=edge_labels,
        )

    raise RuntimeError(
        "failed to generate a query graph after {} attempt(s)".format(max_attempts)
    )


def output_path(output_dir, query_index):
    """Build the output path for one query index."""
    return Path(output_dir) / "{}.txt".format(query_index + 1)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-graph", required=True, help="Standard-format data graph.")
    parser.add_argument("--vertices-num", required=True, type=int, help="Query vertex count.")
    parser.add_argument("--avg-degree", type=float, default=None, help="Target average degree.")
    parser.add_argument("--density", type=float, default=None, help="Target undirected density.")
    parser.add_argument(
        "--missing-edge-threshold",
        required=True,
        type=int,
        help="Maximum missing random edge probes allowed while adding edges.",
    )
    parser.add_argument(
        "--num-per-setting",
        required=True,
        type=int,
        help="Number of query graphs to generate for this parameter setting.",
    )
    parser.add_argument("--output-dir", required=True, help="Output directory.")
    parser.add_argument(
        "--output-prefix",
        default="query",
        help="Legacy filename prefix argument; query files are now named 1.txt..N.txt.",
    )
    parser.add_argument("--seed", type=int, default=None, help="Optional random seed.")
    parser.add_argument(
        "--strict-data-edges",
        action="store_true",
        help="Require added non-tree edges to also exist in the data graph.",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=1000,
        help="Maximum sampling attempts per query graph.",
    )
    parser.add_argument(
        "--max-walk-steps",
        type=int,
        default=None,
        help="Maximum random-walk steps per attempt. Defaults to max(1000, vertices*100).",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files.")
    return parser.parse_args()


def validate_args(args):
    if args.vertices_num <= 0:
        raise ValueError("--vertices-num must be positive")
    if args.num_per_setting <= 0:
        raise ValueError("--num-per-setting must be positive")
    if args.missing_edge_threshold < 0:
        raise ValueError("--missing-edge-threshold must be non-negative")
    if args.max_attempts <= 0:
        raise ValueError("--max-attempts must be positive")
    if args.density is not None and not 0.0 <= args.density <= 1.0:
        raise ValueError("--density must be between 0 and 1")


def main():
    args = parse_args()
    validate_args(args)

    data_graph = read_standard_graph(args.data_graph)
    if args.vertices_num > len(data_graph.vertices):
        raise ValueError(
            "--vertices-num {} exceeds data graph vertex count {}".format(
                args.vertices_num, len(data_graph.vertices)
            )
        )

    adjacency, edge_labels = build_adjacency(data_graph)
    component_sizes = connected_component_sizes(adjacency)
    starts = eligible_start_vertices(adjacency, component_sizes, args.vertices_num)
    if not starts:
        raise ValueError(
            "data graph has no connected component with {} vertex/vertices".format(
                args.vertices_num
            )
        )

    target_edges = target_edge_count(args.vertices_num, args.avg_degree, args.density)
    max_walk_steps = args.max_walk_steps or max(1000, args.vertices_num * 100)
    rng = random.Random(args.seed)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    written = 0
    skipped = 0
    for query_index in range(args.num_per_setting):
        path = output_path(output_dir, query_index)
        if path.exists() and not args.overwrite:
            skipped += 1
            continue

        vertices, edges = generate_query_graph(
            data_graph=data_graph,
            adjacency=adjacency,
            edge_labels=edge_labels,
            starts=starts,
            vertices_num=args.vertices_num,
            target_edges=target_edges,
            missing_edge_threshold=args.missing_edge_threshold,
            max_attempts=args.max_attempts,
            max_walk_steps=max_walk_steps,
            rng=rng,
            strict_data_edges=args.strict_data_edges,
        )
        write_standard_graph(path, path.stem, vertices, edges)
        written += 1

    print(
        "wrote {} query graph(s), skipped {} existing file(s): {}".format(
            written, skipped, output_dir
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
