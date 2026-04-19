#!/usr/bin/env python3
"""Generate query graphs by sampling connected subgraphs from a data graph."""

import argparse
import random
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "SSM-GraphGen"))

from graph_utils import canonicalize_undirected_edges, read_standard_graph, write_standard_graph

DEFAULT_QUERY_VERTICES = 10
MAX_QUERY_VERTICES = 30


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


def eligible_start_vertices(adjacency, component_sizes, vertices_num):
    """Return vertices that belong to a large enough connected component."""
    if vertices_num == 1:
        return list(adjacency.keys())

    return [
        vertex
        for vertex, neighbors in adjacency.items()
        if neighbors and component_sizes.get(vertex, 0) >= vertices_num
    ]


def metropolis_hastings_random_walk(adjacency, starts, vertices_num, max_walk_steps, rng):
    """Sample a connected vertex set with a Metropolis-Hastings random walk."""
    if not starts:
        return None

    start = rng.choice(starts)
    selected = [start]
    selected_set = {start}
    current = start

    for _step in range(max_walk_steps):
        if len(selected) >= vertices_num:
            return selected

        neighbors = adjacency.get(current, [])
        if not neighbors:
            return None

        candidate = rng.choice(neighbors)
        current_degree = len(neighbors)
        candidate_degree = len(adjacency.get(candidate, []))
        acceptance = 1.0
        if candidate_degree > 0:
            acceptance = min(1.0, float(current_degree) / float(candidate_degree))

        if rng.random() > acceptance:
            continue

        current = candidate
        if current not in selected_set:
            selected.append(current)
            selected_set.add(current)

    if len(selected) >= vertices_num:
        return selected
    return None


def induced_edge_keys(selected_vertices, edge_labels):
    """Return all data-graph edges induced by the selected vertex set."""
    selected_set = set(selected_vertices)
    return {
        key
        for key in edge_labels
        if key[0] in selected_set and key[1] in selected_set
    }


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
    max_attempts,
    max_walk_steps,
    rng,
):
    """Generate one query graph as an induced subgraph of the data graph."""
    for _attempt in range(max_attempts):
        selected_vertices = metropolis_hastings_random_walk(
            adjacency=adjacency,
            starts=starts,
            vertices_num=vertices_num,
            max_walk_steps=max_walk_steps,
            rng=rng,
        )
        if selected_vertices is None:
            continue

        query_edge_keys = induced_edge_keys(selected_vertices, edge_labels)
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
    parser.add_argument(
        "--vertices-num",
        type=int,
        default=DEFAULT_QUERY_VERTICES,
        help="Query vertex count. Defaults to {}; maximum is {}.".format(
            DEFAULT_QUERY_VERTICES, MAX_QUERY_VERTICES
        ),
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
    if args.vertices_num > MAX_QUERY_VERTICES:
        raise ValueError("--vertices-num must be at most {}".format(MAX_QUERY_VERTICES))
    if args.num_per_setting <= 0:
        raise ValueError("--num-per-setting must be positive")
    if args.max_attempts <= 0:
        raise ValueError("--max-attempts must be positive")


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
            max_attempts=args.max_attempts,
            max_walk_steps=max_walk_steps,
            rng=rng,
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
