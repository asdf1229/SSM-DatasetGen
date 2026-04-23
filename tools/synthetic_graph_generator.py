#!/usr/bin/env python3
"""Generate synthetic standard-format SSM data graphs."""

import argparse
import bisect
import math
import random
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "SSM-GraphGen"))

from graph_utils import write_standard_graph

FIXED_DEGREE_DISTRIBUTION = "R-MAT"


def edge_key(u, v):
    """Return a canonical undirected edge key."""
    return tuple(sorted((int(u), int(v))))


def target_edge_count(vertices, avg_degree):
    """Convert average degree into an exact edge target."""
    max_edges = vertices * (vertices - 1) // 2
    target = int(round(float(avg_degree) * vertices / 2.0))
    if target < 0:
        raise ValueError("--avg-degree must be non-negative")
    if target > max_edges:
        raise ValueError(
            "target edge count {} exceeds complete graph edge count {}".format(
                target, max_edges
            )
        )
    return target


def random_pair(vertices, rng):
    """Sample one undirected vertex pair uniformly."""
    u = rng.randrange(vertices)
    v = rng.randrange(vertices - 1)
    if v >= u:
        v += 1
    return edge_key(u, v)


def fill_uniform_edges(vertices, target_edges, rng, edges=None, max_attempt_factor=20):
    """Fill an edge set by uniform pair sampling."""
    edges = set(edges or [])
    if len(edges) >= target_edges:
        return edges

    max_edges = vertices * (vertices - 1) // 2
    if target_edges > max_edges:
        raise ValueError("requested more edges than possible")

    attempt_limit = max(1000, (target_edges - len(edges)) * max_attempt_factor)
    attempts = 0
    while len(edges) < target_edges and attempts < attempt_limit:
        attempts += 1
        edges.add(random_pair(vertices, rng))

    if len(edges) >= target_edges:
        return edges

    for u in range(vertices):
        for v in range(u + 1, vertices):
            edges.add((u, v))
            if len(edges) >= target_edges:
                return edges

    return edges


def sample_rmat_pair(scale, rng, probabilities):
    """Sample one edge endpoint pair with the R-MAT initiator matrix."""
    a, b, c, _d = probabilities
    u = 0
    v = 0
    for level in range(scale):
        bit = 1 << level
        draw = rng.random()
        if draw < a:
            continue
        if draw < a + b:
            v |= bit
        elif draw < a + b + c:
            u |= bit
        else:
            u |= bit
            v |= bit
    return u, v


def generate_rmat_edges(vertices, target_edges, rng, probabilities, max_attempt_factor):
    """Generate a simple graph with an R-MAT-style edge distribution."""
    if vertices <= 1 or target_edges == 0:
        return set()

    scale = max(1, int(math.ceil(math.log(vertices, 2))))
    edges = set()
    attempt_limit = max(10000, target_edges * max_attempt_factor)
    attempts = 0

    while len(edges) < target_edges and attempts < attempt_limit:
        attempts += 1
        u, v = sample_rmat_pair(scale, rng, probabilities)
        if u >= vertices or v >= vertices or u == v:
            continue
        edges.add(edge_key(u, v))

    if len(edges) < target_edges:
        edges = fill_uniform_edges(vertices, target_edges, rng, edges=edges)
    return edges


def normalize_distribution_name(value):
    """Normalize a distribution name for comparisons."""
    return str(value).strip().lower().replace("_", "-")


def generate_edges(args, rng):
    """Generate edges with the fixed R-MAT distribution."""
    target_edges = target_edge_count(args.vertices, args.avg_degree)
    probabilities = (
        args.rmat_a,
        args.rmat_b,
        args.rmat_c,
        1.0 - args.rmat_a - args.rmat_b - args.rmat_c,
    )
    return generate_rmat_edges(
        args.vertices,
        target_edges,
        rng,
        probabilities,
        args.max_attempt_factor,
    )


def build_degree_counts(vertices, edges):
    """Compute degree counts from edge keys."""
    degrees = [0] * vertices
    for u, v in edges:
        degrees[u] += 1
        degrees[v] += 1
    return degrees


def weighted_label_sampler(label_count, exponent):
    """Build a Zipf label sampler."""
    cumulative = []
    total = 0.0
    for rank in range(1, label_count + 1):
        total += 1.0 / (rank ** exponent)
        cumulative.append(total)

    def sample(rng):
        draw = rng.random() * total
        return bisect.bisect_left(cumulative, draw)

    return sample


def generate_uniform_labels(vertices, label_count, rng):
    """Assign labels uniformly."""
    return {vertex: str(rng.randrange(label_count)) for vertex in range(vertices)}


def generate_zipf_labels(vertices, label_count, exponent, rng):
    """Assign labels with a Zipf distribution."""
    sample_label = weighted_label_sampler(label_count, exponent)
    return {vertex: str(sample_label(rng)) for vertex in range(vertices)}


def generate_degree_correlated_labels(vertices, label_count, degrees, rng):
    """Assign low label ids to high-degree vertices."""
    ordered_vertices = list(range(vertices))
    tie_breakers = {vertex: rng.random() for vertex in ordered_vertices}
    ordered_vertices.sort(key=lambda vertex: (-degrees[vertex], tie_breakers[vertex]))

    labels = {}
    for rank, vertex in enumerate(ordered_vertices):
        label = min(label_count - 1, int(rank * label_count / max(1, vertices)))
        labels[vertex] = str(label)
    return labels


def generate_labels(args, edges, rng):
    """Dispatch to the requested vertex-label distribution."""
    distribution = normalize_distribution_name(args.label_distribution)
    if distribution == "uniform":
        return generate_uniform_labels(args.vertices, args.label_count, rng)
    if distribution == "zipf":
        return generate_zipf_labels(
            args.vertices, args.label_count, args.zipf_exponent, rng
        )
    if distribution == "degree-correlated":
        degrees = build_degree_counts(args.vertices, edges)
        return generate_degree_correlated_labels(
            args.vertices, args.label_count, degrees, rng
        )

    raise ValueError("unsupported label distribution: {}".format(args.label_distribution))


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vertices", required=True, type=int, help="Number of vertices.")
    parser.add_argument(
        "--avg-degree",
        required=True,
        type=float,
        help="Target average degree for the generated graph.",
    )
    parser.add_argument(
        "--label-count",
        required=True,
        type=int,
        help="Number of possible vertex labels.",
    )
    parser.add_argument(
        "--degree-distribution",
        default=FIXED_DEGREE_DISTRIBUTION,
        help="Fixed degree distribution. Only R-MAT is supported.",
    )
    parser.add_argument(
        "--label-distribution",
        required=True,
        help="Label distribution: uniform, Zipf, or degree-correlated.",
    )
    parser.add_argument("--output", required=True, help="Output .txt graph file path.")
    parser.add_argument("--seed", type=int, default=None, help="Optional random seed.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output file.")
    parser.add_argument(
        "--zipf-exponent",
        type=float,
        default=1.2,
        help="Zipf exponent used by --label-distribution Zipf.",
    )
    parser.add_argument(
        "--rmat-a",
        type=float,
        default=0.57,
        help="R-MAT initiator probability a.",
    )
    parser.add_argument(
        "--rmat-b",
        type=float,
        default=0.19,
        help="R-MAT initiator probability b.",
    )
    parser.add_argument(
        "--rmat-c",
        type=float,
        default=0.19,
        help="R-MAT initiator probability c.",
    )
    parser.add_argument(
        "--max-attempt-factor",
        type=int,
        default=50,
        help="Sampling attempts per requested edge before deterministic fill-in.",
    )
    return parser.parse_args()


def validate_args(args):
    if args.vertices <= 0:
        raise ValueError("--vertices must be positive")
    if args.label_count <= 0:
        raise ValueError("--label-count must be positive")
    if args.avg_degree < 0:
        raise ValueError("--avg-degree must be non-negative")
    if normalize_distribution_name(args.degree_distribution) != "r-mat":
        raise ValueError(
            "--degree-distribution is fixed to {}".format(FIXED_DEGREE_DISTRIBUTION)
        )
    if args.zipf_exponent <= 0:
        raise ValueError("--zipf-exponent must be positive")
    if args.max_attempt_factor <= 0:
        raise ValueError("--max-attempt-factor must be positive")

    rmat_d = 1.0 - args.rmat_a - args.rmat_b - args.rmat_c
    if min(args.rmat_a, args.rmat_b, args.rmat_c, rmat_d) < 0:
        raise ValueError("R-MAT probabilities must be non-negative and sum to <= 1")


def default_graph_id(output):
    """Use the containing parameter directory as the graph id for packaged data graphs."""
    if output.name == "graph_g.txt" and output.parent.name:
        return output.parent.name
    return output.stem


def main():
    args = parse_args()
    validate_args(args)

    output = Path(args.output)
    if output.exists() and not args.overwrite:
        print("skip existing {}".format(output))
        return 0

    rng = random.Random(args.seed)
    edges = generate_edges(args, rng)
    labels = generate_labels(args, edges, rng)
    write_standard_graph(
        output,
        graph_id=default_graph_id(output),
        vertices=labels,
        edges=[(u, v, None) for u, v in sorted(edges)],
    )
    print(
        "wrote synthetic graph: {} vertices, {} edges, {} labels -> {}".format(
            args.vertices, len(edges), args.label_count, output
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
