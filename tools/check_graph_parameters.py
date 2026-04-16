#!/usr/bin/env python3
"""Inspect standard-format SSM graph files and check expected parameters."""

import argparse
import csv
import math
import sys
from collections import Counter
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "SSM-GraphGen"))

from graph_utils import canonicalize_undirected_edges, read_standard_graph

FIELDNAMES = [
    "file_path",
    "graph_id",
    "vertices",
    "edges",
    "avg_degree",
    "density",
    "label_count",
    "min_degree",
    "max_degree",
    "isolated_vertices",
    "components",
    "largest_component",
    "is_connected",
    "top_labels",
    "is_valid",
    "checks_passed",
    "errors",
]


def iter_input_files(paths):
    """Yield graph files from file or directory arguments."""
    for path_value in paths:
        path = Path(path_value)
        if not path.exists():
            yield path
            continue
        if path.is_file():
            yield path
            continue
        for child in sorted(path.rglob("*")):
            if not child.is_file() or child.name.startswith("."):
                continue
            if child.suffix.lower() in (".csv", ".json", ".yaml", ".yml", ".md"):
                continue
            yield child


def connected_components(vertices, edges):
    """Return connected component sizes for an undirected graph."""
    adjacency = {vertex_id: set() for vertex_id in vertices}
    for u, v, _label in edges:
        adjacency.setdefault(u, set()).add(v)
        adjacency.setdefault(v, set()).add(u)

    visited = set()
    sizes = []
    for start in adjacency:
        if start in visited:
            continue
        stack = [start]
        visited.add(start)
        size = 0
        while stack:
            vertex = stack.pop()
            size += 1
            for neighbor in adjacency.get(vertex, set()):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                stack.append(neighbor)
        sizes.append(size)
    return sizes


def validate_graph_shape(graph):
    """Validate ids, duplicate edges, self-loops, and undefined references."""
    errors = []
    vertex_ids = set(graph.vertices.keys())
    expected_ids = set(range(len(graph.vertices)))
    if vertex_ids != expected_ids:
        missing = sorted(expected_ids - vertex_ids)
        extra = sorted(vertex_ids - expected_ids)
        errors.append("non-contiguous vertex ids: missing={}, extra={}".format(missing, extra))

    seen_edges = set()
    for u, v, _label in graph.edges:
        if u == v:
            errors.append("self-loop edge {}-{}".format(u, v))
        if u not in vertex_ids or v not in vertex_ids:
            errors.append("edge {}-{} references undefined vertex".format(u, v))
        key = tuple(sorted((u, v)))
        if key in seen_edges:
            errors.append("duplicate undirected edge {}-{}".format(key[0], key[1]))
        seen_edges.add(key)
    return errors


def graph_stats(path):
    """Build a parameter row for one graph file."""
    graph = read_standard_graph(path)
    shape_errors = validate_graph_shape(graph)
    edges = canonicalize_undirected_edges(graph.edges)
    vertices_count = len(graph.vertices)
    edges_count = len(edges)
    max_edges = vertices_count * (vertices_count - 1) / 2.0
    density = (edges_count / max_edges) if max_edges else 0.0

    degrees = {vertex_id: 0 for vertex_id in graph.vertices}
    for u, v, _label in edges:
        degrees[u] = degrees.get(u, 0) + 1
        degrees[v] = degrees.get(v, 0) + 1

    component_sizes = connected_components(graph.vertices, edges)
    label_counts = Counter(graph.vertices.values())
    top_labels = ";".join(
        "{}:{}".format(label, count)
        for label, count in label_counts.most_common(5)
    )

    return {
        "file_path": str(Path(path).resolve()),
        "graph_id": graph.graph_id,
        "vertices": vertices_count,
        "edges": edges_count,
        "avg_degree": round((2.0 * edges_count / vertices_count) if vertices_count else 0.0, 6),
        "density": round(density, 6),
        "label_count": len(label_counts),
        "min_degree": min(degrees.values()) if degrees else 0,
        "max_degree": max(degrees.values()) if degrees else 0,
        "isolated_vertices": sum(1 for degree in degrees.values() if degree == 0),
        "components": len(component_sizes),
        "largest_component": max(component_sizes) if component_sizes else 0,
        "is_connected": "true" if len(component_sizes) <= 1 else "false",
        "top_labels": top_labels,
        "is_valid": "true" if not shape_errors else "false",
        "checks_passed": "",
        "errors": "; ".join(shape_errors),
    }


def values_close(actual, expected, tolerance):
    """Return whether two numeric values are within tolerance."""
    return math.isclose(float(actual), float(expected), abs_tol=tolerance)


def apply_expected_checks(row, args):
    """Apply optional expected-value checks to a stats row."""
    errors = []
    if args.expected_vertices is not None and int(row["vertices"]) != args.expected_vertices:
        errors.append("vertices expected {}, got {}".format(args.expected_vertices, row["vertices"]))
    if args.expected_edges is not None and int(row["edges"]) != args.expected_edges:
        errors.append("edges expected {}, got {}".format(args.expected_edges, row["edges"]))
    if args.expected_avg_degree is not None and not values_close(
        row["avg_degree"], args.expected_avg_degree, args.tolerance
    ):
        errors.append(
            "avg_degree expected {}, got {}".format(
                args.expected_avg_degree, row["avg_degree"]
            )
        )
    if args.expected_label_count is not None and int(row["label_count"]) != args.expected_label_count:
        errors.append(
            "label_count expected {}, got {}".format(
                args.expected_label_count, row["label_count"]
            )
        )
    if args.require_connected and row["is_connected"] != "true":
        errors.append("graph is not connected")

    if row["errors"]:
        errors.insert(0, row["errors"])

    row["checks_passed"] = "true" if not errors else "false"
    row["errors"] = "; ".join(errors)
    return row


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", help="Graph file(s) or directories to inspect.")
    parser.add_argument("--expected-vertices", type=int, default=None)
    parser.add_argument("--expected-edges", type=int, default=None)
    parser.add_argument("--expected-avg-degree", type=float, default=None)
    parser.add_argument("--expected-label-count", type=int, default=None)
    parser.add_argument("--require-connected", action="store_true")
    parser.add_argument("--tolerance", type=float, default=1e-6)
    parser.add_argument("--output", default="-", help="CSV output path, or '-' for stdout.")
    return parser.parse_args()


def main():
    args = parse_args()
    rows = []
    for path in iter_input_files(args.paths):
        try:
            row = graph_stats(path)
        except Exception as exc:
            row = {
                "file_path": str(Path(path).resolve()),
                "graph_id": Path(path).stem,
                "is_valid": "false",
                "checks_passed": "false",
                "errors": str(exc),
            }
        rows.append(apply_expected_checks(row, args))

    output_handle = sys.stdout
    close_output = False
    if args.output != "-":
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_handle = output_path.open("w", encoding="utf-8", newline="")
        close_output = True

    try:
        writer = csv.DictWriter(output_handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in FIELDNAMES})
    finally:
        if close_output:
            output_handle.close()

    return 0 if all(row.get("checks_passed") == "true" for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
