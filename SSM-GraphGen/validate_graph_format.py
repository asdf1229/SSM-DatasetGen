"""Validate standard SSM graph files and write a CSV report."""

import argparse
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "SSM-Pipeline"))

from graph_utils import compute_graph_stats, iter_graph_files, read_standard_graph
from pipeline_utils import log, project_relative, write_csv

FIELDNAMES = [
    "file_path",
    "graph_id",
    "vertices",
    "edges",
    "avg_degree",
    "label_count",
    "is_valid",
    "errors",
]

DATA_GRAPH_FILENAME = "graph_g.txt"
QUERY_GRAPH_DIRNAME = "query_graph"


def _is_query_graph_path(path):
    return QUERY_GRAPH_DIRNAME in Path(path).parts


def _iter_validation_files(input_path):
    """Yield data graph files, preferring packaged synthetic graph_g.txt files."""
    input_path = Path(input_path)
    if input_path.is_file():
        yield from iter_graph_files(input_path)
        return

    if input_path.name == "synthetic" and input_path.is_dir():
        packaged = sorted(
            child
            for child in input_path.rglob(DATA_GRAPH_FILENAME)
            if not _is_query_graph_path(child)
        )
        if packaged:
            for child in packaged:
                yield child
            return

    skip_query_graphs = QUERY_GRAPH_DIRNAME not in input_path.parts
    for child in iter_graph_files(input_path):
        if not skip_query_graphs or not _is_query_graph_path(child):
            yield child


def validate_graph_file(path):
    """Validate one graph file and return a report row."""
    path = Path(path)
    errors = []

    try:
        graph = read_standard_graph(path)
    except Exception as exc:
        return {
            "file_path": project_relative(path),
            "graph_id": path.stem,
            "vertices": "",
            "edges": "",
            "avg_degree": "",
            "label_count": "",
            "is_valid": "false",
            "errors": str(exc),
        }

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

    stats = compute_graph_stats(graph)
    return {
        "file_path": project_relative(path),
        "graph_id": graph.graph_id,
        "vertices": stats["vertices"],
        "edges": stats["edges"],
        "avg_degree": stats["avg_degree"],
        "label_count": stats["label_count"],
        "is_valid": "true" if not errors else "false",
        "errors": "; ".join(errors),
    }


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        nargs="*",
        default=[
            str(PROJECT_ROOT / "datasets" / "real"),
            str(PROJECT_ROOT / "datasets" / "synthetic"),
        ],
        help="Graph file or directories to validate.",
    )
    parser.add_argument(
        "--report",
        default=str(PROJECT_ROOT / "datasets" / "manifests" / "graph_validation_report.csv"),
        help="CSV report path.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    rows = []
    for input_path in args.input:
        for graph_file in _iter_validation_files(input_path):
            rows.append(validate_graph_file(graph_file))

    write_csv(args.report, FIELDNAMES, rows)
    log("wrote validation report with {} row(s): {}".format(len(rows), args.report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
