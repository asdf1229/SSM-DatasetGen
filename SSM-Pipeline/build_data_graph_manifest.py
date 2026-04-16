"""Build data_graph_manifest.csv from real and synthetic graph directories."""

import argparse
import re
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "SSM-GraphGen"))

from graph_utils import compute_graph_stats, iter_graph_files, read_standard_graph
from pipeline_utils import log, project_relative, write_csv

FIELDNAMES = [
    "graph_id",
    "graph_type",
    "file_path",
    "vertices",
    "edges",
    "avg_degree",
    "label_count",
    "degree_distribution",
    "label_distribution",
    "source_name",
    "is_valid",
]


def _parse_synthetic_params(path):
    """Best-effort parsing for filenames produced by generate_synthetic_graphs.py."""
    name = Path(path).stem
    result = {}
    patterns = {
        "degree_distribution": r"__degree_dist([^_]+)",
        "label_distribution": r"__label_dist([^_]+)",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, name)
        if match:
            result[key] = match.group(1)
    return result


def build_manifest_row(path, graph_type):
    """Build one data graph manifest row."""
    path = Path(path)
    row = {
        "graph_id": path.stem,
        "graph_type": graph_type,
        "file_path": project_relative(path, PROJECT_ROOT),
        "vertices": "",
        "edges": "",
        "avg_degree": "",
        "label_count": "",
        "degree_distribution": "",
        "label_distribution": "",
        "source_name": path.stem,
        "is_valid": "false",
    }

    try:
        graph = read_standard_graph(path)
        stats = compute_graph_stats(graph)
        row.update(stats)
        row["graph_id"] = graph.graph_id
        row["is_valid"] = "true"
    except Exception as exc:
        row["source_name"] = "{} ({})".format(path.stem, exc)

    if graph_type == "synthetic":
        row.update(_parse_synthetic_params(path))

    return row


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--real-dir",
        default=str(PROJECT_ROOT / "datasets" / "real"),
        help="Directory containing standard real graphs.",
    )
    parser.add_argument(
        "--synthetic-dir",
        default=str(PROJECT_ROOT / "datasets" / "synthetic"),
        help="Directory containing synthetic graphs.",
    )
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "datasets" / "manifests" / "data_graph_manifest.csv"),
        help="Output manifest CSV.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    rows = []
    for graph_file in iter_graph_files(args.real_dir):
        rows.append(build_manifest_row(graph_file, "real"))
    for graph_file in iter_graph_files(args.synthetic_dir):
        rows.append(build_manifest_row(graph_file, "synthetic"))

    write_csv(args.output, FIELDNAMES, rows)
    log("wrote data graph manifest with {} row(s): {}".format(len(rows), args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
