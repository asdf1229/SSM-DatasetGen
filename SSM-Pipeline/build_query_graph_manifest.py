"""Build query_graph_manifest.csv from generated query graph files."""

import argparse
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "SSM-GraphGen"))
sys.path.insert(0, str(PROJECT_ROOT / "SSM-QueryGen"))

from graph_utils import compute_graph_stats, iter_graph_files, read_standard_graph
from pipeline_utils import log, project_relative, write_csv
from query_generator_wrapper import avg_degree_to_density
from query_utils import parse_query_filename

FIELDNAMES = [
    "query_id",
    "source_graph_id",
    "query_file",
    "vertices_num",
    "avg_degree",
    "missing_edge_threshold",
    "density",
    "query_index",
    "generation_mode",
]


def build_manifest_row(path, queries_root):
    """Build one query graph manifest row."""
    path = Path(path)
    parsed = parse_query_filename(path.name)
    row = {
        "query_id": parsed.get("query_id", path.stem),
        "source_graph_id": parsed.get("source_graph_id", ""),
        "query_file": project_relative(path, PROJECT_ROOT),
        "vertices_num": parsed.get("vertices_num", ""),
        "avg_degree": parsed.get("avg_degree", ""),
        "missing_edge_threshold": parsed.get("missing_edge_threshold", ""),
        "density": "",
        "query_index": parsed.get("query_index", ""),
        "generation_mode": parsed.get("generation_mode", path.parent.name),
    }

    try:
        graph = read_standard_graph(path)
        stats = compute_graph_stats(graph)
        row["vertices_num"] = row["vertices_num"] or stats["vertices"]
        row["avg_degree"] = row["avg_degree"] or stats["avg_degree"]
    except Exception:
        pass

    if row["vertices_num"] and row["avg_degree"]:
        row["density"] = round(
            avg_degree_to_density(row["vertices_num"], row["avg_degree"]), 6
        )

    if not row["source_graph_id"]:
        try:
            relative = path.relative_to(queries_root)
            if len(relative.parts) >= 2:
                row["source_graph_id"] = relative.parts[0]
        except ValueError:
            pass

    return row


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--queries-dir",
        default=str(PROJECT_ROOT / "datasets" / "queries"),
        help="Directory containing query graph files.",
    )
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "datasets" / "manifests" / "query_graph_manifest.csv"),
        help="Output manifest CSV.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    queries_root = Path(args.queries_dir).resolve()
    rows = [build_manifest_row(path, queries_root) for path in iter_graph_files(args.queries_dir)]
    write_csv(args.output, FIELDNAMES, rows)
    log("wrote query graph manifest with {} row(s): {}".format(len(rows), args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
