"""Build query_graph_manifest.csv from generated query graph files."""

import argparse
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "SSM-GraphGen"))
sys.path.insert(0, str(PROJECT_ROOT / "SSM-QueryGen"))

from graph_utils import compute_graph_stats, iter_graph_files, read_standard_graph
from pipeline_utils import load_task_specs, log, project_relative, safe_token, write_csv
from query_generator_wrapper import avg_degree_to_density
from query_utils import parse_query_filename

DEFAULT_TASKS = PROJECT_ROOT / "configs" / "ofat_tasks" / "query_graph_tasks.csv"
QUERY_GRAPH_DIRNAME = "query_graph"

FIELDNAMES = [
    "query_id",
    "source_graph_id",
    "query_file",
    "vertices_num",
    "avg_degree",
    "density",
    "query_index",
    "generation_mode",
]


def _is_query_graph_path(path):
    return QUERY_GRAPH_DIRNAME in Path(path).parts


def _has_query_graph_dir(path):
    path = Path(path)
    if not path.exists():
        return False
    if path.name == QUERY_GRAPH_DIRNAME:
        return True
    return any(
        child.is_dir() and child.name == QUERY_GRAPH_DIRNAME
        for child in path.rglob(QUERY_GRAPH_DIRNAME)
    )


def iter_query_graph_files(path):
    """Yield query graph files from packaged or legacy query directories."""
    path = Path(path)
    if not path.exists():
        return

    has_packaged_queries = _has_query_graph_dir(path)
    if path.name == "synthetic" and not has_packaged_queries:
        return
    for child in iter_graph_files(path):
        if not has_packaged_queries or _is_query_graph_path(child):
            yield child


def _default_query_roots():
    """Prefer packaged real/synthetic query_graph directories, then legacy datasets/queries."""
    real_root = PROJECT_ROOT / "datasets" / "real"
    synthetic_root = PROJECT_ROOT / "datasets" / "synthetic"
    packaged_roots = []
    for root in (real_root, synthetic_root):
        if _has_query_graph_dir(root) and any(iter_query_graph_files(root)):
            packaged_roots.append(root)
    if packaged_roots:
        return packaged_roots
    return [PROJECT_ROOT / "datasets" / "queries"]


def _load_task_lookup(path):
    return {task["task_id"]: task.get("params", {}) for task in load_task_specs(path)}


def _path_metadata(path, queries_root):
    """Infer source graph id and query task id from the containing directories."""
    path = Path(path)
    root = Path(queries_root)
    try:
        parts = path.relative_to(root).parts
    except ValueError:
        parts = path.parts
    if QUERY_GRAPH_DIRNAME not in parts and QUERY_GRAPH_DIRNAME in path.parts:
        parts = path.parts

    result = {}
    if QUERY_GRAPH_DIRNAME in parts:
        query_graph_index = parts.index(QUERY_GRAPH_DIRNAME)
        if query_graph_index >= 1:
            result["source_graph_id"] = parts[query_graph_index - 1]
        if len(parts) > query_graph_index + 2:
            result["generation_mode"] = parts[query_graph_index + 1]
    else:
        if len(parts) >= 3:
            result["source_graph_id"] = parts[0]
            result["generation_mode"] = parts[1]
        elif len(parts) >= 2:
            result["generation_mode"] = parts[-2]

    return result


def _query_id(row, path):
    parts = [
        row.get("source_graph_id", ""),
        row.get("generation_mode", ""),
        row.get("query_index", ""),
    ]
    if all(parts):
        return "__".join(safe_token(part) for part in parts)
    return path.stem


def build_manifest_row(path, queries_root, task_lookup=None):
    """Build one query graph manifest row."""
    path = Path(path)
    task_lookup = task_lookup or {}
    parsed = parse_query_filename(path.name)
    path_metadata = _path_metadata(path, queries_root)
    generation_mode = parsed.get(
        "generation_mode", path_metadata.get("generation_mode", path.parent.name)
    )
    task_params = task_lookup.get(generation_mode, {})
    row = {
        "query_id": parsed.get("query_id", path.stem),
        "source_graph_id": parsed.get(
            "source_graph_id", path_metadata.get("source_graph_id", "")
        ),
        "query_file": project_relative(path, PROJECT_ROOT),
        "vertices_num": parsed.get("vertices_num", task_params.get("vertices_num", "")),
        "avg_degree": parsed.get("avg_degree", task_params.get("avg_degree", "")),
        "density": "",
        "query_index": parsed.get("query_index", ""),
        "generation_mode": generation_mode,
    }
    row["query_id"] = _query_id(row, path)

    try:
        graph = read_standard_graph(path)
        stats = compute_graph_stats(graph)
        row["vertices_num"] = row["vertices_num"] or stats["vertices"]
        row["avg_degree"] = stats["avg_degree"]
    except Exception:
        pass

    if row["vertices_num"] and row["avg_degree"]:
        row["density"] = round(
            avg_degree_to_density(row["vertices_num"], row["avg_degree"]), 6
        )

    return row


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--queries-dir",
        nargs="*",
        default=None,
        help=(
            "Directory or directories containing query graph files. "
            "Defaults to packaged datasets/real and datasets/synthetic query_graph "
            "directories when present."
        ),
    )
    parser.add_argument(
        "--tasks",
        default=str(DEFAULT_TASKS),
        help="Prepared query task CSV/JSON file used to recover parameters from short filenames.",
    )
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "datasets" / "manifests" / "query_graph_manifest.csv"),
        help="Output manifest CSV.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    query_roots = [Path(path).resolve() for path in (args.queries_dir or _default_query_roots())]
    task_lookup = _load_task_lookup(args.tasks)
    rows = []
    for queries_root in query_roots:
        rows.extend(
            build_manifest_row(path, queries_root, task_lookup)
            for path in iter_query_graph_files(queries_root)
        )
    write_csv(args.output, FIELDNAMES, rows)
    log("wrote query graph manifest with {} row(s): {}".format(len(rows), args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
