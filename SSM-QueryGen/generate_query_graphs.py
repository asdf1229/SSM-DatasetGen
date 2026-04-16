"""Generate query graphs from prepared task parameter files."""

import argparse
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "SSM-Pipeline"))

from pipeline_utils import (
    build_ofat_tasks,
    load_simple_yaml,
    load_task_specs,
    log,
    read_csv,
    resolve_project_path,
)
from query_generator_wrapper import call_query_generator
from query_utils import safe_token

DEFAULT_TASKS = PROJECT_ROOT / "configs" / "ofat_tasks" / "query_graph_tasks.csv"
DATA_GRAPH_FILENAME = "graph_g.txt"
QUERY_GRAPH_DIRNAME = "query_graph"
LEGACY_QUERY_ROOT = PROJECT_ROOT / "datasets" / "queries"


def load_data_manifest(path):
    """Read the data graph manifest."""
    return read_csv(path)


def load_query_ofat_config(path):
    """Read the query graph OFAT config."""
    config = load_simple_yaml(path)
    return config.get("default", {}), config.get("vary", {})


def build_query_ofat_tasks(config_path):
    """Build query graph OFAT tasks from the legacy config."""
    defaults, vary = load_query_ofat_config(config_path)
    return build_ofat_tasks(defaults, vary)


def load_query_tasks(task_path, config_path):
    """Load prepared query tasks, falling back to the legacy OFAT config."""
    tasks = load_task_specs(task_path)
    if tasks:
        return tasks

    log(
        "no query graph task file found at {}; fallback to {}".format(
            task_path, config_path
        )
    )
    return build_query_ofat_tasks(config_path)


def _row_is_valid(row):
    value = str(row.get("is_valid", "true")).strip().lower()
    return value not in ("0", "false", "no", "invalid")


def _source_path(row):
    value = row.get("file_path", "")
    if not value:
        return None
    return resolve_project_path(value, PROJECT_ROOT)


def _query_output_base(source_file, source_graph_id, row, output_root):
    """Return the directory that should contain query task subdirectories."""
    if output_root is not None:
        return Path(output_root) / safe_token(source_graph_id)
    if row.get("graph_type") == "synthetic" and source_file.name == DATA_GRAPH_FILENAME:
        return source_file.parent / QUERY_GRAPH_DIRNAME
    return LEGACY_QUERY_ROOT / safe_token(source_graph_id)


def generate_queries_for_graph(row, tasks, tool_path, output_root=None):
    """Generate all query graph OFAT tasks for one data graph row."""
    source_file = _source_path(row)
    if source_file is None or not source_file.exists():
        log("skip missing data graph file for graph_id={}".format(row.get("graph_id", "")))
        return []

    source_graph_id = row.get("graph_id") or source_file.stem
    query_base = _query_output_base(source_file, source_graph_id, row, output_root)
    generated = []
    for task in tasks:
        params = task["params"]
        task_dir = query_base / task["task_id"]
        generated.extend(
            call_query_generator(
                tool_path=tool_path,
                data_graph_file=source_file,
                output_dir=task_dir,
                output_prefix="query",
                vertices_num=params["vertices_num"],
                avg_degree=params["avg_degree"],
                missing_edge_threshold=params["missing_edge_threshold"],
                num_per_setting=params["num_per_setting"],
            )
        )
    return generated


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        default=str(PROJECT_ROOT / "datasets" / "manifests" / "data_graph_manifest.csv"),
        help="Data graph manifest CSV.",
    )
    parser.add_argument(
        "--tasks",
        default=str(DEFAULT_TASKS),
        help="Prepared task CSV/JSON file, or a directory of per-task JSON files.",
    )
    parser.add_argument(
        "--config",
        default=str(PROJECT_ROOT / "configs" / "query_graph_ofat.yaml"),
        help="Fallback query graph OFAT config when --tasks is missing or empty.",
    )
    parser.add_argument(
        "--tool",
        default=str(PROJECT_ROOT / "tools" / "query_graph_generator.py"),
        help="External query graph generator Python script.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help=(
            "Optional legacy root directory for generated query graphs. "
            "When omitted, synthetic graph_g.txt inputs write to sibling query_graph/ directories."
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Print tasks without running the tool.")
    parser.add_argument(
        "--fail-on-missing-tool",
        action="store_true",
        help="Return a non-zero exit code if the generator script is missing.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    rows = [row for row in load_data_manifest(args.manifest) if _row_is_valid(row)]
    tasks = load_query_tasks(args.tasks, args.config)
    tool_path = Path(args.tool)

    if args.dry_run:
        log("data graph rows: {}".format(len(rows)))
        for task in tasks:
            print("{} -> {}".format(task["task_id"], task["params"]))
        return 0

    if not rows:
        log("no valid data graph rows found in {}".format(args.manifest))
        return 0

    if not tool_path.exists():
        message = "missing query graph generator: {}".format(tool_path)
        if args.fail_on_missing_tool:
            log(message)
            return 1
        log(message + "; skip query graph generation")
        return 0

    generated = []
    for row in rows:
        generated.extend(generate_queries_for_graph(row, tasks, tool_path, args.output_dir))

    log("generated {} query graph file(s)".format(len(generated)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
