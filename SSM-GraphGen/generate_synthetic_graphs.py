"""Generate synthetic data graphs from prepared task parameter files."""

import argparse
import subprocess
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
    safe_token,
)

DEFAULT_TASKS = PROJECT_ROOT / "configs" / "ofat_tasks" / "data_graph_tasks.csv"


def load_ofat_config(path):
    """Read the data graph OFAT config."""
    config = load_simple_yaml(path)
    return config.get("default", {}), config.get("vary", {})


def load_generation_tasks(task_path, config_path):
    """Load prepared generation tasks, falling back to the legacy OFAT config."""
    tasks = load_task_specs(task_path)
    if tasks:
        return tasks

    log(
        "no data graph task file found at {}; fallback to {}".format(
            task_path, config_path
        )
    )
    defaults, vary = load_ofat_config(config_path)
    return build_ofat_tasks(defaults, vary)


DATA_GRAPH_FILENAME = "graph_g.txt"


def build_output_dirname(task):
    """Build a deterministic synthetic graph directory name from task parameters."""
    params = task["params"]
    parts = [
        "synthetic",
        task["task_id"],
        "v{}".format(safe_token(params.get("vertices", ""))),
        "deg{}".format(safe_token(params.get("avg_degree", ""))),
        "label_count{}".format(safe_token(params.get("label_count", ""))),
        "degree_dist{}".format(safe_token(params.get("degree_distribution", ""))),
        "label_dist{}".format(safe_token(params.get("label_distribution", ""))),
    ]
    return "__".join(parts)


def build_output_filename(task):
    """Build the synthetic graph path below the output root."""
    return str(Path(build_output_dirname(task)) / DATA_GRAPH_FILENAME)


def build_generator_command(tool_path, params, output_file):
    """Build the standard command expected by the synthetic generator script."""
    return [
        sys.executable,
        str(tool_path),
        "--vertices",
        str(params["vertices"]),
        "--avg-degree",
        str(params["avg_degree"]),
        "--label-count",
        str(params["label_count"]),
        "--degree-distribution",
        str(params["degree_distribution"]),
        "--label-distribution",
        str(params["label_distribution"]),
        "--output",
        str(output_file),
    ]


def run_generator(tool_path, task, output_file, overwrite=False):
    """Run the external synthetic graph generator for one task."""
    output_file = Path(output_file)
    if output_file.exists() and not overwrite:
        log("skip existing {}".format(output_file))
        return output_file

    output_file.parent.mkdir(parents=True, exist_ok=True)
    command = build_generator_command(tool_path, task["params"], output_file)
    subprocess.run(command, check=True)
    return output_file


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tasks",
        default=str(DEFAULT_TASKS),
        help="Prepared task CSV/JSON file, or a directory of per-task JSON files.",
    )
    parser.add_argument(
        "--config",
        default=str(PROJECT_ROOT / "configs" / "data_graph_ofat.yaml"),
        help="Fallback data graph OFAT config when --tasks is missing or empty.",
    )
    parser.add_argument(
        "--tool",
        default=str(PROJECT_ROOT / "tools" / "synthetic_graph_generator.py"),
        help="External synthetic graph generator Python script.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "datasets" / "synthetic"),
        help="Directory for generated synthetic graphs.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs.")
    parser.add_argument("--dry-run", action="store_true", help="Print tasks without running the tool.")
    parser.add_argument(
        "--fail-on-missing-tool",
        action="store_true",
        help="Return a non-zero exit code if the generator script is missing.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    tasks = load_generation_tasks(args.tasks, args.config)
    output_dir = Path(args.output_dir)
    tool_path = Path(args.tool)

    if args.dry_run:
        for task in tasks:
            print("{} -> {}".format(task["task_id"], build_output_filename(task)))
        return 0

    if not tool_path.exists():
        message = "missing synthetic graph generator: {}".format(tool_path)
        if args.fail_on_missing_tool:
            log(message)
            return 1
        log(message + "; skip synthetic graph generation")
        return 0

    generated = []
    for task in tasks:
        output_file = output_dir / build_output_filename(task)
        generated.append(
            run_generator(tool_path, task, output_file, overwrite=args.overwrite)
        )

    log("generated {} synthetic graph file(s)".format(len(generated)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
