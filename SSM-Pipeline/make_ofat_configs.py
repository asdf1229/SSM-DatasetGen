"""Expand OFAT YAML configs into reusable graph-generation task files."""

import argparse
from pathlib import Path

from pipeline_utils import (
    build_ofat_tasks,
    load_simple_yaml,
    log,
    safe_token,
    task_to_row,
    write_csv,
    write_json,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _tasks(config_path):
    config = load_simple_yaml(config_path)
    defaults = config.get("default", {})
    vary = config.get("vary", {})
    return build_ofat_tasks(defaults, vary)


def _task_rows(tasks):
    return [task_to_row(task) for task in tasks]


def _fieldnames(rows):
    fixed = ["task_id", "generation_mode", "varied_parameter", "varied_value"]
    dynamic = []
    for row in rows:
        for key in row:
            if key not in fixed and key not in dynamic:
                dynamic.append(key)
    return fixed + dynamic


def _write_task_outputs(tasks, output_dir, stem):
    rows = _task_rows(tasks)
    csv_output = output_dir / "{}_tasks.csv".format(stem)
    json_output = output_dir / "{}_tasks.json".format(stem)
    per_task_dir = output_dir / stem

    write_csv(csv_output, _fieldnames(rows), rows)
    write_json(json_output, {"tasks": tasks})
    for task in tasks:
        write_json(per_task_dir / "{}.json".format(safe_token(task["task_id"])), task)

    return csv_output, json_output, per_task_dir


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-config",
        default=str(PROJECT_ROOT / "configs" / "data_graph_ofat.yaml"),
        help="Data graph OFAT config.",
    )
    parser.add_argument(
        "--query-config",
        default=str(PROJECT_ROOT / "configs" / "query_graph_ofat.yaml"),
        help="Query graph OFAT config.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "configs" / "ofat_tasks"),
        help="Directory for expanded task parameter files.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)

    data_tasks = _tasks(args.data_config)
    query_tasks = _tasks(args.query_config)

    data_csv, data_json, data_dir = _write_task_outputs(data_tasks, output_dir, "data_graph")
    query_csv, query_json, query_dir = _write_task_outputs(query_tasks, output_dir, "query_graph")

    log(
        "wrote {} data OFAT task(s): {}, {}, {}".format(
            len(data_tasks), data_csv, data_json, data_dir
        )
    )
    log(
        "wrote {} query OFAT task(s): {}, {}, {}".format(
            len(query_tasks), query_csv, query_json, query_dir
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
