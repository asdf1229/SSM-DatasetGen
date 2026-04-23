"""Shared helpers for the SSM-DatasetGen workflow."""

import csv
import json
import re
from datetime import datetime
from pathlib import Path

TASK_METADATA_FIELDS = [
    "task_id",
    "generation_mode",
    "varied_parameter",
    "varied_value",
]


def project_root():
    """Return the SSM-DatasetGen project root."""
    return Path(__file__).resolve().parents[1]


def ensure_dir(path):
    """Create a directory if it does not already exist."""
    Path(path).mkdir(parents=True, exist_ok=True)


def log(message):
    """Print a timestamped workflow message."""
    print("[{}] {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), message))


def safe_token(value):
    """Return a filesystem-friendly token for a parameter value."""
    token = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value).strip())
    token = token.strip("._-")
    return token or "value"


def coerce_scalar(value):
    """Parse a small YAML-like scalar into a Python value."""
    if value is None:
        return None
    if not isinstance(value, str):
        return value

    value = value.strip()
    if not value:
        return ""

    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]

    lower = value.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    if lower in ("null", "none"):
        return None

    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        return value


def parse_inline_list(value):
    """Parse a simple inline list such as [1, 2, R-MAT]."""
    inner = value.strip()[1:-1].strip()
    if not inner:
        return []
    return [coerce_scalar(part.strip()) for part in inner.split(",")]


def load_simple_yaml(path):
    """Load the simple two-level YAML shape used by the OFAT configs."""
    path = Path(path)
    data = {}
    section = None

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue

        if not line.startswith((" ", "\t")) and line.endswith(":"):
            section = line[:-1].strip()
            data[section] = {}
            continue

        if section is None or ":" not in line:
            continue

        key, raw_value = line.strip().split(":", 1)
        raw_value = raw_value.strip()
        if raw_value.startswith("[") and raw_value.endswith("]"):
            value = parse_inline_list(raw_value)
        else:
            value = coerce_scalar(raw_value)
        data[section][key.strip()] = value

    return data


def build_ofat_tasks(defaults, vary, include_baseline=True):
    """Build one-factor-at-a-time tasks from defaults and vary values."""
    tasks = []

    if include_baseline:
        tasks.append(
            {
                "task_id": "baseline",
                "generation_mode": "baseline",
                "varied_parameter": "",
                "varied_value": "",
                "params": dict(defaults),
            }
        )

    for parameter, values in vary.items():
        if not isinstance(values, list):
            values = [values]

        for value in values:
            if parameter in defaults and value == defaults[parameter]:
                continue

            params = dict(defaults)
            params[parameter] = value
            task_id = "{}_{}".format(safe_token(parameter), safe_token(value))
            tasks.append(
                {
                    "task_id": task_id,
                    "generation_mode": "vary_{}".format(parameter),
                    "varied_parameter": parameter,
                    "varied_value": value,
                    "params": params,
                }
            )

    return tasks


def task_to_row(task):
    """Flatten a task spec into one CSV row."""
    row = {
        "task_id": task.get("task_id", ""),
        "generation_mode": task.get("generation_mode", ""),
        "varied_parameter": task.get("varied_parameter", ""),
        "varied_value": task.get("varied_value", ""),
    }
    row.update(task.get("params", {}))
    return row


def task_from_row(row):
    """Build a task spec from one CSV row."""
    params = {}
    for key, value in row.items():
        if key in TASK_METADATA_FIELDS:
            continue
        if value == "":
            continue
        params[key] = coerce_scalar(value)

    return {
        "task_id": row.get("task_id", ""),
        "generation_mode": row.get("generation_mode", ""),
        "varied_parameter": row.get("varied_parameter", ""),
        "varied_value": coerce_scalar(row.get("varied_value", "")),
        "params": params,
    }


def normalize_task(task):
    """Return a task spec with the expected top-level shape."""
    if "params" in task:
        normalized = {
            "task_id": task.get("task_id", ""),
            "generation_mode": task.get("generation_mode", ""),
            "varied_parameter": task.get("varied_parameter", ""),
            "varied_value": task.get("varied_value", ""),
            "params": dict(task.get("params", {})),
        }
    else:
        normalized = task_from_row(task)

    if not normalized["task_id"]:
        normalized["task_id"] = safe_token(normalized.get("generation_mode", "task"))
    return normalized


def read_csv(path):
    """Read CSV rows into dictionaries."""
    path = Path(path)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def read_json(path):
    """Read a JSON file."""
    path = Path(path)
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path, data):
    """Write JSON with stable formatting."""
    path = Path(path)
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(data, handle, indent=2, sort_keys=True)
        handle.write("\n")


def write_csv(path, fieldnames, rows):
    """Write dictionaries to a CSV file with a stable header."""
    path = Path(path)
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def load_task_specs(path):
    """Load task specs from a CSV file, JSON file, or directory of JSON files."""
    path = Path(path)
    if not path.exists():
        return []

    if path.is_dir():
        tasks = []
        for child in sorted(path.glob("*.json")):
            tasks.extend(load_task_specs(child))
        return tasks

    suffix = path.suffix.lower()
    if suffix == ".csv":
        return [task_from_row(row) for row in read_csv(path)]

    if suffix == ".json":
        payload = read_json(path)
        if payload is None:
            return []
        if isinstance(payload, list):
            return [normalize_task(task) for task in payload]
        if isinstance(payload, dict) and "tasks" in payload:
            return [normalize_task(task) for task in payload["tasks"]]
        if isinstance(payload, dict):
            return [normalize_task(payload)]

    raise ValueError("unsupported task spec path: {}".format(path))


def project_relative(path, root=None):
    """Return a POSIX-style path relative to the project root when possible."""
    root = Path(root or project_root()).resolve()
    path = Path(path).resolve()
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def resolve_project_path(path, root=None):
    """Resolve a path that may be relative to the project root."""
    root = Path(root or project_root())
    path = Path(path)
    if path.is_absolute():
        return path
    return root / path
