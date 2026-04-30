#!/usr/bin/env python3
"""Rewrite legacy run outputs into packaged data/query graph directories."""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
sys.path.insert(0, str(CURRENT_DIR))

from pipeline_utils import read_csv, resolve_project_path, safe_token

DATA_GRAPH_FILENAME = "graph_g.txt"
QUERY_GRAPH_DIRNAME = "query_graph"


def log(message):
    print(message, flush=True)


def resolve_run_dir(value):
    """Resolve either a full run path or a run id below datasets/runs."""
    path = Path(value)
    if path.exists():
        return path.resolve()

    candidate = PROJECT_ROOT / "datasets" / "runs" / value
    if candidate.exists():
        return candidate.resolve()

    return path.resolve()


def load_data_manifest(run_dir):
    manifest = run_dir / "manifests" / "data_graph_manifest.csv"
    return read_csv(manifest) if manifest.exists() else []


def row_path(row):
    value = row.get("file_path", "")
    if not value:
        return None
    return resolve_project_path(value, PROJECT_ROOT).resolve()


def graph_id_from_relative(path, root):
    relative = Path(path).resolve().relative_to(Path(root).resolve()).with_suffix("")
    return "__".join(safe_token(part) for part in relative.parts)


def manifest_lookup(rows):
    by_path = {}
    by_id = {}
    for row in rows:
        graph_id = row.get("source_name") or row.get("graph_id")
        if graph_id:
            by_id[graph_id] = row
        path = row_path(row)
        if path is not None:
            by_path[path] = row
    return by_path, by_id


def is_query_graph_path(path):
    return QUERY_GRAPH_DIRNAME in Path(path).parts


def iter_real_data_files(real_root):
    if not real_root.exists():
        return
    for child in sorted(real_root.rglob("*")):
        if not child.is_file() or child.name.startswith("."):
            continue
        if is_query_graph_path(child):
            continue
        if child.name == DATA_GRAPH_FILENAME:
            continue
        if child.suffix.lower() in (".csv", ".json", ".yaml", ".yml", ".md"):
            continue
        yield child


def existing_package_dirs(real_root):
    packages = {}
    if not real_root.exists():
        return packages
    for graph_file in sorted(real_root.rglob(DATA_GRAPH_FILENAME)):
        if is_query_graph_path(graph_file):
            continue
        packages[graph_file.parent.name] = graph_file.parent
    return packages


def move_file(source, target, overwrite=False, dry_run=False):
    if source.resolve() == target.resolve():
        return
    if target.exists():
        if not overwrite:
            raise FileExistsError("{} already exists; use --overwrite".format(target))
        if dry_run:
            log("would replace {}".format(target))
        elif target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()

    log("{} {} -> {}".format("would move" if dry_run else "move", source, target))
    if dry_run:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(target))


def merge_dir(source, target, overwrite=False, dry_run=False):
    if not source.exists():
        return

    log("{} {} -> {}".format("would merge" if dry_run else "merge", source, target))
    if dry_run:
        return

    target.mkdir(parents=True, exist_ok=True)
    for child in sorted(source.iterdir()):
        destination = target / child.name
        if destination.exists():
            if child.is_dir() and destination.is_dir():
                merge_dir(child, destination, overwrite=overwrite, dry_run=False)
                continue
            if not overwrite:
                raise FileExistsError(
                    "{} already exists; use --overwrite".format(destination)
                )
            if destination.is_dir():
                shutil.rmtree(destination)
            else:
                destination.unlink()
        shutil.move(str(child), str(destination))

    try:
        source.rmdir()
    except OSError:
        pass


def remove_empty_dirs(root, stop_at):
    root = Path(root)
    stop_at = Path(stop_at).resolve()
    for directory in sorted(
        (path for path in root.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    ):
        if directory.resolve() == stop_at:
            continue
        try:
            directory.rmdir()
        except OSError:
            pass


def package_real_graphs(run_dir, overwrite=False, dry_run=False):
    real_root = run_dir / "real"
    rows = load_data_manifest(run_dir)
    by_path, by_id = manifest_lookup(rows)
    packages = existing_package_dirs(real_root)

    for graph_file in iter_real_data_files(real_root):
        row = by_path.get(graph_file.resolve())
        graph_id = None
        if row is not None:
            graph_id = row.get("source_name") or row.get("graph_id")
        if not graph_id:
            graph_id = graph_id_from_relative(graph_file, real_root)

        target = real_root / safe_token(graph_id) / DATA_GRAPH_FILENAME
        move_file(graph_file, target, overwrite=overwrite, dry_run=dry_run)
        packages[safe_token(graph_id)] = target.parent

    if not dry_run and real_root.exists():
        remove_empty_dirs(real_root, real_root)

    for row in rows:
        graph_id = row.get("source_name") or row.get("graph_id")
        if not graph_id or graph_id in packages:
            continue
        source = row_path(row)
        if source and source.name == DATA_GRAPH_FILENAME and source.exists():
            packages[graph_id] = source.parent

    for graph_id, row in by_id.items():
        if graph_id in packages:
            continue
        source = row_path(row)
        if source and source.exists() and source.name == DATA_GRAPH_FILENAME:
            packages[graph_id] = source.parent

    return packages


def package_legacy_queries(run_dir, packages, overwrite=False, dry_run=False):
    legacy_queries = run_dir / "queries"
    if not legacy_queries.exists():
        return

    rows = load_data_manifest(run_dir)
    _by_path, by_id = manifest_lookup(rows)

    for query_source_dir in sorted(path for path in legacy_queries.iterdir() if path.is_dir()):
        graph_id = query_source_dir.name
        package_dir = packages.get(graph_id)

        row = by_id.get(graph_id)
        source = row_path(row) if row is not None else None
        if package_dir is None and source is not None and source.name == DATA_GRAPH_FILENAME:
            package_dir = source.parent
        if package_dir is None:
            package_dir = run_dir / "real" / graph_id

        merge_dir(
            query_source_dir,
            package_dir / QUERY_GRAPH_DIRNAME,
            overwrite=overwrite,
            dry_run=dry_run,
        )

    if not dry_run:
        remove_empty_dirs(legacy_queries, legacy_queries)
        try:
            legacy_queries.rmdir()
        except OSError:
            pass


def existing_roots(run_dir):
    roots = []
    for name in ("real", "synthetic"):
        root = run_dir / name
        if root.exists():
            roots.append(root)
    return roots


def rebuild_manifests(run_dir):
    manifests_dir = run_dir / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)

    real_dir = run_dir / "real"
    synthetic_dir = run_dir / "synthetic"
    tasks = run_dir / "configs" / "ofat_tasks" / "query_graph_tasks.csv"

    subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "SSM-GraphGen" / "validate_graph_format.py"),
            "--input",
            str(real_dir),
            str(synthetic_dir),
            "--report",
            str(manifests_dir / "graph_validation_report.csv"),
        ],
        check=True,
    )
    subprocess.run(
        [
            sys.executable,
            str(CURRENT_DIR / "build_data_graph_manifest.py"),
            "--real-dir",
            str(real_dir),
            "--synthetic-dir",
            str(synthetic_dir),
            "--output",
            str(manifests_dir / "data_graph_manifest.csv"),
        ],
        check=True,
    )

    query_roots = existing_roots(run_dir)
    command = [
        sys.executable,
        str(CURRENT_DIR / "build_query_graph_manifest.py"),
        "--queries-dir",
    ]
    command.extend(str(root) for root in query_roots)
    command.extend(
        [
            "--tasks",
            str(tasks),
            "--output",
            str(manifests_dir / "query_graph_manifest.csv"),
        ]
    )
    subprocess.run(command, check=True)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "run",
        help="Run id under datasets/runs, such as 20260419_001233, or a full run directory.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace colliding files.")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without moving files.")
    parser.add_argument(
        "--skip-manifests",
        action="store_true",
        help="Do not rebuild validation/data/query manifest CSV files after packaging.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    run_dir = resolve_run_dir(args.run)
    if not run_dir.exists():
        raise FileNotFoundError("run directory does not exist: {}".format(run_dir))

    packages = package_real_graphs(
        run_dir,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
    )
    package_legacy_queries(
        run_dir,
        packages,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
    )

    if args.skip_manifests:
        return 0
    if args.dry_run:
        log("would rebuild manifests under {}".format(run_dir / "manifests"))
        return 0

    rebuild_manifests(run_dir)
    log("packaged run outputs under {}".format(run_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
