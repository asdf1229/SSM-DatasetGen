"""Wrapper around the external query_graph_generator.py script."""

import subprocess
import sys
from pathlib import Path


def avg_degree_to_density(vertices_num, avg_degree):
    """Convert average degree to undirected graph density."""
    vertices_num = int(vertices_num)
    avg_degree = float(avg_degree)
    if vertices_num <= 1:
        return 0.0
    density = avg_degree / float(vertices_num - 1)
    return max(0.0, min(1.0, density))


def build_query_generator_command(
    tool_path,
    data_graph_file,
    output_dir,
    output_prefix,
    vertices_num,
    num_per_setting,
    overwrite=False,
):
    """Build the standard command expected by the query generator script."""
    command = [
        sys.executable,
        str(tool_path),
        "--data-graph",
        str(data_graph_file),
        "--vertices-num",
        str(vertices_num),
        "--num-per-setting",
        str(num_per_setting),
        "--output-dir",
        str(output_dir),
        "--output-prefix",
        str(output_prefix),
    ]
    if overwrite:
        command.append("--overwrite")
    return command


def _snapshot_files(path):
    path = Path(path)
    if not path.exists():
        return set()
    return {child.resolve() for child in path.rglob("*") if child.is_file()}


def _expected_query_files(output_dir, num_per_setting):
    return [
        (Path(output_dir) / "{}.txt".format(query_index + 1)).resolve()
        for query_index in range(int(num_per_setting))
    ]


def call_query_generator(
    tool_path,
    data_graph_file,
    output_dir,
    output_prefix,
    vertices_num,
    num_per_setting,
    overwrite=False,
):
    """Call query_graph_generator.py and return files created or overwritten."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    before = _snapshot_files(output_dir)

    command = build_query_generator_command(
        tool_path=tool_path,
        data_graph_file=data_graph_file,
        output_dir=output_dir,
        output_prefix=output_prefix,
        vertices_num=vertices_num,
        num_per_setting=num_per_setting,
        overwrite=overwrite,
    )
    subprocess.run(command, check=True)

    after = _snapshot_files(output_dir)
    if overwrite:
        return sorted(
            path for path in _expected_query_files(output_dir, num_per_setting) if path in after
        )
    return sorted(after - before)
