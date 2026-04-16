"""Utilities for query graph naming and parameter parsing."""

import re


def safe_token(value):
    """Return a filename-safe token."""
    token = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value).strip())
    token = token.strip("._-")
    return token or "value"


def build_query_filename(
    source_graph_id,
    vertices_num,
    avg_degree,
    missing_edge_threshold,
    query_index,
    generation_mode="ofat",
    ext=".graph",
):
    """Build a query graph filename containing source and parameter metadata."""
    if not ext.startswith("."):
        ext = "." + ext
    return (
        "query__source_{source}__mode_{mode}__vertices_{vertices}"
        "__avg_degree_{degree}__missing_{missing}__idx_{idx}{ext}"
    ).format(
        source=safe_token(source_graph_id),
        mode=safe_token(generation_mode),
        vertices=safe_token(vertices_num),
        degree=safe_token(avg_degree),
        missing=safe_token(missing_edge_threshold),
        idx=safe_token(query_index),
        ext=ext,
    )


def build_query_prefix(
    source_graph_id,
    task_id,
    vertices_num=None,
    avg_degree=None,
    missing_edge_threshold=None,
):
    """Build the prefix passed to the external query generator."""
    parts = [
        "query",
        "source_{}".format(safe_token(source_graph_id)),
        "mode_{}".format(safe_token(task_id)),
    ]
    if vertices_num is not None:
        parts.append("vertices_{}".format(safe_token(vertices_num)))
    if avg_degree is not None:
        parts.append("avg_degree_{}".format(safe_token(avg_degree)))
    if missing_edge_threshold is not None:
        parts.append("missing_{}".format(safe_token(missing_edge_threshold)))
    return "__".join(parts)


def parse_query_filename(filename):
    """Parse metadata from filenames produced by build_query_filename."""
    stem = str(filename).split("/")[-1].rsplit("\\", 1)[-1].rsplit(".", 1)[0]
    parts = stem.split("__")
    result = {"query_id": stem}

    for part in parts:
        if part.startswith("source_"):
            result["source_graph_id"] = part[len("source_") :]
        elif part.startswith("mode_"):
            result["generation_mode"] = part[len("mode_") :]
        elif part.startswith("vertices_"):
            result["vertices_num"] = part[len("vertices_") :]
        elif part.startswith("avg_degree_"):
            result["avg_degree"] = part[len("avg_degree_") :]
        elif part.startswith("missing_"):
            result["missing_edge_threshold"] = part[len("missing_") :]
        elif part.startswith("idx_"):
            result["query_index"] = part[len("idx_") :]

    return result
