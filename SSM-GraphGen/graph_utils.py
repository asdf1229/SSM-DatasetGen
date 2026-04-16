"""Utilities for the standard SSM graph file format."""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class StandardGraph:
    """In-memory representation of one standard-format graph."""

    graph_id: str
    vertices: dict
    edges: list


def read_standard_graph(path):
    """Read the first graph from a standard graph file."""
    path = Path(path)
    graph_id = path.stem
    vertices = {}
    edges = []
    seen_header = False

    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            tokens = line.split()
            record_type = tokens[0]

            if record_type == "t":
                if seen_header:
                    break
                seen_header = True
                if len(tokens) >= 3 and tokens[1] == "#":
                    graph_id = tokens[2]
                elif len(tokens) >= 2:
                    graph_id = tokens[-1]
                continue

            if record_type == "v":
                if len(tokens) < 3:
                    raise ValueError("invalid vertex line: {}".format(line))
                vertices[int(tokens[1])] = tokens[2]
                continue

            if record_type == "e":
                if len(tokens) < 3:
                    raise ValueError("invalid edge line: {}".format(line))
                label = tokens[3] if len(tokens) > 3 else None
                edges.append((int(tokens[1]), int(tokens[2]), label))
                continue

            raise ValueError("unknown graph line: {}".format(line))

    return StandardGraph(graph_id=graph_id, vertices=vertices, edges=edges)


def canonicalize_undirected_edges(edges):
    """Remove self-loops and duplicate undirected edges, preserving the first label."""
    canonical = {}
    for edge in edges:
        if len(edge) == 2:
            u, v = edge
            label = None
        else:
            u, v, label = edge
        u = int(u)
        v = int(v)
        if u == v:
            continue
        a, b = sorted((u, v))
        canonical.setdefault((a, b), label)

    return [(u, v, label) for (u, v), label in sorted(canonical.items())]


def write_standard_graph(path, graph_id, vertices, edges):
    """Write one graph in the standard t/v/e format."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    canonical_edges = canonicalize_undirected_edges(edges)

    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write("t # {}\n".format(graph_id))
        for vertex_id, label in sorted(vertices.items()):
            handle.write("v {} {}\n".format(int(vertex_id), label))
        for u, v, label in canonical_edges:
            if label is None:
                handle.write("e {} {}\n".format(u, v))
            else:
                handle.write("e {} {} {}\n".format(u, v, label))


def compute_graph_stats(graph):
    """Compute basic graph statistics for a StandardGraph."""
    vertices_count = len(graph.vertices)
    canonical_edges = canonicalize_undirected_edges(graph.edges)
    edges_count = len(canonical_edges)
    labels = set(graph.vertices.values())
    avg_degree = 0.0
    if vertices_count:
        avg_degree = (2.0 * edges_count) / float(vertices_count)

    return {
        "vertices": vertices_count,
        "edges": edges_count,
        "avg_degree": round(avg_degree, 6),
        "label_count": len(labels),
    }


def iter_graph_files(path):
    """Yield candidate graph files below a file or directory path."""
    path = Path(path)
    if not path.exists():
        return
    if path.is_file():
        yield path
        return
    for child in sorted(path.rglob("*")):
        if not child.is_file():
            continue
        if child.name.startswith("."):
            continue
        if child.suffix.lower() in (".csv", ".json", ".yaml", ".yml", ".md"):
            continue
        yield child
