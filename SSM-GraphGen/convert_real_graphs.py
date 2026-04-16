"""Convert raw real graph files into the standard SSM graph format."""

import argparse
import csv
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "SSM-Pipeline"))

from graph_utils import canonicalize_undirected_edges, write_standard_graph
from pipeline_utils import log

DEFAULT_LABEL = "0"
COMMENT_PREFIXES = ("#", "%", "//")
SOURCE_HEADER_NAMES = ("source", "src", "from", "u", "node1", "source_id")
TARGET_HEADER_NAMES = ("target", "dst", "to", "v", "node2", "target_id")
PARSER_BY_SUFFIX = {}


def register_parser(*suffixes):
    """Register a parser function for one or more file suffixes."""
    normalized_suffixes = [normalize_suffix(suffix) for suffix in suffixes]

    def decorator(func):
        for suffix in normalized_suffixes:
            PARSER_BY_SUFFIX[suffix] = func
        return func

    return decorator


def normalize_suffix(suffix):
    """Return a normalized suffix, including the leading dot."""
    suffix = str(suffix).strip().lower()
    if not suffix:
        return ""
    if not suffix.startswith("."):
        suffix = "." + suffix
    return suffix


def parser_for_path(path):
    """Return the parser selected by file suffix."""
    suffix = normalize_suffix(Path(path).suffix)
    return PARSER_BY_SUFFIX.get(suffix, parse_whitespace_graph)


def _sort_key(value):
    try:
        return (0, int(value))
    except ValueError:
        return (1, str(value))


def parse_whitespace_graph(path):
    """Load standard t/v/e lines or a whitespace-separated edge list."""
    path = Path(path)
    graph_id = path.stem
    vertex_labels = {}
    edges = []

    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith(COMMENT_PREFIXES):
                continue

            tokens = line.split()
            record_type = tokens[0]

            if record_type == "t":
                if len(tokens) >= 3 and tokens[1] == "#":
                    graph_id = tokens[2]
                elif len(tokens) >= 2:
                    graph_id = tokens[-1]
                continue

            if record_type == "v":
                if len(tokens) < 3:
                    raise ValueError("invalid vertex line in {}: {}".format(path, line))
                vertex_labels[tokens[1]] = tokens[2]
                continue

            if record_type == "e":
                if len(tokens) < 3:
                    raise ValueError("invalid edge line in {}: {}".format(path, line))
                label = tokens[3] if len(tokens) > 3 else None
                edges.append((tokens[1], tokens[2], label))
                vertex_labels.setdefault(tokens[1], DEFAULT_LABEL)
                vertex_labels.setdefault(tokens[2], DEFAULT_LABEL)
                continue

            if len(tokens) >= 2:
                label = tokens[2] if len(tokens) > 2 else None
                edges.append((tokens[0], tokens[1], label))
                vertex_labels.setdefault(tokens[0], DEFAULT_LABEL)
                vertex_labels.setdefault(tokens[1], DEFAULT_LABEL)
                continue

            raise ValueError("cannot parse line in {}: {}".format(path, line))

    return graph_id, vertex_labels, edges


def parse_delimited_edge_list(path, delimiter):
    """Load a delimited edge list with optional edge labels."""
    path = Path(path)
    graph_id = path.stem
    vertex_labels = {}
    edges = []

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter=delimiter)
        for row in reader:
            tokens = [token.strip() for token in row]
            tokens = [token for token in tokens if token != ""]
            if not tokens:
                continue
            if tokens[0].startswith(COMMENT_PREFIXES):
                continue
            if len(tokens) < 2:
                raise ValueError("cannot parse row in {}: {}".format(path, row))
            if tokens[0].lower() in SOURCE_HEADER_NAMES and tokens[1].lower() in TARGET_HEADER_NAMES:
                continue

            label = tokens[2] if len(tokens) > 2 else None
            edges.append((tokens[0], tokens[1], label))
            vertex_labels.setdefault(tokens[0], DEFAULT_LABEL)
            vertex_labels.setdefault(tokens[1], DEFAULT_LABEL)

    return graph_id, vertex_labels, edges


@register_parser(".graph", ".txt", ".edges", ".edgelist", ".el")
def parse_default_text_graph(path):
    """Parse project-standard graph text or whitespace edge-list text."""
    return parse_whitespace_graph(path)


@register_parser(".csv")
def parse_csv_edge_list(path):
    """Parse a comma-separated edge list: source,target[,label]."""
    return parse_delimited_edge_list(path, delimiter=",")


@register_parser(".tsv")
def parse_tsv_edge_list(path):
    """Parse a tab-separated edge list: source<TAB>target[<TAB>label]."""
    return parse_delimited_edge_list(path, delimiter="\t")


def load_raw_graph(path):
    """Load a raw graph using the parser selected by file suffix."""
    parser = parser_for_path(path)
    return parser(path)


def normalize_vertex_ids(vertex_labels, edges):
    """Map arbitrary raw vertex ids to contiguous integer ids."""
    raw_ids = set(vertex_labels.keys())
    for u, v, _label in edges:
        raw_ids.add(str(u))
        raw_ids.add(str(v))

    ordered_ids = sorted(raw_ids, key=_sort_key)
    id_map = {raw_id: idx for idx, raw_id in enumerate(ordered_ids)}
    vertices = {
        id_map[raw_id]: vertex_labels.get(raw_id, DEFAULT_LABEL)
        for raw_id in ordered_ids
    }
    normalized_edges = [
        (id_map[str(u)], id_map[str(v)], label)
        for u, v, label in edges
    ]

    return vertices, canonicalize_undirected_edges(normalized_edges)


def convert_file(input_file, output_dir, input_root, overwrite=False):
    """Convert one raw graph file and return the output path."""
    graph_id, vertex_labels, edges = load_raw_graph(input_file)
    vertices, normalized_edges = normalize_vertex_ids(vertex_labels, edges)

    relative = Path(input_file).relative_to(input_root)
    output_file = Path(output_dir) / relative.with_suffix(".txt")
    if output_file.exists() and not overwrite:
        log("skip existing {}".format(output_file))
        return output_file

    write_standard_graph(output_file, graph_id, vertices, normalized_edges)
    return output_file


def iter_raw_files(input_path):
    """Yield raw input files below a path."""
    input_path = Path(input_path)
    if not input_path.exists():
        return
    if input_path.is_file():
        yield input_path
        return
    for child in sorted(input_path.rglob("*")):
        if child.is_file() and not child.name.startswith("."):
            yield child


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default=str(PROJECT_ROOT / "datasets" / "raw" / "real_graphs"),
        help="Raw real graph file or directory.",
    )
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "datasets" / "real"),
        help="Output directory for standard real graphs.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite outputs.")
    parser.add_argument(
        "--strict-suffix",
        action="store_true",
        help="Fail when no parser is registered for a file suffix.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output)
    input_root = input_path if input_path.is_dir() else input_path.parent

    files = list(iter_raw_files(input_path))
    if not files:
        log("no raw real graph files found under {}".format(input_path))
        return 0

    if args.strict_suffix:
        unsupported = [
            path
            for path in files
            if normalize_suffix(path.suffix) not in PARSER_BY_SUFFIX
        ]
        if unsupported:
            for path in unsupported:
                log("unsupported raw graph suffix: {}".format(path))
            return 1

    converted = []
    for input_file in files:
        converted.append(
            convert_file(input_file, output_dir, input_root, overwrite=args.overwrite)
        )

    log("converted {} real graph file(s)".format(len(converted)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
