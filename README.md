# SSM-DatasetGen

SSM-DatasetGen is used to generate data graph and query graph datasets for both subgraph similarity matching (SSM) and exact subgraph matching experiments.

Modules:
- SSM-GraphGen: real-graph preprocessing, synthetic graph generation, and graph format validation
- SSM-QueryGen: query graph generation based on data graphs
- SSM-Pipeline: OFAT configuration management, manifest construction, and workflow orchestration

Experiment strategy:
- Use only OFAT (One-Factor-At-a-Time)
- Generate one corresponding query graph set for each data graph
- Expand OFAT experiment configurations into task parameter files first; graph generation scripts consume only these task files

## Workflow

```bash
bash SSM-Pipeline/run_ofat_pipeline.sh
```

Each run writes outputs below a timestamped directory:
`datasets/runs/YYYYMMDD_HHMMSS/`. Raw inputs under `datasets/raw/` are
preserved.

Useful scope examples:

```bash
# Real graphs only: convert datasets/raw/real_graphs, validate them, and build queries.
bash SSM-Pipeline/run_ofat_pipeline.sh --scope real

# Synthetic graphs only.
bash SSM-Pipeline/run_ofat_pipeline.sh --scope synthetic

# Use a deterministic output folder name instead of the current timestamp.
bash SSM-Pipeline/run_ofat_pipeline.sh --scope real --run-id 20260419_001233
```

Generated files are grouped inside the run directory:

- `configs/ofat_tasks/`: expanded OFAT task files
- `real/`: converted real graphs, when `--scope real` or `--scope all`
- `synthetic/`: generated synthetic graphs, when `--scope synthetic` or `--scope all`
- `queries/`: generated query graphs for valid selected data graphs
- `manifests/`: validation reports and data/query manifests

The pipeline runs these steps:

1. Expand OFAT configs into reusable task parameter files.
2. Convert raw real graphs when the selected scope includes real graphs.
3. Generate synthetic data graphs when the selected scope includes synthetic graphs.
4. Validate selected standard graph files.
5. Build the selected data graph manifest.
6. Generate query graphs from `query_graph_tasks.csv` for every valid selected data graph manifest row.
7. Build the query graph manifest.

## Real Graph Conversion

Raw real graphs are read from `datasets/raw/real_graphs/` and converted into
standard `.txt` files. The pipeline writes them under the current run's
`real/` directory; the standalone converter defaults to `datasets/real/`.

The converter dispatches by file suffix:

- `.graph`, `.txt`, `.edges`, `.edgelist`, `.el`: standard `t/v/e` text or whitespace edge list
- `.csv`: comma-separated edge list, `source,target[,label]`
- `.tsv`: tab-separated edge list, `source<TAB>target[<TAB>label]`

Unknown suffixes fall back to the whitespace parser by default. Use
`--strict-suffix` to fail on unregistered suffixes.

To add a new real-graph format, register a parser in
`SSM-GraphGen/convert_real_graphs.py`:

```python
@register_parser(".myfmt")
def parse_my_format(path):
    return graph_id, vertex_labels, edges
```

## Graph Generation Tools

The low-level graph generators live under `tools/`. They write the project
standard `t/v/e` graph format.

### Synthetic Data Graphs

`tools/synthetic_graph_generator.py` generates one synthetic data graph from a
vertex count, target average degree, fixed R-MAT degree distribution, label
count, and label distribution.

Degree distribution:

- `R-MAT`: samples edges with an R-MAT initiator matrix. Defaults are `a=0.57`, `b=0.19`, `c=0.19`, `d=0.05`.

Supported vertex-label distributions:

- `uniform`: assigns labels uniformly from `0..label_count-1`.
- `Zipf`: assigns labels with a Zipf distribution. The default exponent is `1.2`.
- `degree-correlated`: sorts vertices by degree and assigns lower label ids to higher-degree vertices.

Example:

```bash
python3 tools/synthetic_graph_generator.py \
  --vertices 20000 \
  --avg-degree 12 \
  --label-count 200 \
  --degree-distribution R-MAT \
  --label-distribution uniform \
  --output datasets/synthetic/example/graph_g.txt
```

Useful optional parameters:

- `--seed`: make generation reproducible.
- `--overwrite`: replace an existing output file.
- `--zipf-exponent`: tune `Zipf` label skew.
- `--rmat-a`, `--rmat-b`, `--rmat-c`: tune the `R-MAT` initiator matrix.

### Query Graphs

`tools/query_graph_generator.py` generates query graphs from an existing data
graph. For each query graph, it samples a connected vertex set with a
Metropolis-Hastings random walk, then writes the data graph's induced subgraph
on those vertices. Because every query edge is copied from the source graph,
the sampled query graph has a corresponding match in the data graph. Failed
walks are retried.

Supported query-generation mode:

- Metropolis-Hastings random-walk sampling from the data graph.
- Induced-subgraph edge extraction from the sampled vertices.
- Query vertex count defaults to 10 and is capped at 30.

Example:

```bash
python3 tools/query_graph_generator.py \
  --data-graph datasets/synthetic/example/graph_g.txt \
  --vertices-num 10 \
  --num-per-setting 100 \
  --output-dir datasets/synthetic/example/query_graph/baseline
```

Useful optional parameters:

- `--seed`: make generation reproducible.
- `--overwrite`: replace existing query files.
- `--max-attempts`: maximum retry count per query graph.
- `--max-walk-steps`: maximum random-walk steps per retry.

### Graph Parameter Checks

`tools/check_graph_parameters.py` reads generated standard graph files and reports
vertices, edges, average degree, density, label count, degree range, isolated
vertices, connected components, top labels, validity, and expected-parameter
checks.

Example:

```bash
python3 tools/check_graph_parameters.py datasets/synthetic/example/query_graph/baseline \
  --expected-vertices 10 \
  --require-connected \
  --output datasets/synthetic/example/query_check.csv
```

You can also run the two decoupled stages separately:

```bash
python3 SSM-Pipeline/make_ofat_configs.py
python3 SSM-GraphGen/generate_synthetic_graphs.py --tasks configs/ofat_tasks/data_graph_tasks.csv
python3 SSM-QueryGen/generate_query_graphs.py --tasks configs/ofat_tasks/query_graph_tasks.csv
```

Add `--overwrite` to `generate_query_graphs.py` when replacing query files
created by an older generation method.

`make_ofat_configs.py` writes both summary task files and per-task JSON files:

- `configs/ofat_tasks/data_graph_tasks.csv`
- `configs/ofat_tasks/data_graph_tasks.json`
- `configs/ofat_tasks/data_graph/*.json`
- `configs/ofat_tasks/query_graph_tasks.csv`
- `configs/ofat_tasks/query_graph_tasks.json`
- `configs/ofat_tasks/query_graph/*.json`

The two lower-level generators are expected to be Python scripts:

- `tools/synthetic_graph_generator.py`
- `tools/query_graph_generator.py`

If either script is not present yet, the corresponding generation step is skipped.
