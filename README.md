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

The pipeline runs these steps:

1. Expand OFAT configs into reusable task parameter files under `configs/ofat_tasks/`.
2. Convert raw real graphs from `datasets/raw/real_graphs/` into standard graph files under `datasets/real/`.
3. Generate synthetic data graphs from `configs/ofat_tasks/data_graph_tasks.csv`.
4. Validate standard graph files under `datasets/real/` and `datasets/synthetic/`.
5. Build `datasets/manifests/data_graph_manifest.csv`.
6. Generate query graphs from `configs/ofat_tasks/query_graph_tasks.csv` for every valid data graph manifest row.
7. Build `datasets/manifests/query_graph_manifest.csv`.

## Real Graph Conversion

Raw real graphs are read from `datasets/raw/real_graphs/` and converted into
standard `.graph` files under `datasets/real/`.

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
vertex count, target average degree, degree distribution, label count, and label
distribution.

Supported degree distributions:

- `ER`: uniformly samples undirected edges until the exact target edge count is reached.
- `power-law`: uses preferential attachment and preferential edge fill-in to produce a skewed degree profile.
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
  --degree-distribution ER \
  --label-distribution uniform \
  --output datasets/synthetic/example.graph
```

Useful optional parameters:

- `--seed`: make generation reproducible.
- `--overwrite`: replace an existing output file.
- `--zipf-exponent`: tune `Zipf` label skew.
- `--rmat-a`, `--rmat-b`, `--rmat-c`: tune the `R-MAT` initiator matrix.

### Query Graphs

`tools/query_graph_generator.py` generates query graphs from an existing data
graph. For each query graph, it first samples a connected vertex set through a
random walk tree on the data graph, then adds random edges among the sampled
vertices until the requested average degree or density is reached. Failed random
walks are retried.

Supported query-generation mode:

- Random-walk tree sampling from the data graph.
- Random edge fill-in among sampled query vertices.
- Optional strict mode with `--strict-data-edges`, where added non-tree edges
  must also exist in the original data graph.

Example:

```bash
python3 tools/query_graph_generator.py \
  --data-graph datasets/synthetic/example.graph \
  --vertices-num 40 \
  --avg-degree 4 \
  --density 0.102564 \
  --missing-edge-threshold 3 \
  --num-per-setting 100 \
  --output-dir datasets/queries/example/baseline \
  --output-prefix query__source_example__mode_baseline
```

Useful optional parameters:

- `--seed`: make generation reproducible.
- `--overwrite`: replace existing query files.
- `--max-attempts`: maximum retry count per query graph.
- `--max-walk-steps`: maximum random-walk steps per retry.
- `--strict-data-edges`: require added edges to exist in the source data graph.

### Graph Parameter Checks

`tools/check_graph_parameters.py` reads generated `.graph` files and reports
vertices, edges, average degree, density, label count, degree range, isolated
vertices, connected components, top labels, validity, and expected-parameter
checks.

Example:

```bash
python3 tools/check_graph_parameters.py datasets/queries/example/baseline \
  --expected-vertices 40 \
  --expected-edges 80 \
  --expected-avg-degree 4 \
  --require-connected \
  --output datasets/queries/example/query_check.csv
```

You can also run the two decoupled stages separately:

```bash
python3 SSM-Pipeline/make_ofat_configs.py
python3 SSM-GraphGen/generate_synthetic_graphs.py --tasks configs/ofat_tasks/data_graph_tasks.csv
python3 SSM-QueryGen/generate_query_graphs.py --tasks configs/ofat_tasks/query_graph_tasks.csv
```

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
