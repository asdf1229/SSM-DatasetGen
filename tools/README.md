# External generator scripts

This directory contains the two lower-level generator scripts:

- `synthetic_graph_generator.py`
- `query_graph_generator.py`

Both scripts are included. The workflow scripts look for these `.py` files and
skip the generation step when one is missing.

Expected synthetic generator command:

```bash
python tools/synthetic_graph_generator.py \
  --vertices 20000 \
  --avg-degree 12 \
  --label-count 200 \
  --degree-distribution ER \
  --label-distribution uniform \
  --output datasets/synthetic/output/graph_g.txt
```

Expected query generator command:

```bash
python tools/query_graph_generator.py \
  --data-graph datasets/synthetic/output/graph_g.txt \
  --vertices-num 40 \
  --avg-degree 4 \
  --density 0.102564 \
  --missing-edge-threshold 3 \
  --num-per-setting 100 \
  --output-dir datasets/synthetic/output/query_graph/baseline
```
