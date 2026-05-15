# MAY-viewer

Local-first visualisation tool for [MAY](../MAY) synthetic-world HDF5 files.
Run it against **your own** `world_state.h5` — from 1K to ~60M agents — and
explore the world: geography drill-down, households and their members, venues
(schools/classrooms/… *whatever your world defines*), and stats. No upload, no
server to host.

**Schema-driven by design.** Nothing domain-specific is hardcoded. Geography
level names, venue types, subset roles and property names are read from each
world's own `metadata/registries` and group layout (`mayviewer/schema.py`), so
an England world and a New Zealand world — or a world with no schools — all
just work. Geographic boundary shapefiles are an optional, country-agnostic
overlay matched by geo code; the default render needs none.

## Status

| Phase | What | State |
|---|---|---|
| 1 | Synthetic scale generator (tile a real world to N agents) | ✅ done |
| 2 | Streaming prep pipeline (aggregates + tiles + Arrow shards) | next |
| 3 | Frontend (map drill-down + entity inspector) | — |
| 4 | 60M scale + static-deploy validation | — |

## Usage

```bash
pip install -e .

# Inspect what a world contains (no hardcoded terms — all discovered)
mayviewer describe /path/to/world_state.h5

# Generate a synthetic scaled world to stress-test at 60M
mayviewer synth /path/to/world_state.h5 --people 60M --out world_60m.h5
```

`synth` tiles the source into K disjoint, ID-rebased replicas: every dataset,
partition index and registry stays schema-valid, replicas are laid on a map
grid so they render as distinct clusters, and `population/properties/friendships`
(a JSON string per person in MAY) is upgraded to a typed CSR triple under
`population/friendships/`. Peak memory ≈ one source world regardless of target
size.

## Dev

```bash
python -m pytest -q tests/
```
