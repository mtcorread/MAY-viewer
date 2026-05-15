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
| 2 | Streaming prep pipeline (aggregates + tiles + Arrow shards) | ✅ done |
| 3 | Frontend (map drill-down + entity inspector) | ✅ done |
| 4 | 60M scale + static-deploy validation | — |

## Usage

```bash
pip install -e .

# Inspect what a world contains (no hardcoded terms — all discovered)
mayviewer describe /path/to/world_state.h5

# Generate a synthetic scaled world to stress-test at 60M
mayviewer synth /path/to/world_state.h5 --people 60M --out world_60m.h5

# Build the cached viewer artifacts (idempotent; memory-flat, scales to 60M)
mayviewer prep /path/to/world_state.h5

# Open the browser viewer (builds nothing at run time — see Frontend below)
mayviewer serve /path/to/world_state.h5
```

## Frontend (Phase 3)

A local-first browser app — React + MapLibre GL + PMTiles — that consumes
**only** the prep cache, never the `.h5`, through bounded reads:

- **Map mode** — PMTiles hexbin density backdrop (offline, no external
  basemap) with a tree-driven geography drill-down (breadcrumb + child list
  built from the aggregate parquets' `parent_id`/`level`) and a live,
  schema-labelled stats panel for the selected unit.
- **Inspect mode** — pick a geo unit → its venues and people via a single
  O(1) Parquet **row-group** read (manifest `row_groups` index); venue
  attributes + member roster grouped by subset; person attributes + social
  graph (friends in the loaded unit are clickable, cross-unit friends shown
  as IDs).

Every label is rendered from `manifest.json` (`schema`/`geo` blocks and the
self-describing aggregate columns) — zero hardcoded domain terms. Fonts are
self-hosted so the app works fully offline.

`mayviewer serve` is the entire backend: a dependency-free, Range-capable
static server. The exact same cache directory can be dropped onto any static
CDN unchanged (no bespoke API). The frontend is a build artifact and is not
committed — build it once:

```bash
cd frontend && npm install && npm run build   # → mayviewer/web/dist/
```

For UI iteration, run `mayviewer serve <world>` and `npm run dev` in
`frontend/` (Vite proxies `/cache` to the running server).

### Known v1 limitations / Phase-4 caveats

- The map is a density backdrop; it does **not** recentre on the selected
  unit — the cache carries no per-unit centroid (aggregates have no lat/lon).
  A one-line prep addendum emitting unit lon/lat into the aggregate parquets
  would later enable map-follow with no frontend rework.
- The hexbin PMTiles pyramid is sparse (tiles only at zooms 4/7/10/13), so
  the map is pinned to one representative zoom and overzoomed — a single
  density resolution. Multi-resolution needs either a contiguous prep
  pyramid or one MapLibre source per discrete zoom.
- Cross-unit friend *identity* resolution is deferred (count + in-unit links
  only) — needs a `person_id → row-group` index added in prep.
- One row group per geo unit makes the Parquet footer grow with unit count:
  ~43 MB at 34k units (10M synth). It is read once and cached per file
  handle, but at ~200k England OAs this is hundreds of MB on first read —
  revisit at 60M (page index / sidecar offsets / shard split).

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
