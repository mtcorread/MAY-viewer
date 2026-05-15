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

## Frontend

A local-first browser app — React + MapLibre GL + PMTiles — that consumes
**only** the prep cache, never the `.h5`, through bounded reads. The UI is a
warm-paper, single-typeface (IBM Plex Sans) design with a shared header
(brand, file, Map/Inspect switch, world KPIs):

- **Map mode** — three panels: a left **Geography** panel (selected-ancestor
  path, child drill list, leaf-unit explainer, dataset level reference), the
  map, and a right **Stats** panel (hero population + per-demographic bars,
  schema-labelled). The map renders real geo-boundary polygons (offline
  PMTiles, one source-layer per level) as the click-to-drill target with a
  hexbin density fallback; a floating glass **Layers** card toggles
  overlay/basemap and a cursor tooltip previews a region's stats. Clicking a
  region — or returning from Inspect — recentres and fits the map to that
  unit's extent (from the aggregate `geo:bbox_*`/`geo:lon,lat` columns).
- **Inspect mode** — a four-column cascade (Categories → venues →
  sub-units → detail) with a path crumb trail, fed by a single O(1) Parquet
  **row-group** read (manifest `row_groups` index). The detail column shows
  a venue summary card + member table; selecting a person opens their
  attributes and social graph (in-unit friends clickable, cross-unit shown
  as IDs). People-partitioned leaf units surface a synthetic "People" list.

An optional online basemap is **opt-in only** via
`mayviewer serve … --basemap osm|carto-dark|carto-light|<xyz-url>`; with no
flag the viewer makes zero external requests. Every label is rendered from
`manifest.json` (`schema`/`geo` blocks and the self-describing aggregate
columns) — zero hardcoded domain terms. Fonts are self-hosted so the app
works fully offline.

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

- The hexbin density **fallback** PMTiles pyramid is sparse (tiles only at
  zooms 4/7/10/13), so when no boundary shapes are baked the map is pinned to
  one representative zoom and overzoomed — a single density resolution.
  Multi-resolution needs either a contiguous prep pyramid or one MapLibre
  source per discrete zoom. (With boundary shapes present the map drills and
  recentres per level normally.)
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
