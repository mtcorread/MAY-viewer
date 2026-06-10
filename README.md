# MAY-viewer

> Explore [MAY](https://github.com/mtcorread/MAY) synthetic worlds in your browser, from a thousand agents to tens of millions.

Point MAY-viewer at a `world_state.h5` and explore it interactively: geography,
households and their members, venues, and demographics. It's **local-first** (your
data never leaves your machine), **fast at scale** (built to handle millions of agents),
and **schema-driven**: level names, venue types, and attribute names are read from
each world's own metadata, so worlds with different structures all just work.

<img width="500" alt="Map view" src="https://github.com/user-attachments/assets/8f24379a-bd69-4b01-809c-ebab5dc630a6" />
<img width="500" alt="Inspect view" src="https://github.com/user-attachments/assets/78ea9f4e-372d-40d4-9b34-224467341b06" />

## Features

- **Map mode**: drill from coarse to fine geography on a real map of boundaries
  (or a density hexbin view), with live population and demographic stats for the
  selected region.
- **Inspect mode**: cascade from a venue category → venue → sub-unit → its
  members, then open any person's attributes and social graph.
- **Scales**: precomputed map tiles and per-region aggregates keep huge worlds
  responsive.
- **Optional overlays**: bring your own boundary shapefiles, human-readable
  attribute labels, and an online basemap (OpenStreetMap, Carto, any XYZ tiles).
- **Transit lines**: draw the world's train/tube routes on the map, click a line
  to see who rides it, follow a rider's multi-leg journey (with start/end markers),
  and compare lines for shared ridership.

## Quick start

You'll need **Python ≥ 3.10** and **Node.js ≥ 18**.

```bash
# 1. Get the code
git clone https://github.com/mtcorread/MAY-viewer.git
cd MAY-viewer

# 2. Install the tool (in an isolated environment)
python3 -m venv .venv && source .venv/bin/activate
pip install -e .

# 3. Build the browser app (required, once, plus after pulling new code)
cd frontend && npm install && npm run build && cd ..
```

Then point it at a world file:

```bash
mayviewer prep  /path/to/world_state.h5 --no-drilldown   # build map + stats (seconds)
mayviewer serve /path/to/world_state.h5                  # opens http://127.0.0.1:8000/
```

`serve` opens your browser automatically. Press `Ctrl`+`C` to stop it.

**Coming back later** is just two lines from the project folder, no reinstall:

```bash
source .venv/bin/activate                 # Windows: .\.venv\Scripts\Activate.ps1
mayviewer serve /path/to/world_state.h5
```

## Commands

| Command | What it does |
| --- | --- |
| `mayviewer describe <world.h5>` | Print the world's discovered schema (geo levels, venue types, attribute names). |
| `mayviewer prep <world.h5>` | Build the cached viewer artifacts. `--no-drilldown` for a fast, tiny build; `--force` to rebuild; `--boundary-config` for real shapes; `--transit-geometry` + `--mgu-coords` for the transit layer. |
| `mayviewer serve <world.h5>` | Serve the viewer in your browser. `--host`/`--port`/`--no-open`/`--basemap` to configure. |
| `mayviewer match <world.h5> <shapes>` | Diagnose how boundary shapes line up with the world's geo units (read-only). |

## Two ways to serve

`prep` always builds the **map** and **aggregates** (cheap, needed for the map and
stat panels). The expensive part is the per-region **drill-down** (people, venues,
members). You choose how to serve it:

- **Local inspection** (`prep --no-drilldown`): `serve` reads each region straight
  from the `.h5` on click. Build takes seconds, the cache stays tiny, and "create a
  world → look at it immediately" is instant. The `.h5` must stay where `prep`
  recorded it. *Best for development and large worlds.*

- **Static hosting** (`prep` with no flag): builds a full, self-contained cache
  (Parquet shards + tiles + manifest) that the browser reads directly by HTTP range
  request. No `.h5` needed at serve time, so you can drop the cache folder behind any
  range-capable static host or CDN. *Best when you only have static web space.* Note
  the full cache grows with the world (whole-England is multiple GB and a longer build).

`prep` is memory-flat and idempotent: re-running on an unchanged world is a no-op.
Use `--force` to rebuild (also needed when switching between the two modes).

## Configuration

### Geographic boundaries: render real coastlines and admin areas

The default `prep` builds a density-only render (hex tiles, no polygons). To overlay
real shapes, supply one GeoJSON/shapefile per geo level plus a `boundary_config.json`
that tells `prep` how to join each file to the world's geo codes:

```json
{
  "levels": {
    "<LEVEL_NAME>": {
      "file":     "<path or filename of a GeoJSON/shapefile>",
      "prop":     "<feature property to match on>",
      "strategy": "code"
    }
  }
}
```

- `<LEVEL_NAME>` is a level reported by `mayviewer describe` (world-specific).
- `file` is resolved relative to the config file's directory, or absolute.
- `prop` is the feature property holding the join key (e.g. `LAD21NM`, `OA21CD`).
- `strategy: "code"` matches the geo code exactly (casefold); `"name"` is loose,
  punctuation-insensitive. Omitted levels simply get no polygons.

Sanity-check join rates before a full build, then bake them in:

```bash
mayviewer match /path/to/world_state.h5 /path/to/boundaries.geojson   # per-level matched/total/rate
mayviewer prep  /path/to/world_state.h5 --boundary-config /path/to/boundary_config.json
```

Geo extras need `pip install -e ".[geo]"`. Changing the config triggers a rebuild
automatically. For England/Wales, the ONS Open Geography Portal publishes matching
files; baking a national OA-level file can take 10+ minutes.

### Transit lines: draw train/tube routes and explore who rides them

Worlds built with MAY's route distributor contain transit line venues (which lines
exist, who rides them, per-leg timings) — but not the route *geometry*. Supply the
two CSVs the world was built from and `prep` bakes a transit layer:

```bash
mayviewer prep /path/to/world_state.h5 \
    --transit-geometry /path/to/line_stops.csv \
    --mgu-coords      /path/to/coord_mgu.csv
```

- `line_stops.csv` — each line's ordered stops (`line_id, position, node_mgu, name, …`).
  In the MAY repo: `dev-scripts/transport/line_stops.csv`.
- `coord_mgu.csv` — geo-unit centroids (`MGU, latitude, longitude`) used to place
  each stop. In the MAY repo: `data/geography/coord_mgu.csv`.
- The flags must be given together; changing either CSV triggers a rebuild
  automatically. Worlds with no train/tube venues skip the layer with a warning.
- Buses are excluded by design: `bus_pool_*` venues are a single pooled hop with
  no route geometry.

A **Transit** chip then appears in the map's Layers card. Click a line to list its
riders; pick a rider to see their multi-leg journey highlighted on the map, with the
legs and timings in the panel; pin two or more lines to see the riders they share.

Worlds whose route distributor records journey origin/destination (per-leg
`origin/dest/board/alight` geo units in `membership_metadata`) additionally get
**start/end markers** on the map — a hollow ring where the journey begins, a filled
disc where it ends, dots at interchanges — and the endpoint unit names in the panel.
The unit centroids come from the boundary overlay, so bake it too (see above) to see
the markers. Older worlds without those fields simply omit them.

### Display labels: show human-readable names for attribute codes

Worlds often store attributes as short codes (`W`, `neuro`, …). Create a `labels.json`
next to the world's `manifest.json` (inside `.mayviewer_cache/<world_stem>/`) to map
them to readable labels:

```json
{
  "ethnicity": { "A": "Asian", "B": "Black", "W": "White" },
  "sex": { "male": "Male", "female": "Female" },
  "comorbidities": { "neuro": "Neurological", "cardio": "Cardiovascular" }
}
```

- Field names match the parquet column names (run `mayviewer describe` to see them).
- List-valued fields (e.g. `comorbidities`) are looked up element by element.
- Anything not listed renders as its raw code.
- Read on page load: edit and refresh the browser, no re-`prep` needed.
- Add a top-level `venues` map to relabel venue type names too.

### Basemap: show OpenStreetMap or other tiles behind your data

The viewer is offline by default. Opt in to an online raster basemap with `--basemap`:

```bash
mayviewer serve /path/to/world_state.h5 --basemap osm
```

Accepts `osm`, `carto-light`, `carto-dark`, or any XYZ template URL
(`https://tile.example.com/{z}/{x}/{y}.png`). A Layers card in the map lets you
toggle it off at runtime.

## Development

```bash
# Run the test suite
python -m pytest -q tests/

# Frontend hot-reload (no rebuild needed, but bypasses serve's cache layer)
cd frontend && npm run dev
```

The frontend is a build artifact: `mayviewer serve` ships the compiled bundle from
`mayviewer/web/dist`, so edits under `frontend/src/` only appear after
`npm run build`. After a production build, hard-refresh the browser
(`Cmd`/`Ctrl`+`Shift`+`R`) to bypass cached assets.
