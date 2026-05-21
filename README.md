# MAY-viewer

A visualisation tool for [MAY](https://github.com/mtcorread/MAY)
synthetic-world files. Point it at a `world_state.h5` — from a thousand to
tens of millions of agents — and explore the world in your browser:
geography, households and their members, venues, and demographics.

**Schema-driven by design.** Geography level names, venue types, subset
roles and property names are read from each world's own metadata, so worlds
with different structures all just work. Geographic boundary shapefiles are
an optional overlay matched by geo code.

## Usage

```bash
pip install -e .

# See what a world contains
mayviewer describe /path/to/world_state.h5

# Build the viewer's cached artifacts
mayviewer prep /path/to/world_state.h5

# Open the browser viewer (serves what prep built)
mayviewer serve /path/to/world_state.h5
```

### Adding geographic boundaries

The default `prep` builds a density-only render (hex tiles, no polygons).
To see real coastlines and admin areas, supply your own boundary
shapefiles — one GeoJSON per geo level — and point `prep` at a JSON
config that tells the prepper how to join each file to the world's geo
codes.

You bring:

1. A folder of GeoJSON files (any location, any CRS — pyproj reprojects
   to EPSG:4326). For England/Wales the ONS Open Geography Portal
   publishes matching files for Regions, LADs, MSOAs and OAs.
2. A `boundary_config.json` somewhere on disk with one entry per geo
   level you want overlaid. Shape:

   ```json
   {
     "levels": {
       "<LEVEL_NAME>": {
         "file":     "<path or filename of a GeoJSON>",
         "prop":     "<feature property to match on>",
         "strategy": "code" | "name"
       }
     }
   }
   ```

   - `<LEVEL_NAME>` is one of the levels reported by
     `mayviewer describe <world.h5>` (e.g. `XLGU`, `LGU`, `MGU`, `SGU`
     in an ONS-style world — names are world-specific).
   - `file` is resolved relative to the config file's directory, or
     absolute.
   - `prop` is the GeoJSON feature property holding the join key (e.g.
     `LAD21NM`, `OA21CD`).
   - `strategy: "code"` matches the world's geo code to `prop` exactly
     (casefold); `"name"` is loose, punctuation-insensitive.
   - Levels you omit get no polygons — the world still renders, just
     without shapes at that level.

Before baking, sanity-check the join rates:

```bash
mayviewer match /path/to/world_state.h5 /path/to/some_boundaries.geojson
```

`match` reports per-level matched / total / rate, so you can pick the
right `prop` and `strategy` before committing to a full prep. The geo
extras need pyproj + shapely: `pip install -e ".[geo]"`.

Then rebuild the cache with the config:

```bash
mayviewer prep /path/to/world_state.h5 \
  --boundary-config /path/to/boundary_config.json
```

Switching from no-config to with-config (or changing the file) flips the
cache's `boundary_fingerprint` and triggers a rebuild automatically;
pass `--force` only if you've edited boundary code without changing the
config. Bake time is dominated by the finest, largest file — a national
OA-level file can take 10+ minutes.

### Custom display labels for attribute codes

Worlds typically store demographic attributes as short codes (`W`, `A`,
`neuro`, `heterosexual`, …). The viewer can show human-readable labels
instead, driven by a small JSON file you maintain. Labels are optional —
if the file is missing the UI just shows the raw codes.

Create `labels.json` next to the world's `manifest.json`, inside the
cache directory:

```
<world.h5 parent>/.mayviewer_cache/<world_stem>/
├── manifest.json        ← produced by `prep`
├── labels.json          ← YOU create this by hand
├── aggregates/
└── …
```

Shape — `field name → raw value → display label`:

```json
{
  "ethnicity": {
    "A": "Asian",
    "B": "Black",
    "W": "White",
    "M": "Mixed",
    "O": "Other"
  },
  "sex": { "male": "Male", "female": "Female" },
  "comorbidities": {
    "neuro": "Neurological",
    "cardio": "Cardiovascular",
    "resp": "Respiratory"
  },
  "sexual_orientation": {
    "heterosexual": "Straight",
    "homosexual": "Gay/Lesbian"
  }
}
```

Notes:

- Field names match the parquet column names (lowercase, as shown in the
  Inspector). Run `mayviewer describe <world.h5>` to see them.
- For list-valued fields (e.g. `comorbidities`), each element is looked
  up individually and the labels are joined together.
- Any field or value that isn't in `labels.json` just renders as before.
- The file is read on page load. Edit it, refresh the browser — no
  `prep` rerun, no rebuild. `prep` never touches sibling files in the
  cache directory, so it survives re-prepping.

Labels apply to person detail attributes in the Inspector and to the
demographic group bars in the Map mode's Stats panel. Add a top-level
`venues` map to also relabel venue type names in the Stats panel's
Venues table.

### Showing a basemap (OpenStreetMap, etc.)

The viewer is offline by default — no external tiles are fetched. Pass
`--basemap` to `serve` to opt in to an online raster basemap behind your
data:

```bash
mayviewer serve /path/to/world_state.h5 --basemap osm
```

Accepted values:

- `osm` — OpenStreetMap standard tiles
- `carto-light`, `carto-dark` — Carto's positron / dark-matter
- any XYZ template URL, e.g. `https://tile.example.com/{z}/{x}/{y}.png`

Once enabled, a Layers card in the map gives you a "No basemap" toggle
to turn it off at runtime.

### See your world on the map, with shapes and OSM

Putting it together for a fresh run:

```bash
mayviewer prep /path/to/world_state.h5 \
  --boundary-config /path/to/boundary_config.json
mayviewer serve /path/to/world_state.h5 --basemap osm
```

If polygons don't appear, the cache was probably prepped without
`--boundary-config`; check the manifest in
`.mayviewer_cache/<stem>/manifest.json` — `artifacts.boundaries` should
be present and `source.boundary_fingerprint` non-null. Re-run `prep`
with the config to bake them in.

## The viewer

A warm, single-typeface browser app with a shared header (brand, current
file, a Map/Inspect switch, and world totals).

- **Map mode** — three panels. On the left, a Geography panel with the
  selected unit's path, its child units to drill into, and a reference of
  the dataset's levels. In the centre, a map of real geographic boundaries
  you click to drill from coarse to fine, with a density view as an
  alternative. On the right, a Stats panel: total population plus
  demographic breakdowns for the selected unit. A floating Layers card
  switches the overlay and basemap, and hovering a region previews its
  stats. Selecting a region recentres the map on it.
<p align="center">
  <img width="500" height="280" alt="map_view" src="https://github.com/user-attachments/assets/1ecac625-3271-4ee3-bba1-eb89a97eeb71" />
</p>

- **Inspect mode** — a four-column cascade: pick a venue category, then a
  venue, then a sub-unit, then see its detail. The detail view summarises
  the venue and lists its members; selecting a person opens their
  attributes and their social graph, with friends in view clickable.
<p align="center">
  <img width="500" height="280" alt="inspect_view" src="https://github.com/user-attachments/assets/47bef233-90e4-4c2d-82da-e5cfe7ef0df6" />
</p>


Every label comes from the world's own metadata, so there are no
domain-specific terms baked in. See **Adding geographic boundaries** and
**Showing a basemap** above for how to render real shapes and an online
basemap.

The frontend is a build artifact — build it once:

```bash
cd frontend && npm install && npm run build
```

For UI work, run `mayviewer serve <world>` alongside `npm run dev` in
`frontend/`.

## Dev

```bash
python -m pytest -q tests/
```
