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
domain-specific terms baked in. An online basemap is available on request
via `mayviewer serve … --basemap osm|carto-dark|carto-light|<xyz-url>`.

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
