# MAY-viewer — Handover

> Read this first. It is the **self-contained brief** for continuing this
> project in a fresh Claude conversation started inside
> `/Users/marthacorrea/MAY-viewer`. You should not need to re-discover
> anything below by reading code.

## 0. Start here (current state + immediate next action)

- **Phase 1 (synth scale generator) — ✅ done.**
- **Phase 2 (streaming prep pipeline) — ✅ done, 10M-validated.**
- **Phase 3 (frontend + `mayviewer serve`) — ✅ built & working.**
- **Phase 3.5 (geo boundary overlay) — ✅ done.** Diagnostic
  (`mayviewer match`), ingestion (#8), frontend click-drill (#9), opt-in
  basemap (#10) **and the real-world re-validate (#11) are all built &
  validated**. #11: the London world (`london_xlgu.h5`, 26,369 OAs)
  matched the *same* ONS files at **100% on all 4 levels** with zero
  config/code changes; boundaries baked into its cache, tiles decode.
- **Committed?** Not since the initial commit. The user commits only when
  they explicitly ask — do **not** commit or push unprompted. The frontend
  build output (`mayviewer/web/dist/`) is gitignored and is *not* committed.

**Phase 3.5 is complete (#8–#11 all done).** There is no blocked next
action; pick up Phase 4 (see §7 caveats — sparse-pyramid/footer scaling) or
new direction from the user. Reference state below. The dev cache at
`/Users/marthacorrea/MAY/output/2021/.mayviewer_cache/world_state/` has
`boundaries.pmtiles` (layers `XLGU/LGU/MGU/SGU`, contiguous bands
0-5 / 6-8 / 9-11 / 12-22) + `geo:lon/lat/bbox*` columns + `manifest.json
artifacts.boundaries`; the frontend renders clickable boundary polygons
with map↔tree drill; and an **opt-in** online basemap is available via
`mayviewer serve … --basemap osm|carto-dark|carto-light|<xyz-url>`
(default none ⇒ zero external requests). Smoke tests:
`frontend/scripts/boundtile.mjs <base>` (tiles+props) and
`curl <base>/app-config.json` (`{"basemap":null}` w/o the flag).

Boundary ingest is run with:
```
mayviewer prep <world.h5> --boundary-config <config.json>
```
The config (per level: `file`, `prop`, `strategy`, optional `crs`) is at
`user_data/shapefiles/boundary_config.json`. Real shapes (one GeoJSON **per
level**, mixed CRS) live in `user_data/shapefiles/`.

Ready-made caches to develop the frontend against (no rebuild needed):
- 630k NE-England world, **boundaries baked**: `/Users/marthacorrea/MAY/output/2021/.mayviewer_cache/world_state/`
- London world (26,369 OAs), **boundaries baked**: `/Users/marthacorrea/MAY/output/2021/.mayviewer_cache/london_xlgu/`
- 10M synth world (no shapes — hexbin fallback): `/Users/marthacorrea/MAY-viewer/.synth/.mayviewer_cache/world_10m/`

## 1. What this is

A **local-first visualisation tool** for [MAY](/Users/marthacorrea/MAY)
synthetic-world HDF5 files. A user runs a CLI against **their own**
`world_state.h5`; a browser opens and lets them explore the world: geography
drill-down, households and their individual members with all attributes,
venues (schools, classrooms, companies… whatever that world defines), and
stats. It must work from **1,000 to ~60,000,000 agents** without being slow.

## 2. Philosophy / non-negotiables

1. **Never ship the whole world to the browser; never load the whole file to
   answer one query.** Phase 3 consumes the cache through bounded reads only
   (one PMTiles tile, one Parquet row group, one aggregate row). `manifest.json`
   is the one intentional whole fetch — it *is* the precomputed index.
2. **Schema-driven, zero hardcoded domain terms.** Geo level names, venue
   types, subset roles, property names differ per world. They are *always*
   read from `manifest.json` (mirrors `mayviewer/schema.py`). The frontend
   renders every label from the manifest / self-describing column tokens.
3. **Local-first, ~zero cost.** The file never leaves the user's machine.
   `mayviewer serve` is a dependency-free static server; the same cache dir
   is droppable on any CDN unchanged. Any internet dependency (e.g. an online
   basemap) must be **opt-in**, never the default.
4. **Lazy + bounded.** Map shows precomputed aggregates/tiles; individuals
   are materialised only when the user drills into one bounded unit/venue —
   one O(1) Parquet row-group read via the manifest `row_groups` index.
5. **All viewer-derived data is computed in the `prep` layer, never by
   changing world creation.** The `.h5` is MAY's authoritative artifact.
   Everything the viewer needs (centroids, tiles, boundaries) is derivable
   from what the world already contains; prep is idempotent and re-runnable.
6. **Geography boundaries are an optional, country/world-agnostic overlay**
   matched by geo code/name. Default render (hexbin/centroid) needs no
   shapefiles and must keep working when none are supplied (e.g. Mordor).

## 3. Key facts about the MAY data

- Authoritative schema = `/Users/marthacorrea/MAY/may/serialization/world_serializer.py`
  (+ `world_loader.py`, `serialization_config.py`).
- Dev source world: `/Users/marthacorrea/MAY/output/2021/world_state.h5`
  (203 MB, 630,206 people, 303,415 venues, 2,234 geo units, 4 levels
  XLGU>LGU>MGU>SGU ≈ region>LAD>MSOA>OA; North-East/Darlington, **not** London).
- Layout is SoA + CSR, partition-indexed by geo unit. This is the scaling
  enabler. ⚠️ Use dataset lengths, never the `num_*` count attrs.
- ⚠️ **World-specific facts (confirmed, in memory):** containers can
  partition at *any* geo level (this world's workplaces are at MGU, not the
  SGU leaf). Most venues + **all people have no own coordinate**; everything
  is placed at its **geo-unit centroid**. The world *does* carry per-unit
  `geography/latitudes|longitudes` (real centroids) and `geography/{ids,
  levels,parent_ids}`.
- ⚠️ **`metadata/names/geography` is mixed *per level*** (discovered in
  Phase 3.5): in the dev world XLGU=`North East`, LGU=`Darlington`/`County
  Durham` (human **names**); MGU=`E02002570`, SGU=`E00062043` (ONS **codes**).
  Hence boundary matching must be **per-level** (exact-code vs normalized-name).
- The user is separately building a **London** world in MAY; the whole stack
  is schema/coord-driven and needs no changes for it.

## 4. What has been done

`mayviewer/` package, console script `mayviewer`, conda env `mayviewer`
(Python 3.13). **14 tests** (`tests/test_scale_generator.py` +
`tests/test_prep.py`) — see §9 for a known full-suite ordering flake.

### Phases 1–2 (pipeline)
- `schema.py` — `describe(path) -> WorldSchema`, the single domain source.
- `synth/scale_generator.py` — streaming K-replica tiler (Phase 1).
- `prep/` — streaming, memory-flat pipeline (Phase 2): `reader.py`
  (per-unit CSR slicing), `geo_tree.py` (level-agnostic rollup),
  `aggregates.py`, `drilldown.py` (1 row group/unit), `hexbin.py`
  (multi-res H3), `pmtiles.py` (pure-Python PMTiles v3/MVT), `pipeline.py`
  + `mayviewer prep`. 10M gate: 280 s, peak RSS ≈ 1.0 GB.

### Phase 3 (frontend + serve) — ✅ done
- **`mayviewer/serve.py`** — dependency-free, **Range-capable** static
  server (`mayviewer serve <world|cache> [--port] [--host] [--no-open]`).
  Serves `/cache/*` + the bundled SPA. Verified: 206/suffix ranges,
  traversal → 404, SPA fallback. This is the *entire* backend.
- **`frontend/`** — Vite + React + TS, **MapLibre GL + PMTiles + hyparquet**
  (deck.gl intentionally dropped — native MapLibre fill is lighter and fully
  offline; the hexes carry only `{count,res}` so an overlay added nothing).
  Builds to `mayviewer/web/dist/` (gitignored). Self-hosted fonts (offline).
  - `data/manifest.ts`, `data/parquet.ts` (bounded row-group reads via
    `row_groups` index; **`normalizeRows` coerces hyparquet BigInt→Number**),
    `data/columns.ts` (parses self-describing aggregate columns → labels).
  - `state/store.ts` (zustand), `components/`: `MapView`, `GeoTree`,
    `StatsPanel`, `Inspector`. Aesthetic: "cartographic observatory".
  - Map mode = hexbin density backdrop + tree-driven geo drill-down +
    schema-labelled stats. Inspect mode = venues/people/person via one
    row-group read; member roster by subset; friends resolved in-loaded
    row-group else shown as IDs (the agreed v1 social-graph behaviour).
  - `frontend/scripts/probe.mjs` & `maptile.mjs` validate bounded reads /
    tile decode against the running server (both caches).

### Phase 3.5 (boundary overlay) — diagnostic + ingestion done
- **`prep/boundaries.py` + `mayviewer match`** — ✅ built & smoke-tested.
  Read-only diagnostic (see above). `pip install -e ".[geo]"` adds pyshp +
  shapely + pyproj (wheels, no system GDAL); the diagnostic & core viewer
  need none of it.
- **Boundary ingestion (task #8) — ✅ built & validated.**
  - `mayviewer prep <world.h5> --boundary-config FILE`. Config = JSON, one
    entry **per level**: `{file, prop, strategy, crs?}`. The user's real
    shapes are **one GeoJSON per level** (not one file for all) in
    `user_data/shapefiles/`; sample config there.
  - `boundaries.py`: config loader + a **single size/format-agnostic
    streaming feature reader** (`iter_features`) — no per-level/size special
    case (which level is huge is world-specific). Cheap regex key
    pre-filter ⇒ only matched features are JSON/geometry-parsed (2.7 GB OA
    streamed in seconds). `read_geojson_crs` reads the file's declared CRS.
  - `boundary_build.py`: per-level match→shapely; **reprojects from each
    file's own CRS to EPSG:4326** (dev LAD file is EPSG:27700 — never assume
    lon/lat); contiguous per-level zoom bands computed from the world's level
    count (`_zoom_bands`); centroid+bbox per unit; manifest report payload.
  - `pmtiles.py`: generalised to real polygons (rings+holes+MultiPolygon,
    winding-correct, string props), **per-tile clipped + per-zoom
    simplified**; `write_boundary_pmtiles` bakes one layer/level into one
    contiguous pyramid (MapLibre overzooms within a band ⇒ viewport-only
    tile fetch/evict, the user's memory requirement). int32 e7 clamp.
  - `pipeline.py`/`cli.py`: `--boundary-config` wired; adds
    `geo:lon/lat/bbox*` cols to `aggregates/level_*.parquet`,
    `artifacts.boundaries` + stored report to manifest, and a
    `boundary_fingerprint` so editing the config invalidates the cache.
    **No config ⇒ behaviour byte-identical to before (overlay optional).**
  - Validated on 2021 dev world: 1/1, 2/2, 79/79, 2152/2152 (100%), 80.9 s
    full build, 109 tiles, tiles decode to POLYGONs with geo_id/level/code.

### Phase 3.5 (boundary overlay) — frontend click-drill done (#9)
- **`frontend/src/data/manifest.ts`** — typed `artifacts.boundaries`
  (`BoundaryArtifact` + per-level `BoundaryLevel`) and
  `source.boundary_fingerprint`.
- **`state/store.ts`** — `mapMode: "boundaries"|"hexbin"` (defaults to
  boundaries when baked, else hexbin → non-negotiable #6 preserved);
  `drillToGeo(geoId, level)` resolves a unit and **rebuilds the full
  ancestor breadcrumb via `parent_id`** (this is the long-missing map
  geo_id path); `aggRow(level, geoId)` synchronous extent/centroid read.
- **`components/MapView.tsx`** — one PMTiles **vector source per level**
  pinned `minzoom=maxzoom=bake_zoom` (MapLibre overzooms the single baked
  zoom across the band ⇒ viewport-only fetch, levels swap on zoom);
  fill+line+selection layers per band; click a polygon → `drillToGeo`;
  `selected` (tree OR map) → highlight via `setFilter` + `fitBounds` to
  `geo:bbox_*` (centroid `geo:lon/lat` fallback), capped at the level's
  band so the selected level stays visible. Boundaries/Hexbin toggle +
  manifest-driven level-band legend. Hexbin path byte-identical when no
  shapes. `geo:*` are FLOAT64 — numeric-safe, no BigInt trap.
- **`frontend/scripts/boundtile.mjs`** — new headless probe: decodes every
  level's MVT tiles via the pmtiles+vector-tile path and checks
  `geo_id/level/code` vs the manifest. All 4 levels PASS on the dev cache;
  tile `geo_id`s cross-check against aggregate rows + real `geo:bbox_*`.
  (Headless can't drive actual map click/render — interaction unverified
  in-browser; data contract fully verified.)

### Decisions already taken with the user (do not relitigate)
- Serve model: **static + client-side** (hyparquet/pmtiles over Range).
- Geo drill UX: **tree-driven; map is a density backdrop**, not the click
  target (hexes have no geo_id). *Boundary overlay will add real click-drill.*
- Social graph v1: list + in-unit links, IDs for cross-unit.
- Phase 3.5 scope: **full boundary ingestion**; user has shapes but is
  **unsure of code match** (hence the diagnostic-first approach); basemap =
  **opt-in online + local default**.

## 5. The prep cache contract (what the frontend consumes)

`.mayviewer_cache/<source_stem>/`: `manifest.json`,
`aggregates/level_<L>.parquet` (1 row/unit), `people|venues|members.parquet`
(1 row group/unit), `hexbin.pmtiles` (layers `people`,`venues`; props
`{count,res}`).

`manifest.json`: `schema` (full WorldSchema), `geo`
(`level_values` leaf-first, `level_names`, `leaf_level`, `units_per_level`),
`artifacts.aggregates["<L>"]`, `artifacts.drilldown.{people,venues,members}`
= `{path, row_groups: {"<geo_id>": <rg_index>}}` (the O(1) drill-down),
`artifacts.hexbin`, `peak_unit_rows`, `build_seconds`, `source.fingerprint`.
Aggregate columns self-describe: `people,mean_age,age:<band>,sex:<label>,
venues:<type>,occ:<type>,p:<prop>=<cat>` + `geo_id,geo_name,level,
level_name,parent_id` + (when boundaries baked) `geo:lon,geo:lat,
geo:bbox_minlon,geo:bbox_minlat,geo:bbox_maxlon,geo:bbox_maxlat` (NULL for
unmatched units). **All INT64 → BigInt in hyparquet — already normalized in
`data/parquet.ts`; the `geo:*` cols are FLOAT64; keep new code numeric-safe.**
When boundaries baked, manifest also has `artifacts.boundaries` =
`{path, tiles, features, minzoom, maxzoom, bytes, layers, levels:[{level,
level_name, prop, strategy, src_crs, world_units, matched, rate, bake_zoom,
minzoom, maxzoom, file}]}` and `source.boundary_fingerprint`.

## 6. Phase 3.5 plan — remaining work (tasks #9–#11)

8. **Boundary ingestion in `prep`** — ✅ **DONE & validated** (see §4
   "Phase 3.5"). `mayviewer prep world.h5 --boundary-config FILE`. Per-level
   JSON config `{file, prop, strategy, crs?}`; one file per level supported;
   reprojects from each file's declared CRS; contiguous per-level zoom
   pyramid in `boundaries.pmtiles`; `geo:*` aggregate cols; manifest report;
   no config ⇒ unchanged. Modules: `prep/boundaries.py` (+config/streaming),
   `prep/boundary_build.py` (new), `prep/pmtiles.py` (generalised).
9. **Frontend: real shapes + click-drill** — ✅ **DONE & validated** (see
   §4 "frontend click-drill done"). Per-level PMTiles vector sources +
   fill/line/selection layers; click → `store.drillToGeo` (rebuilds the
   breadcrumb via `parent_id`); `selected`→highlight+`fitBounds` to
   `geo:bbox_*`; Boundaries/Hexbin toggle; hexbin unchanged with no shapes.
10. **Optional basemap** — ✅ **DONE & validated**. `serve --basemap
    PRESET|URL` (presets `osm`/`carto-dark`/`carto-light`, or an XYZ raster
    URL template). Resolver + presets in `serve.py` (`_basemap_spec`);
    served as `/app-config.json` (`{"basemap": …|null}` — app config, read
    once like the manifest). Frontend: `data/appConfig.ts` (defensively
    validated so a SPA-fallback can't enable tiles), store `basemap`/
    `basemapOn`, `MapView` adds a raster source/layer **beneath** all data
    + a None/<name> switcher + attribution. Default (no flag) ⇒ no source,
    no switcher, zero external requests (non-negotiable #3). The flag is
    the explicit opt-in so it starts on; "No basemap" turns it off.
    *Not yet covered (future): MapLibre style-URL basemaps, local
    raster/PMTiles, and the fictional-world non-georeferenced-CRS wrinkle
    (flat coord box / orthographic) — raster XYZ only for now.*
11. **Validate** — `mayviewer match` on the real world vs ONS shapes; full
    `prep --boundaries`; verify click-drill, fit-to-unit, bounded tile
    reads, no whole-file fetch, labels still 100% manifest-driven.

## 7. Known limitations / Phase-4 caveats (documented in README + memory)

- Map is a **single-resolution** density backdrop: the hexbin pyramid is
  *sparse* (`RES_TO_ZOOM={3:4,5:7,7:10,9:13}` — tiles only at z4/7/10/13).
  MapLibre can't fill interior zoom gaps, so the source is pinned to one
  representative zoom and overzoomed. Real fix = boundary overlay (Phase
  3.5) or a contiguous prep pyramid.
- ~~Map does not yet recentre on the selected unit.~~ Fixed in #9:
  `selected` fits to `geo:bbox_*`. (Sparse-pyramid note below still applies
  to **hexbin mode only**; boundary mode uses per-level pinned sources.)
- Cross-unit friend identity deferred (count + in-unit links only).
- **Parquet footer grows with unit count** (1 row group/unit): ~43 MB at
  34k units (10M). Read once and cached per file handle, but ~200k England
  OAs → hundreds of MB first-read. Phase-4 (page index / sidecar offsets /
  shard split). Lives alongside `members.parquet` linear growth.

## 8. Running things

```bash
source /opt/homebrew/anaconda3/etc/profile.d/conda.sh && conda activate mayviewer
# (recreate env: conda create -y -n mayviewer python=3.13 && pip install -e ".[dev]")

python -m pytest -q tests/                       # 14 tests (see §9 flake)
mayviewer describe <world.h5>
mayviewer prep     <world.h5> [--force] [--boundary-config FILE]
mayviewer match    <world.h5> <shapes.geojson|.shp> [--prop NAME]   # read-only
# boundary bake (validated): config at user_data/shapefiles/boundary_config.json
mayviewer prep <world.h5> --boundary-config /abs/path/boundary_config.json
cd frontend && npm install && npm run build       # → mayviewer/web/dist/
mayviewer serve    <world.h5|cache-dir> [--port 8000] [--no-open]
#   optional opt-in basemap (default none): --basemap osm|carto-dark|carto-light|<xyz-url>
# UI hot-reload: `mayviewer serve …` + (cd frontend && npm run dev) — Vite
# proxies /cache to :8000. Hard-reload the browser after a rebuild.
```

`pip install -e ".[geo]"` adds the optional boundary stack (pyshp/shapely/
pyproj). Note: the Bash working directory **does persist** between calls in
this harness — a `cd` earlier in the session changes where later relative
paths resolve (this caused a doubled-path bug here). Prefer **absolute
paths** for `--boundary-config` and cache/world arguments.

## 9. Gotchas a successor must know

- **hyparquet returns Parquet INT64 as `BigInt`.** Comparing against
  manifest numbers (`0n === 0` is false) silently breaks all geo/inspector
  lookups while `StatsPanel` still works (it routes through `num()`).
  Handled by `normalizeRows` in `data/parquet.ts`; keep new reads numeric.
- **Sparse PMTiles pyramid** — see §7; don't "fix" the map by widening the
  zoom range, that just re-blanks it.
- **Full-suite test flake:** `tests/test_prep.py::test_drilldown_one_row_
  group_per_unit_and_roundtrip` fails only in combined-order runs (passes
  alone and as `pytest tests/test_prep.py`, 9/9). Pre-existing
  cross-file fixture isolation issue; **not** caused by Phase 3. Don't
  modify tests to chase it unless asked.
- The earlier "map black screen" + "XLGU is a leaf" were the BigInt bug +
  sparse pyramid — both fixed. If they reappear, suspect a regression there.
- **`style.glyphs: undefined` ⇒ wholly black map.** A MapLibre upgrade made
  the style validator strict; `glyphs: undefined` (Phase-3 code, no text
  layers) now throws `glyphs: string expected, undefined found` and aborts
  the *entire* style load — zero layers, both modes black, cascade
  `layer '…' does not exist`. Fix (done in #9): omit the `glyphs` key
  entirely; guard `queryRenderedFeatures` behind `isStyleLoaded()`+
  `getLayer()`. See [[frontend-data-gotchas]] #3.

## 10. Memory (persisted across sessions; auto-loaded)

`~/.claude/projects/-Users-marthacorrea-MAY-viewer/memory/`:
- `may-viewer-architecture.md` — project shape/env/phases (Phase 3 done).
- `venue-partition-level-is-world-specific.md` — containers partition at any
  level; sparse venue coords; rollup must stay level-agnostic.
- `parquet-footer-grows-with-unit-count.md` — Phase-4 footer caveat.
- `frontend-data-gotchas.md` — BigInt + sparse-pyramid traps.

Trust these but re-verify file/flag names against the code before relying.
