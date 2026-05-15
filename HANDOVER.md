# MAY-viewer — Handover

> Read this first. It is the self-contained brief for continuing this project
> in a fresh Claude conversation started inside `/Users/marthacorrea/MAY-viewer`.

## 1. What this is

A **local-first visualisation tool** for [MAY](/Users/marthacorrea/MAY)
synthetic-world HDF5 files. A user runs a CLI against **their own**
`world_state.h5`; a browser opens and lets them explore the world: geography
drill-down, households and their individual members with all attributes,
venues (schools, classrooms, companies… whatever that world defines), and
stats (e.g. who is in each classroom, age/ethnicity mix per area).

It must work from **1,000 to ~60,000,000 agents** (all England) without being
slow. It replaces an old prototype that was slow because it was a desktop app
that loaded the whole world into memory as per-agent objects.

## 2. Philosophy / non-negotiables

1. **Never ship the whole world to the browser; never load the whole file to
   answer one query.** Everything else follows from this.
2. **Schema-driven, zero hardcoded domain terms.** Geography level names,
   venue types, subset roles, person/venue property names are all
   config-driven in MAY and differ per world (England vs New Zealand; a world
   may have no schools at all). They are *always* read from the file's own
   `metadata/registries` and group layout via `mayviewer/schema.py`. The only
   thing that may be assumed is MAY's container layout (population / venues /
   geography / activity_mappings, each with a `partition_index`) — that is the
   MAY serializer contract, not a domain term.
3. **Local-first, ~zero cost.** The file never leaves the user's machine.
   Hosting, if ever, is a thin optional static layer (PMTiles + per-geo-unit
   Arrow shards on a CDN) over the same precomputed output — never a
   file-upload backend (a 20 GB browser upload is impractical).
4. **Lazy + bounded.** Map mode shows precomputed aggregates; individual
   agents are materialised only when the user drills into a small, bounded
   area (one geo unit / one venue). This is possible because the MAY file is
   already columnar (structure-of-arrays) and partition-indexed by geo unit
   (CSR: `geo_unit_ids → start_indices → counts`), so any drill-down is one
   O(1) bounded slice.
5. **Geography boundaries are an optional, country-agnostic overlay** matched
   by geo code (user supplies England ONS / NZ SA / …). Default render
   (coordinates / H3 hexbin) needs no shapefiles.

## 3. Key facts about the MAY data

- Authoritative schema = `/Users/marthacorrea/MAY/may/serialization/world_serializer.py`
  (and `world_loader.py`, `serialization_config.py`). Treat it as the contract.
- Dev source world: `/Users/marthacorrea/MAY/output/2021/world_state.h5`
  (203 MB, 630,206 people, 303,415 venues, 2,234 geo units, 4 geo levels
  XLGU>LGU>MGU>SGU ≈ region>LAD>MSOA>OA).
- Layout is SoA + CSR, partition-indexed by geo unit on population, venues,
  subsets, members, and activity maps. **This is the scaling enabler.**
- `metadata/registries/{geo_levels,venue_types,subset_names,sex,properties}`
  hold all categorical labels. `metadata/names/*` hold per-entity names.
- `venues/subsets/members_flat`+`members_offsets` = CSR venue→members.
  `activity_mappings/activity_map/activity_data` = (person_id, activity_idx,
  venue_id, subset_idx) rows; `activity_offsets` indexes it per person.
- ⚠️ Gotcha: `f.attrs["num_venues"]` (303,412) ≠ `venues/ids` length
  (303,415). **Always use dataset lengths, never the count attrs.**
- ⚠️ `population/properties/friendships` in raw MAY is a JSON **string** per
  person (object dtype) — slow/bloated at 60M. Phase 1 upgrades it to a typed
  CSR triple `population/friendships/{flat,offsets,counts}`.

## 4. What has been done

Repo scaffolded: `pyproject.toml` (console script `mayviewer`), package
`mayviewer/`, tests, `.gitignore`, `README.md`. **Not committed** (commit only
when the user asks). Git is initialised.

- **`mayviewer/schema.py`** — `describe(path) -> WorldSchema`. The only place
  the viewer learns what a world contains; everything downstream consumes the
  world through it. Detects typed-CSR relation groups (e.g. `friendships`)
  vs. flat property arrays. Verified on the real world.
- **`mayviewer/synth/scale_generator.py`** (Phase 1, ✅) — `ScaleGenerator` /
  `generate(source, target_people, out)`. Tiles a real world into K disjoint,
  ID-rebased replicas to reach the target size. Faithful to every dataset /
  partition index / registry; discovers venue types & property groups
  dynamically; rebases all id-spaces and CSR offsets per replica; lays
  replicas on a map grid so they render as distinct clusters; converts
  friendships → CSR. Streaming: peak memory ≈ one source world (60M ≈ 96
  sequential replicas).
- **`mayviewer/cli.py`** — `mayviewer describe <world>` and
  `mayviewer synth <source> --people 60M --out f.h5` (`--people` accepts
  `1k`/`10m`/`60M`/int).
- **`tests/test_scale_generator.py`** — 5 tests, all passing: replica
  disjointness, partition-index↔geo-column consistency, friendships-CSR
  correctness (incl. cross-replica offset), activity-data id ranges. Skips if
  the real source world is absent.
- **`mayviewer/prep/`** (Phase 2, ✅ — built & validated on the 630k real
  world; 9 tests in `tests/test_prep.py`, all 14 pass):
  - `reader.py` — `WorldReader`: the memory-flat engine. Slices one geo unit
    at a time via the file's CSR `partition_index`; never reads a bulk array
    whole. Peak memory = largest single unit. **The 60M enabler.**
  - `geo_tree.py` — geography hierarchy; vectorised leaf→ancestor for rollup.
    Level *names* from the schema; only structural fact assumed is
    "larger level value = finer unit, leaf = max" (MAY contract).
  - `aggregates.py` — one sweep → exact per-unit counts (people, age bands,
    sex, categorical person props, venues & occupancy per type), rolled up to
    every level. Categorical props auto-classified (JSON/relational/high-card
    excluded). Rollup is **level-agnostic** — keyed off each row's actual geo
    unit, so workplace venues attached above the leaf are not dropped.
  - `drilldown.py` — `people/venues/members.parquet`, **one row group per
    geo unit**; manifest carries `geo_unit_id→row_group` ⇒ O(1) drill-down.
    Friendships consumed via `schema.person_relations` (CSR) or JSON-array
    property; JSON arrays typed losslessly (int vs string).
  - `hexbin.py` — multi-resolution H3 pyramid (people via geo-unit centroid
    weighted by partition count = zero bulk reads; venues via own coord with
    geo-centroid fallback). O(cells) at coarse zoom ⇒ 60M-safe.
  - `pmtiles.py` — pure-Python PMTiles v3 + MVT writer (no tippecanoe).
    Validated against reference `pmtiles`/`mapbox-vector-tile` libs.
  - `pipeline.py` — orchestrator → `.mayviewer_cache/<stem>/` + `manifest.json`
    (schema snapshot, level/registry labels, artifact paths, row-group
    indices, peak-unit-rows). Idempotent (source size+mtime fingerprint).
  - CLI: `mayviewer prep <world> [--force]`.
  - ⚠️ Data facts learned (world-specific, not assumptions): MAY containers
    can partition at *any* geo level (2021 world: workplaces at MGU, not the
    SGU leaf); most venues have **NaN coordinates** (only ~832/303k
    "destination" venues carry lat/lon) — both handled, see memory notes.

### Running things

```bash
# Dedicated env: conda env `mayviewer` (Python 3.13), project installed -e.[dev].
# Recreate: conda create -y -n mayviewer python=3.13 && conda activate mayviewer
#           && pip install -e ".[dev]"
source /opt/homebrew/anaconda3/etc/profile.d/conda.sh && conda activate mayviewer
python -m pytest -q tests/
mayviewer describe /Users/marthacorrea/MAY/output/2021/world_state.h5
```

## 5. What's next

**Phase 2 — streaming prep pipeline** — ✅ DONE and 10M-validated (see §4).
The scaling/memory-flat gate passed on `.synth/world_10m.h5` (10,083,296
people, 3.8 GB): `mayviewer prep` ran in 280 s with **peak RSS ≈ 1.0 GB**
and `peak_unit_rows` *identical* to the 630k run — memory is bounded by the
largest single geo unit, not world size. Invariants exact at 10M (people
10,083,296 conserved at every level; venues 4,854,640; 34,432 row-groups ↔
34,432 units; unit round-trip exact). Cache ≈ 1.2 GB (members 647M, people
409M, venues 163M, hexbin 3.2M). 60M projection: streaming parts are
O(largest unit); the one O(num_geo_units) term is the in-RAM aggregate
accumulator (34,432 units → ~hundreds of MB at 10M; ~200k OAs at 60M-England
→ low-GB, still bounded). Output scales ~linearly; revisit members.parquet
size at 60M in Phase 4.

**Phase 3 — frontend** (next): React + deck.gl + MapLibre, consuming the
cache only (never the .h5). Map mode = PMTiles hexbin + aggregate panels read
from `aggregates/level_<L>.parquet`, geo drill-down via the manifest geo tree.
Inspector mode = pick a unit/venue/person → one `read_row_group` from
`people|venues|members.parquet` using the manifest `row_groups` index. All
labels from `manifest.schema`, never constants. Likely needs a thin local
read-only server (range reads over the parquet/pmtiles) or a static export.

**Phase 4** — 60M synth end-to-end; confirm flat memory & one-fetch
drill-down latency; package the CLI; optional static-demo export.

Immediate step: confirm the in-flight 10M `prep` run finishes with bounded
RSS and conserved invariants (check the cache at
`/Users/marthacorrea/MAY-viewer/.synth/.mayviewer_cache/world_10m/`), then
start Phase 3. The user is also building a **London** real world in MAY —
the pipeline is schema/coord-driven and needs no changes for it.

## 6. Open questions for the user (when relevant)

- Frontend packaging form factor was chosen as **CLI + browser** (not
  Docker/desktop) — revisit only if asked.
- Boundary-overlay ingestion format (GeoJSON/TopoJSON keyed by geo code) —
  decide when Phase 2/3 reaches the map layer.
