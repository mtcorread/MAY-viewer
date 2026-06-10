"""Phase 2 orchestrator — one streaming pass over a world into a cache.

Produces, next to the source file, ``.mayviewer_cache/<stem>/``:

  * ``manifest.json``        — schema snapshot, level/registry labels, artifact
                               paths and per-artifact ``geo_unit_id -> row_group``
                               indices (everything the server/frontend needs to
                               answer a query without opening the .h5)
  * ``aggregates/level_<L>.parquet`` — exact per-unit panels at every geo level
  * ``people|venues|members.parquet`` — drill-down shards (1 row group/unit)
  * ``hexbin.pmtiles``        — the self-contained map layer

Every step reads through :mod:`~mayviewer.prep.reader`, so the whole pipeline
is memory-flat and runs unchanged from 1k to 60M agents. It is idempotent:
re-running on an unchanged source is a no-op unless ``force``.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict
from pathlib import Path

import pyarrow.parquet as pq

from ..schema import describe
from . import aggregates, geo_tree, hexbin, personindex, pmtiles
from . import drilldown as dd  # aliased: ``drilldown`` is also a bool param of prep()
from .reader import WorldReader

logger = logging.getLogger("mayviewer.prep")

# v2: members carry ``home_geo_unit`` and the cache ships a person_home_unit
# index, so venue members living outside the inspected unit resolve.
# v3: optional transit artifact (train/tube lines, riders, leg chains). Bumping
# invalidates older caches (which lack these) so a feature activates on re-prep
# without a manual --force.
MANIFEST_VERSION = 3


def cache_dir(source: Path) -> Path:
    return source.parent / ".mayviewer_cache" / source.stem


def _fingerprint(source: Path) -> dict:
    st = source.stat()
    return {"size": st.st_size, "mtime": int(st.st_mtime)}


def prep(source: str | Path, force: bool = False,
         boundary_config: str | Path | None = None,
         drilldown: bool = True,
         transit_geometry: str | Path | None = None,
         mgu_coords: str | Path | None = None) -> dict:
    """Build the viewer cache. With ``drilldown=False`` the per-unit drill-down
    shards are skipped; the map + aggregates are still built, and the drill-down
    is recorded as a *lazy* presence index (which units have data), to be served
    live from the source ``.h5`` by ``mayviewer serve``.

    Pass both ``transit_geometry`` (``line_stops.csv``) and ``mgu_coords``
    (``coord_mgu.csv``) to additionally bake the transit layer (train/tube line
    geometry + per-line riders + per-rider leg chains). The two CSVs hold the
    line geometry, which the world ``.h5`` does not carry. The step is skipped
    gracefully when a world has no train/tube line venues, or none of its lines
    resolve to geometry in the supplied CSV."""
    source = Path(source).resolve()
    out = cache_dir(source)
    manifest_path = out / "manifest.json"

    # Boundaries change the cache contents, so a config (or its absence/edit)
    # must invalidate an otherwise up-to-date cache.
    bcfg = None
    if boundary_config:
        from .boundaries import load_boundary_config
        bcfg = load_boundary_config(boundary_config)
    bfp = _fingerprint(Path(boundary_config).resolve()) if boundary_config else None

    # Transit needs both CSVs; the geometry one without coordinates can't place
    # stops, so require the pair (or neither).
    if bool(transit_geometry) != bool(mgu_coords):
        raise ValueError("--transit-geometry and --mgu-coords must be given together.")
    # Either CSV changing (or being added/removed) must invalidate the cache.
    tfp = ([_fingerprint(Path(transit_geometry).resolve()),
            _fingerprint(Path(mgu_coords).resolve())]
           if transit_geometry else None)

    if manifest_path.exists() and not force:
        existing = json.loads(manifest_path.read_text())
        src = existing.get("source", {})
        if (existing.get("manifest_version") == MANIFEST_VERSION
                and src.get("fingerprint") == _fingerprint(source)
                and src.get("boundary_fingerprint") == bfp
                and src.get("transit_fingerprint") == tfp
                and bool(existing.get("drilldown_lazy", False)) == (not drilldown)):
            logger.info("Cache up to date: %s", out)
            return existing

    t0 = time.time()
    (out / "aggregates").mkdir(parents=True, exist_ok=True)
    schema = describe(source)

    with WorldReader(source) as r:
        geo = geo_tree.build(r)
        spatial = r.has_spatial

        boundaries_artifact: dict | None = None
        b_results: list = []
        if bcfg and not spatial:
            logger.warning(
                "Boundary config supplied but world has no geography/"
                "latitudes — boundaries require coordinates; ignoring.")
        if bcfg and spatial:
            from . import boundary_build
            logger.info("Boundaries: streaming shapes, matching, baking...")
            b_stats, b_results = boundary_build.build_boundaries(
                schema, geo, bcfg, out)
            if b_stats:
                boundaries_artifact = boundary_build.report_payload(
                    b_stats, b_results)

        logger.info("Aggregates: sweeping leaves...")
        rows, props = aggregates.compute(r, schema, geo)
        tables = aggregates.to_tables(rows, props, schema, geo)
        agg_paths = {}
        for lv, tbl in tables.items():
            if b_results:
                from . import boundary_build
                tbl = boundary_build.attach_extent_columns(tbl, lv, b_results)
            p = out / "aggregates" / f"level_{lv}.parquet"
            pq.write_table(tbl, p)
            agg_paths[str(lv)] = {
                "level_name": schema.label_geo_level(lv),
                "path": str(p.relative_to(out)),
                "units": tbl.num_rows,
            }

        # person_id -> home geo unit: the join that resolves venue members who
        # live outside the inspected unit. Built memory-flat for both modes —
        # baked into member rows here, and memory-mapped live by ``serve`` in
        # lazy mode (where members are built on demand from the .h5).
        logger.info("Person home-unit index...")
        personindex.build_home_units(r, out / personindex.FILENAME)
        home_lut = personindex.load_home_units(out / personindex.FILENAME)

        has_acts = "activity_mappings/activity_map/activity_data" in r
        if drilldown:
            logger.info("Drill-down shards...")
            p_name, p_idx = dd.write_people(r, schema, out)
            v_name, v_idx, v_meta = dd.write_venues(
                r, schema, out, want_index=has_acts)
            m_name, m_idx = dd.write_members(r, schema, out, home_lut)
            dd_art = {
                "people": {"path": p_name, "row_groups": p_idx},
                "venues": {"path": v_name, "row_groups": v_idx},
                "members": {"path": m_name, "row_groups": m_idx},
            }
            if has_acts:
                logger.info("Activity assignments...")
                a_name, a_idx = dd.write_activities(r, schema, out, v_meta)
                dd_art["activities"] = {"path": a_name, "row_groups": a_idx}
        else:
            # Lazy: skip the shards; record which units have data. ``serve``
            # reads each unit live from the .h5.
            logger.info("Drill-down: lazy (served live from .h5); indexing units...")

            def _presence(container: str) -> dict:
                p = r.partition(container)
                return {int(g): 0 for g, c in zip(p.geo_unit_ids, p.counts)
                        if int(c) > 0}

            dd_art = {
                "people": {"lazy": True, "endpoint": "people",
                           "row_groups": _presence("population")},
                "venues": {"lazy": True, "endpoint": "venues",
                           "row_groups": _presence("venues")},
                "members": {"lazy": True, "endpoint": "members",
                            "row_groups": _presence("subsets")},
            }
            if has_acts:
                dd_art["activities"] = {"lazy": True, "endpoint": "activities",
                                        "row_groups": _presence("activity")}

        pm_stats: dict | None = None
        if spatial:
            logger.info("Hexbin pyramid -> PMTiles...")
            hx = hexbin.build(r, schema, geo)
            pm_stats = pmtiles.write_pmtiles(hx, out / "hexbin.pmtiles")
        else:
            logger.info("Mapless world (no geography/latitudes): "
                        "skipping hexbin + boundaries.")

        # Transit (train/tube lines). Self-contained: line geometry comes from
        # the two CSVs, not the world, so it builds independent of `spatial`.
        transit_artifact: dict | None = None
        if transit_geometry and mgu_coords:
            from . import transit as tr
            logger.info("Transit: joining lines, riders, leg chains...")
            tres = tr.build_transit(r, schema, str(transit_geometry), str(mgu_coords))
            if not tres["line_venues"]:
                logger.warning("Transit: world has no train/tube line venues; "
                               "skipping transit layer.")
            elif not tres["in_world_lines"]:
                logger.warning("Transit: %d line venues found but none resolve to "
                               "geometry in %s; skipping transit layer.",
                               len(tres["line_venues"]), transit_geometry)
            else:
                tpm = pmtiles.write_transit_pmtiles(
                    tres["in_world_lines"], out / "transit.pmtiles")
                r_name, r_idx = tr.write_rider_shards(
                    tres["riders"], tres["line_venues"], out, home_lut)
                c_name, c_idx = tr.write_chain_shards(
                    tres["chains"], tres["line_venues"], tres["chain_fields"],
                    out, home_lut)
                transit_artifact = tr.report_payload(
                    tres, tpm, r_name, r_idx, c_name, c_idx)

        # Largest single-unit slice per container == the pipeline's actual
        # peak-memory bound; recorded so 60M runs can be reasoned about.
        # Some worlds omit containers entirely (e.g. no activity_mappings) —
        # skip those rather than failing.
        from .reader import PARTITIONS
        peak = {}
        for c, base in PARTITIONS.items():
            if base in r:
                peak[c] = r.partition(c).max_count

    artifacts: dict = {
        "aggregates": agg_paths,
        "drilldown": dd_art,
    }
    if pm_stats is not None:
        artifacts["hexbin"] = {"path": "hexbin.pmtiles", **pm_stats}
    if boundaries_artifact:
        artifacts["boundaries"] = boundaries_artifact
    if transit_artifact:
        artifacts["transit"] = transit_artifact

    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "source": {
            "path": str(source),
            "name": source.name,
            "fingerprint": _fingerprint(source),
            "boundary_fingerprint": bfp,
            "transit_fingerprint": tfp,
        },
        "spatial": spatial,
        # True ⇒ drill-down shards were not built; serve reads them live.
        "drilldown_lazy": not drilldown,
        # person_id -> home geo unit index (relative to the cache dir). Used by
        # lazy serve to tag each member with its home unit; ignored by the
        # static browser path (members carry the tag baked in).
        "person_home_unit": personindex.FILENAME,
        "schema": asdict(schema),
        "geo": {
            "level_values": geo.level_values,
            "level_names": {str(lv): schema.label_geo_level(lv)
                            for lv in geo.level_values},
            "leaf_level": geo.leaf_level,
            "units_per_level": {str(lv): int(len(geo.ids_at(lv)))
                                for lv in geo.level_values},
        },
        "artifacts": artifacts,
        "peak_unit_rows": peak,
        "build_seconds": round(time.time() - t0, 1),
    }
    manifest_path.write_text(json.dumps(manifest, indent=1, default=int))
    logger.info("Wrote cache %s (%.1fs)", out, manifest["build_seconds"])
    return manifest
