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
from . import aggregates, drilldown, geo_tree, hexbin, pmtiles
from .reader import WorldReader

logger = logging.getLogger("mayviewer.prep")

MANIFEST_VERSION = 1


def cache_dir(source: Path) -> Path:
    return source.parent / ".mayviewer_cache" / source.stem


def _fingerprint(source: Path) -> dict:
    st = source.stat()
    return {"size": st.st_size, "mtime": int(st.st_mtime)}


def prep(source: str | Path, force: bool = False) -> dict:
    source = Path(source).resolve()
    out = cache_dir(source)
    manifest_path = out / "manifest.json"

    if manifest_path.exists() and not force:
        existing = json.loads(manifest_path.read_text())
        if existing.get("source", {}).get("fingerprint") == _fingerprint(source):
            logger.info("Cache up to date: %s", out)
            return existing

    t0 = time.time()
    (out / "aggregates").mkdir(parents=True, exist_ok=True)
    schema = describe(source)

    with WorldReader(source) as r:
        geo = geo_tree.build(r)

        logger.info("Aggregates: sweeping leaves...")
        rows, props = aggregates.compute(r, schema, geo)
        tables = aggregates.to_tables(rows, props, schema, geo)
        agg_paths = {}
        for lv, tbl in tables.items():
            p = out / "aggregates" / f"level_{lv}.parquet"
            pq.write_table(tbl, p)
            agg_paths[str(lv)] = {
                "level_name": schema.label_geo_level(lv),
                "path": str(p.relative_to(out)),
                "units": tbl.num_rows,
            }

        logger.info("Drill-down shards...")
        p_name, p_idx = drilldown.write_people(r, schema, out)
        v_name, v_idx = drilldown.write_venues(r, schema, out)
        m_name, m_idx = drilldown.write_members(r, schema, out)

        logger.info("Hexbin pyramid -> PMTiles...")
        hx = hexbin.build(r, schema, geo)
        pm_stats = pmtiles.write_pmtiles(hx, out / "hexbin.pmtiles")

        # Largest single-unit slice per container == the pipeline's actual
        # peak-memory bound; recorded so 60M runs can be reasoned about.
        peak = {c: r.partition(c).max_count
                for c in ("population", "venues", "subsets", "members",
                          "activity")}

    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "source": {
            "path": str(source),
            "name": source.name,
            "fingerprint": _fingerprint(source),
        },
        "schema": asdict(schema),
        "geo": {
            "level_values": geo.level_values,
            "level_names": {str(lv): schema.label_geo_level(lv)
                            for lv in geo.level_values},
            "leaf_level": geo.leaf_level,
            "units_per_level": {str(lv): int(len(geo.ids_at(lv)))
                                for lv in geo.level_values},
        },
        "artifacts": {
            "aggregates": agg_paths,
            "drilldown": {
                "people": {"path": p_name, "row_groups": p_idx},
                "venues": {"path": v_name, "row_groups": v_idx},
                "members": {"path": m_name, "row_groups": m_idx},
            },
            "hexbin": {"path": "hexbin.pmtiles", **pm_stats},
        },
        "peak_unit_rows": peak,
        "build_seconds": round(time.time() - t0, 1),
    }
    manifest_path.write_text(json.dumps(manifest, indent=1, default=int))
    logger.info("Wrote cache %s (%.1fs)", out, manifest["build_seconds"])
    return manifest
