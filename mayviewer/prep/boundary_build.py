"""Bake matched boundary polygons into the cache (optional overlay).

This is the *opt-in* half of Phase 3.5. The read-only ``mayviewer match``
diagnostic and the core viewer stay dependency-free; only this bake step needs
the optional ``[geo]`` extra (shapely) — robust per-tile polygon clipping in
pure Python is error-prone, so we require shapely here and fail with a clear
message if it is absent (same spirit as the pyshp/TopoJSON guards).

Nothing about which file/property/strategy a level uses is inferred: it all
comes from the explicit JSON config (:func:`boundaries.load_boundary_config`).
The streaming feature reader is size- and format-agnostic and identical for
every level — there is deliberately no "is this the big file?" branch, because
which level is huge is world-specific (the dev world's OA file is 2.7 GB; a
London build could just as easily have a giant MGU file).

Outputs, all behind "boundaries were supplied":
  * ``boundaries.pmtiles`` — one contiguous zoom pyramid, a layer per level
  * per-unit ``lat/lon/bbox`` (added to ``aggregates/level_*.parquet``)
  * a stored per-level match report (into ``manifest.json``)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from . import pmtiles
from .boundaries import LevelBoundaryCfg, iter_features, read_geojson_crs

logger = logging.getLogger("mayviewer.prep.boundaries")

# Web-mercator zoom envelope the per-level bands are spread across. Coarsest
# world level bakes at Z_MIN, the leaf at Z_MAX; MapLibre overzooms vector
# tiles losslessly up to Z_OVERZOOM so panning/zooming only ever fetches the
# viewport's tiles (the user's "load/unload, don't explode memory" need).
Z_MIN, Z_MAX, Z_OVERZOOM = 3, 12, 22


@dataclass
class LevelBoundaryResult:
    level_name: str
    level_value: int
    world_units: int
    matched: int
    strategy: str
    prop: str
    file: str
    src_crs: str | None
    bake_zoom: int
    minzoom: int
    maxzoom: int
    # geo_id -> (lon, lat, minlon, minlat, maxlon, maxlat)
    geo_extent: dict[int, tuple] = field(default_factory=dict)

    @property
    def rate(self) -> float:
        return self.matched / self.world_units if self.world_units else 0.0


def _is_lonlat(crs: str | None) -> bool:
    """True when the CRS is already WGS84 lon/lat (GeoJSON's implicit default)."""
    if not crs:
        return True
    s = crs.upper()
    return "CRS84" in s or s.endswith(":4326") or s.endswith("::4326")


def _reprojector(crs: str | None):
    """A shapely-geometry transform fn into EPSG:4326, or None if not needed.
    Boundary files are reprojected from whatever CRS they declare — never
    assumed to be lon/lat (the dev world's LAD file is EPSG:27700 metres)."""
    if _is_lonlat(crs):
        return None
    try:
        from pyproj import Transformer
        from shapely.ops import transform as shp_transform
    except ImportError:
        raise SystemExit(
            f"Boundary file is in CRS '{crs}' and needs pyproj to reproject:"
            '  pip install -e ".[geo]"'
        )
    tr = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
    return lambda g: shp_transform(lambda x, y, z=None: tr.transform(x, y), g)


def _zoom_bands(world_levels_coarse_to_fine: list[int]) -> dict[int, tuple[int, int, int]]:
    """level_value -> (bake_zoom, minzoom, maxzoom), contiguous across *all*
    world levels so the spacing is stable even if some levels lack shapes."""
    n = len(world_levels_coarse_to_fine)
    bands: dict[int, tuple[int, int, int]] = {}
    for r, lv in enumerate(world_levels_coarse_to_fine):
        bake = Z_MIN if n <= 1 else round(Z_MIN + (Z_MAX - Z_MIN) * r / (n - 1))
        lo = 0 if r == 0 else bake
        if r < n - 1:
            nxt = round(Z_MIN + (Z_MAX - Z_MIN) * (r + 1) / (n - 1))
            hi = max(bake, nxt - 1)
        else:
            hi = Z_OVERZOOM
        bands[lv] = (bake, lo, hi)
    return bands


def build_boundaries(schema, geo, config: dict[str, LevelBoundaryCfg],
                     out_dir: Path) -> tuple[dict, list[LevelBoundaryResult]]:
    """Stream + match + bake. Returns (pmtiles_artifact_dict, per-level results).
    Levels in the config that aren't in this world are reported and skipped;
    levels absent from the config simply get no polygons."""
    try:
        from shapely.geometry import shape
        from shapely.validation import make_valid
    except ImportError:
        raise SystemExit(
            "Baking --boundary-config needs shapely:  pip install -e \".[geo]\"\n"
            "(The `mayviewer match` diagnostic and the viewer itself do not.)"
        )

    # coarse -> fine: smaller MAY level value == coarser unit.
    coarse_to_fine = sorted(geo.level_values)
    bands = _zoom_bands(coarse_to_fine)

    results: list[LevelBoundaryResult] = []
    pm_levels: list[dict] = []

    for level_name, cfg in config.items():
        if level_name not in schema.geo_levels:
            logger.warning(
                "Boundary config level '%s' is not in this world's levels "
                "(%s) — skipped.", level_name, schema.geo_levels)
            continue
        lv = schema.geo_levels.index(level_name)
        if lv not in geo.level_values:
            logger.warning("Level '%s' has no units in this world — skipped.",
                            level_name)
            continue

        ids = geo.ids_at(lv)
        # Normalised world key -> geo_id (collisions: keep first, report rest).
        key_to_gid: dict[str, int] = {}
        collisions = 0
        for gid in ids.tolist():
            k = cfg.norm(geo.name_of(int(gid)))
            if not k:
                continue
            if k in key_to_gid:
                collisions += 1
                continue
            key_to_gid[k] = int(gid)
        if collisions:
            logger.warning("Level '%s': %d world units share a normalised "
                            "key under strategy '%s' (kept first each).",
                            level_name, collisions, cfg.strategy)

        src_crs = cfg.crs or read_geojson_crs(cfg.file)
        reproj = _reprojector(src_crs)
        if reproj:
            logger.info("  %s: reprojecting %s -> EPSG:4326",
                        level_name, src_crs)

        wanted = set(key_to_gid)
        geoms: dict[int, object] = {}
        for key, gj in iter_features(cfg.file, cfg.prop, cfg.norm, wanted):
            gid = key_to_gid.get(key)
            if gid is None:
                continue
            try:
                g = shape(gj)
                if reproj:
                    g = reproj(g)
            except Exception:  # malformed/unprojectable geometry — skip
                continue
            if not g.is_valid:
                g = make_valid(g)
            if g.is_empty:
                continue
            prev = geoms.get(gid)
            geoms[gid] = g if prev is None else prev.union(g)

        bake, lo, hi = bands[lv]
        geo_extent: dict[int, tuple] = {}
        feats: list[tuple] = []
        for gid, g in geoms.items():
            c = g.centroid
            mnx, mny, mxx, mxy = g.bounds
            geo_extent[gid] = (round(c.x, 6), round(c.y, 6),
                               round(mnx, 6), round(mny, 6),
                               round(mxx, 6), round(mxy, 6))
            feats.append((gid, lv, geo.name_of(gid), g))

        results.append(LevelBoundaryResult(
            level_name=level_name, level_value=lv,
            world_units=int(len(ids)), matched=len(geoms),
            strategy=cfg.strategy, prop=cfg.prop, file=str(cfg.file),
            src_crs=src_crs,
            bake_zoom=bake, minzoom=lo, maxzoom=hi, geo_extent=geo_extent,
        ))
        if feats:
            pm_levels.append({
                "name": level_name, "bake_zoom": bake,
                "minzoom": lo, "maxzoom": hi, "features": feats,
            })
        logger.info("  %s: matched %d/%d  (z%d, band %d-%d)",
                    level_name, len(geoms), len(ids), bake, lo, hi)

    if not pm_levels:
        return {}, results

    # Coarse level first so MapLibre draws fine levels on top.
    pm_levels.sort(key=lambda s: s["bake_zoom"])
    stats = pmtiles.write_boundary_pmtiles(pm_levels, out_dir / "boundaries.pmtiles")
    return stats, results


# Self-describing aggregate columns the frontend already parses by token; the
# polygon centroid drives map recenter/highlight, the bbox drives fitBounds.
EXTENT_COLS = ("geo:lon", "geo:lat", "geo:bbox_minlon", "geo:bbox_minlat",
               "geo:bbox_maxlon", "geo:bbox_maxlat")


def attach_extent_columns(table, level_value: int,
                          results: list[LevelBoundaryResult]):
    """Append lon/lat/bbox columns to a level's aggregate table (NULL where
    that unit had no matched polygon). No-op when this level got no shapes."""
    import pyarrow as pa

    res = next((r for r in results if r.level_value == level_value
                and r.geo_extent), None)
    if res is None:
        return table
    gids = table.column("geo_id").to_pylist()
    cols = {c: [] for c in EXTENT_COLS}
    for gid in gids:
        ext = res.geo_extent.get(int(gid))
        lon, lat, mnx, mny, mxx, mxy = ext if ext else (None,) * 6
        cols["geo:lon"].append(lon)
        cols["geo:lat"].append(lat)
        cols["geo:bbox_minlon"].append(mnx)
        cols["geo:bbox_minlat"].append(mny)
        cols["geo:bbox_maxlon"].append(mxx)
        cols["geo:bbox_maxlat"].append(mxy)
    for c in EXTENT_COLS:
        table = table.append_column(c, pa.array(cols[c], type=pa.float64()))
    return table


def report_payload(stats: dict, results: list[LevelBoundaryResult]) -> dict:
    """The `artifacts.boundaries` block stored in manifest.json."""
    return {
        **stats,
        "levels": [
            {
                "level": r.level_value, "level_name": r.level_name,
                "prop": r.prop, "strategy": r.strategy,
                "src_crs": r.src_crs or "EPSG:4326",
                "world_units": r.world_units, "matched": r.matched,
                "rate": round(r.rate, 4),
                "bake_zoom": r.bake_zoom,
                "minzoom": r.minzoom, "maxzoom": r.maxzoom,
                "file": Path(r.file).name,
            }
            for r in results
        ],
    }
