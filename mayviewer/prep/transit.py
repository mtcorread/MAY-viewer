"""Transit (train + tube) line prep — the data join and its artifacts.

Adds a Transit view to the viewer: commute train/tube **lines** drawn on the
map, the **riders** of each line (click → who rides it; intersect two lines for
shared ridership), and each rider's ordered **multi-leg journey**.

Buses are intentionally excluded: ``bus_pool_*`` venues are a single pooled hop
with no ``line_stops`` geometry.

Three sources:
  * the world ``.h5``      — which lines exist, who rides them, per-leg timings
  * ``line_stops.csv``     — line geometry (ordered stops; not in the h5)
  * ``coord_mgu.csv``      — MGU centroids to place each stop

Memory discipline (the pipeline's non-negotiable):
  * line venues are found by a bounded **span sweep** of ``venues`` (one span at
    a time), never a whole-array read;
  * a line venue's riders live entirely inside that venue's own geo-partition
    (verified), so riders are read from only the handful of partitions that host
    lines — never a scan of all ``venues/subsets`` rows;
  * ``membership_metadata`` (per-leg timings) is read in bounded chunks.

The transit *data model* is inherently small (a subset of the population rides
transit), so the collected dicts are bounded by ridership, not world size. The
one structure proportional to ridership held whole is the leg-chain map; on a
60M world that is still a small fraction of population — see ``collect_chains``.

Artifacts written next to the other cache files:
  * ``transit.pmtiles``          — line geometry (one ``lines`` vector layer)
  * ``transit_riders.parquet``   — 1 row group per line venue → its rider ids
  * ``transit_chains.parquet``   — 1 row group per home geo unit → ordered legs

Run standalone to eyeball the join without touching the cache::

    python -m mayviewer.prep.transit WORLD.h5 line_stops.csv coord_mgu.csv \
        --out-dir /tmp/transit_step1
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
from collections import defaultdict
from pathlib import Path

import numpy as np
import pyarrow as pa

from .personindex import home_unit_of

logger = logging.getLogger(__name__)

# Venue types that have real, drawable multi-leg route geometry. Buses excluded.
TRANSIT_TYPE_NAMES = ("train_line", "tube_line")

# Bound each bulk read. Matches the drill-down sweep cap so peak memory is one
# span, never a whole column. membership_metadata is read in its own chunks.
_SPAN_ROWS = 250_000
_MM_CHUNK = 1_000_000

_MM_GROUP = "activity_mappings/membership_metadata"


def _dec(v) -> str:
    return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)


# --------------------------------------------------------------------------- #
# Geometry from the two CSVs (small, national catalog — read whole).
# --------------------------------------------------------------------------- #
def _load_mgu_xy(coord_mgu_csv: str) -> dict[str, tuple[float, float]]:
    """MGU code -> (lon, lat) centroid, full national coverage."""
    mgu_xy: dict[str, tuple[float, float]] = {}
    with open(coord_mgu_csv, newline="") as fh:
        for row in csv.DictReader(fh):
            mgu_xy[row["MGU"]] = (float(row["longitude"]), float(row["latitude"]))
    return mgu_xy


def build_geometry(line_stops_csv: str, coord_mgu_csv: str,
                   keep: set[str] | None = None) -> dict[str, list[tuple[float, float, str]]]:
    """``line_id -> ordered [(lon, lat, station_name), ...]`` (>= 2 stops).

    ``line_stops.csv`` is a national catalog; pass ``keep`` (the set of in-world
    line_ids) to resolve only the lines this world actually has.
    """
    mgu_xy = _load_mgu_xy(coord_mgu_csv)
    raw: dict[str, list[tuple[int, str, str]]] = defaultdict(list)
    with open(line_stops_csv, newline="") as fh:
        for row in csv.DictReader(fh):
            lid = row["line_id"]
            if keep is not None and lid not in keep:
                continue
            raw[lid].append((int(row["position"]), row["node_mgu"], row["name"]))
    geometry: dict[str, list[tuple[float, float, str]]] = {}
    for line_id, lst in raw.items():
        lst.sort()
        pts = [(*mgu_xy[m], nm) for _pos, m, nm in lst if m in mgu_xy]
        if len(pts) >= 2:
            geometry[line_id] = pts
    return geometry


# --------------------------------------------------------------------------- #
# World joins (reader-based, memory-flat).
# --------------------------------------------------------------------------- #
def transit_type_codes(schema) -> dict[int, str]:
    """Venue-type code -> mode ("train"/"tube"). Read from the registry, never
    hardcoded — type indices are world-specific."""
    return {code: name.replace("_line", "")
            for code, name in enumerate(schema.venue_types)
            if name in TRANSIT_TYPE_NAMES}


def collect_line_venues(reader, schema) -> dict[int, dict]:
    """``venue_id -> {line_id, mode, geo_unit}`` for every train/tube line venue.

    Span sweep over ``venues``: each span reads only its slice of types/ids/
    geo_unit_ids and the parallel name block, keeping the line rows. The result
    is bounded by the number of lines (hundreds), not the venue count.
    """
    mode_by_code = transit_type_codes(schema)
    if not mode_by_code:
        return {}
    codes = np.fromiter(mode_by_code, dtype=np.int64)
    names_ds = reader.file["metadata/names/venues"]  # row-aligned with venues/ids
    out: dict[int, dict] = {}
    for span in reader.partition("venues").spans(_SPAN_ROWS):
        n = span.count
        tys = reader.slice("venues/types", span.start, n).astype(np.int64)
        sel = np.where(np.isin(tys, codes))[0]
        if not len(sel):
            continue
        ids = reader.slice("venues/ids", span.start, n).astype(np.int64)
        geo = reader.slice("venues/geo_unit_ids", span.start, n).astype(np.int64)
        nm = names_ds[span.start:span.start + n]
        for k in sel:
            out[int(ids[k])] = {
                "line_id": _dec(nm[k]),
                "mode": mode_by_code[int(tys[k])],
                "geo_unit": int(geo[k]),
            }
    return out


def collect_riders(reader, line_venues: dict[int, dict]) -> dict[int, list[int]]:
    """``venue_id -> [person_id]`` riders, read only from the partitions that
    host line venues (a line's rider subset lives in the venue's own
    geo-partition — verified), so this never scans all ``venues/subsets``."""
    by_geo: dict[int, list[int]] = defaultdict(list)
    for vid, m in line_venues.items():
        by_geo[m["geo_unit"]].append(vid)
    part = reader.partition("subsets")
    riders: dict[int, list[int]] = {vid: [] for vid in line_venues}
    for g, vids in by_geo.items():
        b = part.bounds(g)
        if b is None:
            continue
        lo, c = b
        sv = reader.slice("venues/subsets/venue_ids", lo, c).astype(np.int64)
        soff = reader.slice("venues/subsets/members_offsets", lo, c).astype(np.int64)
        scnt = reader.slice("venues/subsets/member_counts", lo, c).astype(np.int64)
        want = set(vids)
        for k in range(c):
            vid = int(sv[k])
            if vid not in want:
                continue
            cnt = int(scnt[k])
            if cnt:
                mem = reader.slice("venues/subsets/members_flat", int(soff[k]), cnt)
                riders[vid].extend(int(p) for p in mem)
    return riders


# Chain columns the writer owns; a metadata field with one of these names
# would collide with them and is dropped (with a warning) rather than shadowed.
_CHAIN_BASE_COLS = ("person_id", "home_geo_unit", "leg_idx", "venue_id",
                    "line_id", "mode")


def collect_chains(reader, line_vids: set[int],
                   ) -> tuple[list[str], dict[int, list[tuple[int, tuple]]]]:
    """``(fields, person_id -> [(venue_id, values), ...])`` for legs on a
    train/tube line, where ``fields`` is whatever ``membership_metadata``
    declares in its ``field_names`` registry (t_board/t_alight today; origin/
    dest/board/alight unit ids in worlds that record them) and ``values``
    holds that leg's fields in the same order. Legs are ordered by the
    recorded ``leg_idx`` when the world persists it (the authoritative route
    sequence), else by ``t_board_min`` — a fallback that can misorder
    interchange journeys, since those times are line-relative offsets, not
    journey clocks. Reads are bounded chunks; only transit legs are kept, so
    the held map is bounded by transit ridership (a fraction of a 60M
    population — acceptable; a true external sort would be the next step if a
    world's ridership ever dwarfs RAM)."""
    mm = reader.file[_MM_GROUP]
    declared = ([f.decode() if isinstance(f, bytes) else str(f)
                 for f in mm["field_names"][:]] if "field_names" in mm else [])
    fields = [f for f in declared if f in mm and f not in _CHAIN_BASE_COLS]
    shadowed = [f for f in declared if f in mm
                and f in _CHAIN_BASE_COLS and f != "leg_idx"]
    if shadowed:
        logger.warning("membership_metadata fields shadow chain columns, "
                       "dropped: %s", shadowed)
    sort_ds = ("leg_idx" if "leg_idx" in mm
               else "t_board_min" if "t_board_min" in mm else None)
    pid_ds, vid_ds = mm["person_ids"], mm["venue_ids"]
    n = pid_ds.shape[0]
    want = np.fromiter(line_vids, dtype=np.int64)
    keyed: dict[int, list[tuple[float, int, tuple]]] = defaultdict(list)
    for lo in range(0, n, _MM_CHUNK):
        hi = min(lo + _MM_CHUNK, n)
        v = vid_ds[lo:hi].astype(np.int64)
        keep = np.isin(v, want)
        if not keep.any():
            continue
        p = pid_ds[lo:hi][keep]
        vv = v[keep]
        sk = mm[sort_ds][lo:hi][keep] if sort_ds else None
        cols = [mm[f][lo:hi][keep] for f in fields]
        for j in range(len(p)):
            keyed[int(p[j])].append(
                (float(sk[j]) if sk is not None else 0.0, int(vv[j]),
                 tuple(float(c[j]) for c in cols)))
    chains: dict[int, list[tuple[int, tuple]]] = {}
    for pp, legs in keyed.items():
        legs.sort(key=lambda t: t[0])  # stable: equal keys keep file order
        chains[pp] = [(vid, vals) for _k, vid, vals in legs]
    return fields, chains


def build_transit(reader, schema, line_stops_csv: str, coord_mgu_csv: str) -> dict:
    """Join the world + geometry CSVs into the transit data model.

    Returns ``{line_venues, riders, chains, geometry, in_world_lines}`` where
    ``in_world_lines`` is the per-line render payload (geometry + venue/mode/
    rider_count) for the lines that exist in *this* world and resolve to
    geometry — the input to the PMTiles writer.
    """
    line_venues = collect_line_venues(reader, schema)
    if not line_venues:
        return {"line_venues": {}, "riders": {}, "chains": {},
                "chain_fields": [], "geometry": {}, "in_world_lines": []}

    riders = collect_riders(reader, line_venues)
    chain_fields, chains = collect_chains(reader, set(line_venues))

    world_line_ids = {m["line_id"] for m in line_venues.values()}
    geometry = build_geometry(line_stops_csv, coord_mgu_csv, keep=world_line_ids)

    # One venue per line_id in these worlds; keep the first if ever duplicated.
    vid_by_line: dict[str, int] = {}
    for vid, m in line_venues.items():
        vid_by_line.setdefault(m["line_id"], vid)

    in_world_lines = []
    for line_id, pts in geometry.items():
        vid = vid_by_line.get(line_id)
        if vid is None:
            continue
        in_world_lines.append({
            "line_id": line_id,
            "venue_id": vid,
            "mode": line_venues[vid]["mode"],
            "rider_count": len(riders.get(vid, [])),
            "coords": [(lon, lat) for lon, lat, _nm in pts],
            "stops": [nm for _lon, _lat, nm in pts],
        })

    return {
        "line_venues": line_venues,
        "riders": riders,
        "chains": chains,
        "chain_fields": chain_fields,
        "geometry": geometry,
        "in_world_lines": in_world_lines,
    }


# --------------------------------------------------------------------------- #
# Parquet shard writers (mirror drilldown: 1 row group per key, O(1) range read).
# --------------------------------------------------------------------------- #
_RIDERS_SCHEMA = pa.schema([
    ("venue_id", pa.int64()),
    ("person_id", pa.int64()),
    ("home_geo_unit", pa.int64()),  # which people shard resolves this rider
])



def write_rider_shards(riders: dict, line_venues: dict, out_dir: Path,
                       home_lut=None) -> tuple[str, dict[int, int]]:
    """``transit_riders.parquet``: one row group per line venue → its riders
    (with each rider's home geo unit so the frontend resolves them the same way
    the Inspector resolves venue members). Returns ``(filename, {venue_id: rg})``."""
    from .drilldown import _ShardWriter
    w = _ShardWriter(out_dir / "transit_riders.parquet", _RIDERS_SCHEMA)
    for vid in sorted(line_venues):
        pids = riders.get(vid)
        if not pids:
            continue
        arr = np.asarray(pids, np.int64)
        home = (home_unit_of(home_lut, arr) if home_lut is not None
                else np.full(len(arr), -1, np.int64))
        w.write_unit(vid, {
            "venue_id": np.full(len(arr), vid, np.int64),
            "person_id": arr,
            "home_geo_unit": home,
        })
    w.close()
    return w.path.name, w.index


def write_chain_shards(chains: dict, line_venues: dict, fields: list[str],
                       out_dir: Path, home_lut=None) -> tuple[str, dict[int, int]]:
    """``transit_chains.parquet``: one row group per *home* geo unit, each row a
    leg (ordered by ``leg_idx``). Keyed by home unit so selecting a rider in the
    panel fetches their journey with the same person→home-unit index the rest of
    the viewer uses. Besides the structural columns, every ``fields`` entry
    (whatever the world's membership_metadata declared) becomes a column — an
    integral one when all its values are whole (times, unit ids), float64
    otherwise. Returns ``(filename, {home_geo_unit: rg})``."""
    from .drilldown import _ShardWriter
    pids = np.fromiter(chains.keys(), np.int64, len(chains))
    homes = (home_unit_of(home_lut, pids) if home_lut is not None
             else np.full(len(pids), -1, np.int64))
    by_home: dict[int, list[int]] = defaultdict(list)
    for p, h in zip(pids, homes):
        by_home[int(h)].append(int(p))

    integral = [True] * len(fields)
    for legs in chains.values():
        for _vid, vals in legs:
            for i, x in enumerate(vals):
                if integral[i] and x != int(x):
                    integral[i] = False

    sch = pa.schema([
        ("person_id", pa.int64()),
        ("home_geo_unit", pa.int64()),
        ("leg_idx", pa.int32()),
        ("venue_id", pa.int64()),
        ("line_id", pa.string()),
        ("mode", pa.string()),
        *((f, pa.int64() if integral[i] else pa.float64())
          for i, f in enumerate(fields)),
    ])
    w = _ShardWriter(out_dir / "transit_chains.parquet", sch)
    for h in sorted(by_home):
        cols: dict[str, list] = {k.name: [] for k in sch}
        for p in by_home[h]:
            for i, (vid, vals) in enumerate(chains[p]):
                m = line_venues.get(vid, {})
                cols["person_id"].append(p)
                cols["home_geo_unit"].append(h)
                cols["leg_idx"].append(i)
                cols["venue_id"].append(vid)
                cols["line_id"].append(m.get("line_id", ""))
                cols["mode"].append(m.get("mode", ""))
                for fi, f in enumerate(fields):
                    cols[f].append(int(vals[fi]) if integral[fi] else vals[fi])
        w.write_unit(h, cols)
    w.close()
    return w.path.name, w.index


def report_payload(result: dict, pm_stats: dict, riders_file: str,
                   riders_idx: dict, chains_file: str, chains_idx: dict) -> dict:
    """The manifest ``artifacts.transit`` entry."""
    modes = [m["mode"] for m in result["line_venues"].values()]
    return {
        "lines": {"path": pm_stats["path"], "layer": "lines", **{
            k: pm_stats[k] for k in ("minzoom", "maxzoom", "bake_zooms",
                                     "tiles", "features", "bytes")}},
        "riders": {"path": riders_file, "row_groups": riders_idx},
        # ``fields`` mirrors the world's membership_metadata registry so the
        # frontend can discover per-leg columns without a schema read.
        "chains": {"path": chains_file, "row_groups": chains_idx,
                   "fields": result["chain_fields"]},
        "summary": {
            "lines": len(result["in_world_lines"]),
            "train": sum(1 for m in modes if m == "train"),
            "tube": sum(1 for m in modes if m == "tube"),
            "rider_memberships": sum(len(v) for v in result["riders"].values()),
            "riders_with_chains": len(result["chains"]),
        },
    }


# --------------------------------------------------------------------------- #
# Standalone proof-of-join: dump GeoJSON + a sample JSON and print a summary.
# --------------------------------------------------------------------------- #
def _lines_geojson(result: dict) -> dict:
    """FeatureCollection of train+tube LineStrings for lines in *this world*."""
    features = []
    for ln in sorted(result["in_world_lines"], key=lambda x: x["line_id"]):
        features.append({
            "type": "Feature",
            "properties": {
                "line_id": ln["line_id"],
                "venue_id": ln["venue_id"],
                "mode": ln["mode"],
                "stop_count": len(ln["coords"]),
                "rider_count": ln["rider_count"],
                "stops": ln["stops"],
            },
            "geometry": {
                "type": "LineString",
                "coordinates": [[lon, lat] for lon, lat in ln["coords"]],
            },
        })
    return {"type": "FeatureCollection", "features": features}


def _sample_json(result: dict, n_lines: int = 3,
                 sample_people: list[int] | None = None) -> dict:
    line_id_by_vid = {vid: m["line_id"] for vid, m in result["line_venues"].items()}
    mode_by_vid = {vid: m["mode"] for vid, m in result["line_venues"].items()}
    rideable = sorted(
        ((vid, result["riders"].get(vid, [])) for vid in line_id_by_vid),
        key=lambda kv: len(kv[1]), reverse=True)

    sample_lines = []
    chosen: list[int] = list(sample_people or [])
    for vid, rid in rideable[:n_lines]:
        if not rid:
            continue
        sample_lines.append({
            "venue_id": vid, "line_id": line_id_by_vid[vid],
            "mode": mode_by_vid[vid], "rider_count": len(rid),
            "rider_sample": rid[:10],
        })
        if len(chosen) < 3:
            for p in rid:
                if len(result["chains"].get(p, [])) >= 2 and p not in chosen:
                    chosen.append(p)
                    break

    def resolve(p: int):
        fields = result["chain_fields"]
        legs = [{"venue_id": v, "line_id": line_id_by_vid.get(v),
                 "mode": mode_by_vid.get(v),
                 **dict(zip(fields, vals))}
                for v, vals in result["chains"].get(p, [])]
        return {"person_id": p, "n_legs": len(legs), "legs": legs}

    return {"sample_lines": sample_lines,
            "rider_chains": [resolve(p) for p in chosen]}


def main() -> None:
    from ..schema import describe
    from .reader import WorldReader

    ap = argparse.ArgumentParser(description="Transit join proof-of-concept (Step 1).")
    ap.add_argument("h5_path", help="commute world .h5")
    ap.add_argument("line_stops_csv", help="line_stops.csv (line geometry)")
    ap.add_argument("coord_mgu_csv", help="coord_mgu.csv (MGU centroids)")
    ap.add_argument("--out-dir", default="/tmp/transit_step1")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    schema = describe(args.h5_path)
    with WorldReader(args.h5_path) as r:
        result = build_transit(r, schema, args.line_stops_csv, args.coord_mgu_csv)

    n_train = sum(1 for m in result["line_venues"].values() if m["mode"] == "train")
    n_tube = sum(1 for m in result["line_venues"].values() if m["mode"] == "tube")
    geojson = _lines_geojson(result)
    sample = _sample_json(
        result, sample_people=[611842] if 611842 in result["chains"] else None)

    geojson_path = os.path.join(args.out_dir, "transit_lines.geojson")
    sample_path = os.path.join(args.out_dir, "transit_sample.json")
    with open(geojson_path, "w") as fh:
        json.dump(geojson, fh)
    with open(sample_path, "w") as fh:
        json.dump(sample, fh, indent=2)

    print("=== Transit join summary ===")
    print(f"line venues:        {len(result['line_venues'])}  (train={n_train}, tube={n_tube})")
    print(f"in-world lines w/ geometry: {len(result['in_world_lines'])}")
    print(f"rider memberships:  {sum(len(v) for v in result['riders'].values())}")
    print(f"riders w/ chains:   {len(result['chains'])}")
    if geojson["features"]:
        f0 = geojson["features"][0]
        print("\n--- sample line ---")
        print(f"line_id={f0['properties']['line_id']} mode={f0['properties']['mode']} "
              f"stops={f0['properties']['stop_count']} riders={f0['properties']['rider_count']}")
        print(f"first 3 coords: {f0['geometry']['coordinates'][:3]}")
    if sample["rider_chains"]:
        rc = sample["rider_chains"][0]
        print("\n--- sample rider chain ---")
        print(f"person {rc['person_id']} — {rc['n_legs']} legs:")
        for leg in rc["legs"]:
            print(f"  board {leg['t_board_min']:>4}  alight {leg['t_alight_min']:>4}  "
                  f"{leg['mode']:5} {leg['line_id']}")
    print(f"\nwrote {geojson_path}\nwrote {sample_path}")


if __name__ == "__main__":
    main()
