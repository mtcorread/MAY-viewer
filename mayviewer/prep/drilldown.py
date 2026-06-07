"""Per-geo-unit drill-down shards — the bounded inspector payloads.

Each artifact (people, venues, members) is written as a single Parquet file
with **one row group per geo unit**, in the file's own partition order. The
pipeline records ``geo_unit_id -> row_group`` in the manifest, so the
server/frontend answers "give me everything in this unit" with one O(1)
row-group read — never a scan, never the whole file. Memory stays flat because
each unit is built, written, and released before the next.

Shards are keyed by each container's *actual* partition geo unit, which may sit
at any level (households at the leaf, workplaces possibly coarser) — nothing
here assumes the leaf level or any venue type.

Friendships are consumed through the schema, not by name: every typed-CSR
``person_relation`` and every JSON-array-valued person property becomes a
``list`` column, so the real world's JSON-string friendships and the synth's
typed-CSR friendships both round-trip without a hardcoded term.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .personindex import home_unit_of
from .reader import Span


def _json_scalar(v):
    """numpy scalar -> native Python; NaN -> None (JSON has no NaN)."""
    if isinstance(v, np.generic):
        v = v.item()
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def unit_records(cols: dict) -> list[dict]:
    """Transpose a per-unit column dict (the ``*_unit_cols`` output) into
    JSON-serializable row records for the lazy-serve endpoint."""
    keys = list(cols)
    if not keys:
        return []
    n = len(cols[keys[0]])
    out = []
    for i in range(n):
        rec = {}
        for k in keys:
            v = cols[k][i]
            if isinstance(v, (list, np.ndarray)):
                rec[k] = [_json_scalar(x) for x in v]
            else:
                rec[k] = _json_scalar(v)
        out.append(rec)
    return out

_SAMPLE = 4000

# Geo units are read in coalesced spans (see ``Partition.spans``). This caps the
# rows held in memory at one time.
_SPAN_ROWS = 250_000


def _dec(v) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)


def _classify(reader, schema) -> tuple[list[str], dict[str, str]]:
    """Split person properties into scalar strings and JSON-array columns.

    Returns ``(scalar, jsonish)`` where ``jsonish`` maps a property to its
    element kind ``"int"`` or ``"str"`` (detected from a sample), so an
    int-array (e.g. friendships/partner ids) keeps integer typing while a
    string-array (e.g. comorbidities) is preserved losslessly as strings.
    """
    scalar: list[str] = []
    jsonish: dict[str, str] = {}
    for prop in schema.person_properties:
        if prop in schema.person_relations:
            continue
        path = f"population/properties/{prop}"
        if path not in reader:
            continue
        sample = [_dec(v).strip() for v in reader.slice(path, 0, _SAMPLE)]
        if not any(v[:1] == "[" for v in sample):
            scalar.append(prop)
            continue
        all_int = True
        for v in sample:
            if not v or v == "[]":
                continue
            try:
                elems = json.loads(v)
            except (ValueError, TypeError):
                elems = []
            if any(not isinstance(e, int) for e in elems):
                all_int = False
                break
        jsonish[prop] = "int" if all_int else "str"
    return scalar, jsonish


def _to_list(s: str, kind: str):
    s = s.strip()
    if not s or s == "[]":
        return []
    try:
        elems = json.loads(s)
    except (ValueError, TypeError):
        return []
    return [int(x) for x in elems] if kind == "int" else [str(x) for x in elems]


class _ShardWriter:
    """One Parquet file; one row group per ``write_unit`` call."""

    def __init__(self, path: Path, schema: pa.Schema):
        self.path = path
        self._w = pq.ParquetWriter(path, schema)
        self._schema = schema
        self.index: dict[int, int] = {}  # geo_unit_id -> row group ordinal
        self._rg = 0

    def write_unit(self, geo_unit_id: int, cols: dict) -> None:
        tbl = pa.table(cols, schema=self._schema)
        if tbl.num_rows == 0:
            return
        # One write_table == one row group (row_group_size kept above any
        # single unit so a unit is never split across groups).
        self._w.write_table(tbl, row_group_size=1 << 30)
        self.index[int(geo_unit_id)] = self._rg
        self._rg += 1

    def close(self) -> None:
        self._w.close()


def _friend_lists(reader, schema, s: int, c: int) -> dict[str, list[list[int]]]:
    """Edge lists for a population slice, per typed-CSR relation."""
    out: dict[str, list[list[int]]] = {}
    for rel in schema.person_relations:
        base = f"population/{rel}"
        off = reader.slice(f"{base}/offsets", s, c).astype(np.int64)
        cnt = reader.slice(f"{base}/counts", s, c).astype(np.int64)
        if len(off) == 0:
            out[rel] = []
            continue
        lo, hi = int(off[0]), int(off[-1] + cnt[-1])
        flat = reader.slice(f"{base}/flat", lo, hi - lo)
        out[rel] = [
            flat[int(o) - lo: int(o) - lo + int(n)].tolist()
            for o, n in zip(off, cnt)
        ]
    return out


def people_ctx(reader, schema) -> dict:
    """Per-build constant state for people shards: property classification, the
    sex code→label lookup, and the Arrow schema."""
    scalar, jsonish = _classify(reader, schema)
    code_to_sex = {v: k for k, v in schema.sex_mapping.items()}
    # Vectorized code->label via a small lookup array (codes are 0..N small).
    sex_lut = (np.array([code_to_sex.get(i, str(i))
                         for i in range(max(code_to_sex) + 1)], dtype=object)
               if code_to_sex else np.empty(0, dtype=object))
    fields = [
        ("person_id", pa.int64()),
        ("geo_unit_id", pa.int64()),
        ("age", pa.float32()),
        ("sex", pa.string()),
    ]
    fields += [(p, pa.string()) for p in scalar]
    fields += [(p, pa.list_(pa.int64() if k == "int" else pa.string()))
               for p, k in jsonish.items()]
    fields += [(r, pa.list_(pa.int64())) for r in schema.person_relations]
    return {"scalar": scalar, "jsonish": jsonish, "sex_lut": sex_lut,
            "schema": pa.schema(fields)}


def _people_span(reader, schema, ctx, span: Span):
    """Yield ``(geo_unit_id, cols)`` for every unit in ``span``: read each
    dataset once for the whole span, decode/parse once, then slice per unit."""
    scalar, jsonish, sex_lut = ctx["scalar"], ctx["jsonish"], ctx["sex_lut"]
    n = span.count
    ids = reader.slice("population/ids", span.start, n).astype(np.int64)
    ages = reader.slice("population/ages", span.start, n)
    sx = reader.slice("population/sexes", span.start, n).astype(np.int64)
    in_lut = (sx >= 0) & (sx < len(sex_lut))
    sex_all = np.empty(n, dtype=object)
    if in_lut.any():
        sex_all[in_lut] = sex_lut[sx[in_lut]]
    if not in_lut.all():
        sex_all[~in_lut] = sx[~in_lut].astype(str)
    scal_all = {p: [_dec(v) for v in
                    reader.slice(f"population/properties/{p}", span.start, n)]
                for p in scalar}
    json_all = {p: [_to_list(_dec(v), k) for v in
                    reader.slice(f"population/properties/{p}", span.start, n)]
                for p, k in jsonish.items()}
    rel_all = _friend_lists(reader, schema, span.start, n)

    for gid, lo, cnt in span.units:
        hi = lo + cnt
        cols: dict = {
            "person_id": ids[lo:hi],
            "geo_unit_id": np.full(cnt, gid, np.int64),
            "age": ages[lo:hi],
            "sex": sex_all[lo:hi],
        }
        for p in scalar:
            cols[p] = scal_all[p][lo:hi]
        for p in jsonish:
            cols[p] = json_all[p][lo:hi]
        for rel in schema.person_relations:
            cols[rel] = rel_all[rel][lo:hi]
        yield gid, cols


def _single_span(reader, container: str, gid: int) -> Span | None:
    """A one-unit :class:`Span` for an O(1) per-unit (lazy-serve) read."""
    b = reader.partition(container).bounds(gid)
    if b is None or b[1] == 0:
        return None
    return Span(b[0], b[1], [(int(gid), 0, b[1])])


def people_unit_cols(reader, schema, ctx, gid: int) -> dict | None:
    """One geo unit's people columns (for lazy serve), or None if empty."""
    sp = _single_span(reader, "population", gid)
    if sp is None:
        return None
    for _, cols in _people_span(reader, schema, ctx, sp):
        return cols
    return None


def people_by_rows(reader, schema, ctx, rows) -> dict | None:
    """People columns for an arbitrary set of population rows (lazy by-id
    resolution). ``rows`` are absolute array indices; deduped and sorted here so
    the h5py fancy reads stay valid (increasing order). Same column shape as a
    per-unit people shard, so resolved members and the person panel render
    identically to in-unit people. Returns None if no valid rows."""
    rows = np.unique(np.asarray(rows, np.int64))
    rows = rows[rows >= 0]
    if len(rows) == 0:
        return None
    scalar, jsonish, sex_lut = ctx["scalar"], ctx["jsonish"], ctx["sex_lut"]
    f = reader.file
    ids = f["population/ids"][rows].astype(np.int64)
    ages = f["population/ages"][rows]
    sx = f["population/sexes"][rows].astype(np.int64)
    geo = f["population/geo_unit_ids"][rows].astype(np.int64)
    in_lut = (sx >= 0) & (sx < len(sex_lut))
    sex_all = np.empty(len(rows), dtype=object)
    if in_lut.any():
        sex_all[in_lut] = sex_lut[sx[in_lut]]
    if not in_lut.all():
        sex_all[~in_lut] = sx[~in_lut].astype(str)
    cols: dict = {
        "person_id": ids,
        "geo_unit_id": geo,
        "age": ages,
        "sex": sex_all,
    }
    for p in scalar:
        cols[p] = [_dec(v) for v in f[f"population/properties/{p}"][rows]]
    for p, k in jsonish.items():
        cols[p] = [_to_list(_dec(v), k) for v in f[f"population/properties/{p}"][rows]]
    for rel in schema.person_relations:
        base = f"population/{rel}"
        off = f[f"{base}/offsets"][rows].astype(np.int64)
        cnt = f[f"{base}/counts"][rows].astype(np.int64)
        flat = f[f"{base}/flat"]
        cols[rel] = [flat[int(o):int(o) + int(c)].tolist() if c else []
                     for o, c in zip(off, cnt)]
    return cols


def write_people(reader, schema, out_dir: Path) -> tuple[str, dict[int, int]]:
    ctx = people_ctx(reader, schema)
    w = _ShardWriter(out_dir / "people.parquet", ctx["schema"])
    for span in reader.partition("population").spans(_SPAN_ROWS):
        for gid, cols in _people_span(reader, schema, ctx, span):
            w.write_unit(gid, cols)
    w.close()
    return w.path.name, w.index


def venues_ctx(reader, schema) -> dict:
    """Per-build constant state for venue shards (schema, type lookup, which
    optional columns exist)."""
    vtypes = schema.venue_types
    vprops = schema.venue_properties  # {type: [prop, ...]}
    prop_cols = sorted({f"{t}.{p}" for t, ps in vprops.items() for p in ps})
    fields = [
        ("venue_id", pa.int64()),
        ("geo_unit_id", pa.int64()),
        ("type", pa.string()),
        ("name", pa.string()),
        ("parent_id", pa.int64()),
        ("is_residence", pa.bool_()),
        ("lat", pa.float32()),
        ("lon", pa.float32()),
        ("rank_in_type", pa.int64()),
    ]
    fields += [(c, pa.string()) for c in prop_cols]
    return {
        "names": reader.names("venues"),
        "vtypes": vtypes, "vprops": vprops, "prop_cols": prop_cols,
        # Older / non-georeferenced worlds may lack is_residence and lat/lon.
        "has_isres": "venues/is_residence" in reader,
        "has_vlat": "venues/latitudes" in reader,
        "has_vlon": "venues/longitudes" in reader,
        "vt_arr": np.array([*vtypes, ""], dtype=object),  # trailing "" = oor
        "n_vt": len(vtypes),
        "schema": pa.schema(fields),
    }


def _venues_span(reader, schema, ctx, span: Span, vmeta=None):
    """Yield ``(geo_unit_id, cols)`` for a venue span; when ``vmeta`` is a dict,
    also fill ``{venue_id: (geo_unit_id, type, name)}`` for the span."""
    names, vtypes, vprops = ctx["names"], ctx["vtypes"], ctx["vprops"]
    prop_cols, vt_arr, n_vt = ctx["prop_cols"], ctx["vt_arr"], ctx["n_vt"]
    has_isres, has_vlat, has_vlon = ctx["has_isres"], ctx["has_vlat"], ctx["has_vlon"]
    n = span.count
    vids_all = reader.slice("venues/ids", span.start, n).astype(np.int64)
    tys_i = reader.slice("venues/types", span.start, n).astype(np.int64)
    ranks_all = reader.slice("venues/ranks_in_type", span.start, n).astype(np.int64)
    parent_all = reader.slice("venues/parent_ids", span.start, n).astype(np.int64)
    isres_all = (reader.slice("venues/is_residence", span.start, n)
                 if has_isres else None)
    lat_all = (reader.slice("venues/latitudes", span.start, n)
               if has_vlat else np.full(n, np.nan, np.float32))
    lon_all = (reader.slice("venues/longitudes", span.start, n)
               if has_vlon else np.full(n, np.nan, np.float32))

    toor = (tys_i < 0) | (tys_i >= n_vt)
    type_all = vt_arr[np.where(toor, n_vt, tys_i)]
    if toor.any():
        type_all[toor] = tys_i[toor].astype(str)
    if names is not None:
        name_all = [_dec(x) for x in names[span.start:span.start + n]]
    else:
        name_all = [str(v) for v in vids_all]

    # Per-type properties (rank_in_type indexed): one sorted fancy read per
    # (type, prop) for the whole span, scattered back to span positions.
    prop_span = {col: [None] * n for col in prop_cols}
    for ti, tname in enumerate(vtypes):
        sel = np.where(tys_i == ti)[0]
        if len(sel) == 0:
            continue
        for prop in vprops.get(tname, []):
            ds = f"venues/properties/{tname}/{prop}"
            if ds not in reader:
                continue
            rk = ranks_all[sel]
            order = np.argsort(rk)
            vals = reader.file[ds][rk[order]]
            target = prop_span[f"{tname}.{prop}"]
            for k, oi in enumerate(order):
                target[int(sel[oi])] = _dec(vals[k])

    if vmeta is not None:
        gid_all = np.repeat(
            np.fromiter((u[0] for u in span.units), np.int64, len(span.units)),
            np.fromiter((u[2] for u in span.units), np.int64, len(span.units)))
        for i in range(n):
            vmeta[int(vids_all[i])] = (int(gid_all[i]), type_all[i], name_all[i])

    for gid, lo, cnt in span.units:
        hi = lo + cnt
        cols: dict = {
            "venue_id": vids_all[lo:hi],
            "geo_unit_id": np.full(cnt, gid, np.int64),
            "type": type_all[lo:hi],
            "name": name_all[lo:hi],
            "parent_id": parent_all[lo:hi],
            "is_residence": (isres_all[lo:hi] if has_isres else [None] * cnt),
            "lat": lat_all[lo:hi],
            "lon": lon_all[lo:hi],
            "rank_in_type": ranks_all[lo:hi],
        }
        for col in prop_cols:
            cols[col] = prop_span[col][lo:hi]
        yield gid, cols


def venues_unit_cols(reader, schema, ctx, gid: int) -> dict | None:
    """One geo unit's venue columns (for lazy serve), or None if empty."""
    sp = _single_span(reader, "venues", gid)
    if sp is None:
        return None
    for _, cols in _venues_span(reader, schema, ctx, sp):
        return cols
    return None


def venue_index(reader, schema) -> dict[int, tuple]:
    """Build ``{venue_id: (geo_unit_id, type, name)}`` by sweeping venues once.
    The venue resolver for activity rows in lazy serve. Bounded by venue count."""
    ctx = venues_ctx(reader, schema)
    vmeta: dict[int, tuple] = {}
    for span in reader.partition("venues").spans(_SPAN_ROWS):
        for _ in _venues_span(reader, schema, ctx, span, vmeta):
            pass
    return vmeta


def write_venues(
    reader, schema, out_dir: Path, want_index: bool = False
) -> tuple[str, dict[int, int], dict[int, tuple] | None]:
    """Write venues.parquet. When ``want_index`` (only set if the world has an
    activity map to resolve), also return ``{venue_id: (geo_unit_id, type,
    name)}`` so activity rows can name and locate each venue without a second
    pass. The index is bounded by venue count — the same scale already held in
    the parquet — and is skipped entirely when no activities reference it."""
    ctx = venues_ctx(reader, schema)
    vmeta: dict[int, tuple] | None = {} if want_index else None
    w = _ShardWriter(out_dir / "venues.parquet", ctx["schema"])
    for span in reader.partition("venues").spans(_SPAN_ROWS):
        for gid, cols in _venues_span(reader, schema, ctx, span, vmeta):
            w.write_unit(gid, cols)
    w.close()
    return w.path.name, w.index, vmeta


def members_ctx(reader, schema, home_lut=None) -> dict:
    # Authoritative per-subset names. ``metadata/names/subsets`` is a parallel
    # array aligned 1:1 with the subset metadata arrays (same geo-unit sort,
    # same partition index), holding each subset's real name ("Adults",
    # "teachers", …). The ``subset_indices`` are positions *within a venue*
    # (0,1,2…), NOT registry slots, so indexing the global subset_names
    # registry by them mislabels every subset. Use the names array; fall back
    # to the registry-by-index only if a world lacks the names dataset.
    #
    # ``home_lut`` (person_id -> home geo unit, see :mod:`personindex`) lets each
    # member carry the unit whose people shard holds its attribute row, so the
    # frontend can resolve members who live outside the inspected unit. None on
    # worlds prepped before the index existed → ``home_geo_unit`` is -1.
    return {
        "snames": schema.subset_names,
        "has_names": "metadata/names/subsets" in reader,
        "home_lut": home_lut,
        "schema": pa.schema([
            ("venue_id", pa.int64()),
            ("geo_unit_id", pa.int64()),
            ("subset", pa.string()),
            ("person_id", pa.int64()),
            ("home_geo_unit", pa.int64()),
        ]),
    }


def _members_span(reader, schema, ctx, span: Span):
    """Yield ``(geo_unit_id, cols)`` for a subsets span, expanding each subset's
    members from one ``members_flat`` read."""
    snames, has_names, home_lut = ctx["snames"], ctx["has_names"], ctx["home_lut"]
    s_vid = reader.slice("venues/subsets/venue_ids", span.start, span.count).astype(np.int64)
    s_sub = reader.slice("venues/subsets/subset_indices", span.start, span.count)
    s_off = reader.slice("venues/subsets/members_offsets", span.start, span.count).astype(np.int64)
    s_cnt = reader.slice("venues/subsets/member_counts", span.start, span.count).astype(np.int64)
    s_name = (reader.slice("metadata/names/subsets", span.start, span.count)
              if has_names else None)

    # One members_flat read covering every subset in the span. ``members`` is a
    # second CSR level under subsets; offsets are absolute, so the span block is
    # sliced at ``offset - span_lo``.
    nz = s_cnt > 0
    if not nz.any():
        return
    span_lo = int(s_off[nz].min())
    span_hi = int((s_off[nz] + s_cnt[nz]).max())
    blk = reader.slice("venues/subsets/members_flat", span_lo, span_hi - span_lo)

    for gid, lo, ucnt in span.units:
        uhi = lo + ucnt
        u_vid, u_sub = s_vid[lo:uhi], s_sub[lo:uhi]
        u_off, u_cnt = s_off[lo:uhi], s_cnt[lo:uhi]
        total = int(u_cnt.sum())
        if total == 0:
            continue
        # One label per subset in the unit.
        if s_name is not None:
            labels = np.array([_dec(x) for x in s_name[lo:uhi]], dtype=object)
        else:
            labels = np.array(
                [snames[int(si)] if 0 <= int(si) < len(snames) else str(int(si))
                 for si in u_sub], dtype=object)
        # Expand each subset's [offset, offset+count) members in one gather.
        seg_start = np.repeat(u_off - span_lo, u_cnt)
        within = np.arange(total) - np.repeat(np.cumsum(u_cnt) - u_cnt, u_cnt)
        pids = blk[seg_start + within].astype(np.int64)
        # Home unit per member = the unit whose people shard holds the attribute
        # row. Members living in the inspected unit resolve there already; this
        # points the frontend at the right shard for everyone else.
        home = (home_unit_of(home_lut, pids) if home_lut is not None
                else np.full(total, -1, np.int64))
        yield gid, {
            "venue_id": np.repeat(u_vid, u_cnt),
            "geo_unit_id": np.full(total, gid, np.int64),
            "subset": np.repeat(labels, u_cnt),
            "person_id": pids,
            "home_geo_unit": home,
        }


def members_unit_cols(reader, schema, ctx, gid: int) -> dict | None:
    """One geo unit's members columns (for lazy serve), or None if empty."""
    sp = _single_span(reader, "subsets", gid)
    if sp is None:
        return None
    for _, cols in _members_span(reader, schema, ctx, sp):
        return cols
    return None


def write_members(reader, schema, out_dir: Path,
                  home_lut=None) -> tuple[str, dict[int, int]]:
    ctx = members_ctx(reader, schema, home_lut)
    w = _ShardWriter(out_dir / "members.parquet", ctx["schema"])
    for span in reader.partition("subsets").spans(_SPAN_ROWS):
        for gid, cols in _members_span(reader, schema, ctx, span):
            w.write_unit(gid, cols)
    w.close()
    return w.path.name, w.index


def activities_ctx(reader, schema, vmeta: dict[int, tuple] | None) -> dict:
    """Per-build state for activity shards. ``vmeta`` (``{venue_id:
    (geo_unit_id, type, name)}``) is flattened into parallel sorted arrays for
    venue resolution by ``np.searchsorted``."""
    anames = schema.activity_names
    sch = pa.schema([
        ("person_id", pa.int64()),
        ("geo_unit_id", pa.int64()),
        ("activity", pa.string()),
        ("venue_id", pa.int64()),
        ("venue_geo_unit_id", pa.int64()),
        ("venue_type", pa.string()),
        ("venue_name", pa.string()),
    ])
    ctx = {
        "n_act": len(anames),
        # Trailing "" slot is the out-of-range fallback (str(idx) below).
        "anames_arr": np.array([*anames, ""], dtype=object),
        "schema": sch,
    }
    if vmeta:
        keys = np.fromiter(vmeta.keys(), np.int64, len(vmeta))
        order = np.argsort(keys)
        vvals = list(vmeta.values())
        ctx["vid_sorted"] = keys[order]
        ctx["meta_gid"] = np.fromiter((m[0] for m in vvals), np.int64, len(vvals))[order]
        ctx["meta_type"] = np.array([m[1] for m in vvals], dtype=object)[order]
        ctx["meta_name"] = np.array([m[2] for m in vvals], dtype=object)[order]
    else:
        ctx["vid_sorted"] = np.empty(0, np.int64)
    return ctx


_ACT_DS = "activity_mappings/activity_map/activity_data"
_ACT_OFFSETS = "activity_mappings/activity_map/activity_offsets"


def _resolve_acts(ctx, aidx: np.ndarray, vids: np.ndarray):
    """Activity-name + venue (gid/type/name) for activity rows, via ``vmeta``."""
    n_act, anames_arr, vid_sorted = ctx["n_act"], ctx["anames_arr"], ctx["vid_sorted"]
    oor = (aidx < 0) | (aidx >= n_act)
    acts = anames_arr[np.where(oor, n_act, aidx)]
    if oor.any():  # out-of-range activity index -> its str(index)
        acts[oor] = aidx[oor].astype(str)
    if len(vid_sorted):
        pos = np.clip(np.searchsorted(vid_sorted, vids), 0, len(vid_sorted) - 1)
        hit = vid_sorted[pos] == vids
        v_gid = np.where(hit, ctx["meta_gid"][pos], -1)
        v_type = np.where(hit, ctx["meta_type"][pos], "")
        v_name = np.where(hit, ctx["meta_name"][pos], "")
    else:  # no venue index — every row is a miss (sentinel/no venue)
        v_gid = np.full(len(vids), -1, np.int64)
        v_type = np.full(len(vids), "", dtype=object)
        v_name = np.full(len(vids), "", dtype=object)
    return acts, v_gid, v_type, v_name


def activities_by_rows(reader, schema, ctx, rows) -> dict | None:
    """Resolve activities for an arbitrary set of people (lazy by-id), via the
    per-person ``activity_offsets`` index — an O(1) slice per person, the same
    join the reference uses. ``rows`` are population array indices. Returns the
    activity rows (same shape as the activities shard) for all of them, or None.
    This is what lets a venue member's activities show even when they live
    outside the inspected unit (their activities shard is another unit's)."""
    rows = np.unique(np.asarray(rows, np.int64))
    rows = rows[rows >= 0]
    if len(rows) == 0:
        return None
    f = reader.file
    off_ds = f[_ACT_OFFSETS]
    n = len(off_ds)
    data_ds = f[_ACT_DS]
    total = data_ds.shape[0]
    rows = rows[rows < n]
    if len(rows) == 0:
        return None
    starts = off_ds[rows].astype(np.int64)
    nxt = rows + 1
    ends = np.where(nxt < n, off_ds[np.clip(nxt, 0, n - 1)], total).astype(np.int64)
    # Gather each person's contiguous activity block; tag rows with the owner so
    # resolution stays vectorized over the concatenation.
    pid_at_row = f["population/ids"][rows].astype(np.int64)
    blocks, owners = [], []
    for owner, s, e in zip(pid_at_row, starts, ends):
        if e > s:
            blocks.append(data_ds[s:e])
            owners.append(np.full(e - s, owner, np.int64))
    if not blocks:
        return None
    data = np.concatenate(blocks)
    pids = np.concatenate(owners)
    acts, v_gid, v_type, v_name = _resolve_acts(
        ctx, data[:, 1].astype(np.int64), data[:, 2].astype(np.int64))
    return {
        "person_id": pids,
        "activity": acts,
        "venue_id": data[:, 2].astype(np.int64),
        "venue_geo_unit_id": v_gid,
        "venue_type": v_type,
        "venue_name": v_name,
    }


def _activities_span(reader, schema, ctx, span: Span):
    """Yield ``(geo_unit_id, cols)`` for an activity span, resolving each row's
    venue through the sorted ``vmeta`` arrays."""
    data = reader.slice(_ACT_DS, span.start, span.count)
    pids = data[:, 0].astype(np.int64)
    aidx = data[:, 1].astype(np.int64)
    vids = data[:, 2].astype(np.int64)

    acts, v_gid, v_type, v_name = _resolve_acts(ctx, aidx, vids)

    for gid, lo, cnt in span.units:
        hi = lo + cnt
        yield gid, {
            "person_id": pids[lo:hi],
            "geo_unit_id": np.full(cnt, gid, np.int64),
            "activity": acts[lo:hi],
            "venue_id": vids[lo:hi],
            "venue_geo_unit_id": v_gid[lo:hi],
            "venue_type": v_type[lo:hi],
            "venue_name": v_name[lo:hi],
        }


def activities_unit_cols(reader, schema, ctx, gid: int) -> dict | None:
    """One geo unit's activity rows (for lazy serve), or None if empty."""
    sp = _single_span(reader, "activity", gid)
    if sp is None:
        return None
    for _, cols in _activities_span(reader, schema, ctx, sp):
        return cols
    return None


def write_activities(
    reader, schema, out_dir: Path, vmeta: dict[int, tuple]
) -> tuple[str, dict[int, int]]:
    """Per-person activity → venue assignments, sharded by the person's geo unit.

    The world's ``activity_map`` is a geo-partitioned table of
    ``[person_id, activity_idx, venue_id, _]`` rows (one block per geo unit,
    same CSR index as population). Each row is resolved through ``vmeta`` to the
    venue's home unit / type / name, so the frontend can both *name* an
    assignment and *navigate* to it even when the venue lives in another unit.
    Activity labels come from ``schema.activity_names`` — nothing hardcoded.
    """
    ctx = activities_ctx(reader, schema, vmeta)
    w = _ShardWriter(out_dir / "activities.parquet", ctx["schema"])
    for span in reader.partition("activity").spans(_SPAN_ROWS):
        for gid, cols in _activities_span(reader, schema, ctx, span):
            w.write_unit(gid, cols)
    w.close()
    return w.path.name, w.index
