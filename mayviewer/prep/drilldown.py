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
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

_SAMPLE = 4000


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


def write_people(reader, schema, out_dir: Path) -> tuple[str, dict[int, int]]:
    scalar, jsonish = _classify(reader, schema)
    code_to_sex = {v: k for k, v in schema.sex_mapping.items()}

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
    sch = pa.schema(fields)
    w = _ShardWriter(out_dir / "people.parquet", sch)

    for gid, s, c in reader.partition("population"):
        if c == 0:
            continue
        cols: dict = {
            "person_id": reader.slice("population/ids", s, c).astype(np.int64),
            "geo_unit_id": np.full(c, gid, np.int64),
            "age": reader.slice("population/ages", s, c),
            "sex": [code_to_sex.get(int(x), str(int(x)))
                    for x in reader.slice("population/sexes", s, c)],
        }
        for p in scalar:
            cols[p] = [_dec(v) for v in
                       reader.slice(f"population/properties/{p}", s, c)]
        for p, k in jsonish.items():
            cols[p] = [_to_list(_dec(v), k) for v in
                       reader.slice(f"population/properties/{p}", s, c)]
        cols.update(_friend_lists(reader, schema, s, c))
        w.write_unit(gid, cols)

    w.close()
    return w.path.name, w.index


def write_venues(reader, schema, out_dir: Path) -> tuple[str, dict[int, int]]:
    names = reader.names("venues")
    vtypes = schema.venue_types
    # venues/properties/<type>/<prop> arrays are indexed by rank_in_type.
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
    sch = pa.schema(fields)
    w = _ShardWriter(out_dir / "venues.parquet", sch)

    for gid, s, c in reader.partition("venues"):
        if c == 0:
            continue
        vids = reader.slice("venues/ids", s, c).astype(np.int64)
        tys = reader.slice("venues/types", s, c)
        ranks = reader.slice("venues/ranks_in_type", s, c).astype(np.int64)
        cols: dict = {
            "venue_id": vids,
            "geo_unit_id": np.full(c, gid, np.int64),
            "type": [schema.label_venue_type(int(t)) for t in tys],
            "name": [_dec(n) for n in names[s:s + c]] if names is not None
            else [str(v) for v in vids],
            "parent_id": reader.slice("venues/parent_ids", s, c).astype(np.int64),
            "is_residence": reader.slice("venues/is_residence", s, c),
            "lat": reader.slice("venues/latitudes", s, c),
            "lon": reader.slice("venues/longitudes", s, c),
            "rank_in_type": ranks,
        }
        for col in prop_cols:
            cols[col] = [None] * c
        # Attach per-type properties by rank_in_type (scattered, but bounded
        # to this unit's venues; sorted unique indices keep the h5py read tight).
        for ti, tname in enumerate(vtypes):
            sel = np.where(tys == ti)[0]
            for prop in vprops.get(tname, []):
                ds = f"venues/properties/{tname}/{prop}"
                if ds not in reader:
                    continue
                rk = ranks[sel]
                if len(rk) == 0:
                    continue
                order = np.argsort(rk)
                # h5py sorted fancy index: reads only these rows, never the
                # whole per-type property array (60M-safe).
                vals = reader.file[ds][rk[order]]
                colname = f"{tname}.{prop}"
                target = cols[colname]
                for k, oi in enumerate(order):
                    target[int(sel[oi])] = _dec(vals[k])
        w.write_unit(gid, cols)

    w.close()
    return w.path.name, w.index


def write_members(reader, schema, out_dir: Path) -> tuple[str, dict[int, int]]:
    snames = schema.subset_names
    sch = pa.schema([
        ("venue_id", pa.int64()),
        ("geo_unit_id", pa.int64()),
        ("subset", pa.string()),
        ("person_id", pa.int64()),
    ])
    w = _ShardWriter(out_dir / "members.parquet", sch)

    members = reader.partition("members")
    for gid, s, c in reader.partition("subsets"):
        if c == 0:
            continue
        s_vid = reader.slice("venues/subsets/venue_ids", s, c).astype(np.int64)
        s_sub = reader.slice("venues/subsets/subset_indices", s, c)
        s_off = reader.slice("venues/subsets/members_offsets", s, c).astype(np.int64)
        s_cnt = reader.slice("venues/subsets/member_counts", s, c).astype(np.int64)
        mb = members.bounds(gid)
        if mb is None:
            continue
        m_lo, m_n = mb
        blk = reader.slice("venues/subsets/members_flat", m_lo, m_n)
        v_out, g_out, sub_out, p_out = [], [], [], []
        for vid, si, o, n in zip(s_vid, s_sub, s_off, s_cnt):
            rel = int(o) - m_lo
            mem = blk[rel:rel + int(n)]
            label = snames[int(si)] if 0 <= int(si) < len(snames) else str(int(si))
            v_out.extend([int(vid)] * len(mem))
            g_out.extend([gid] * len(mem))
            sub_out.extend([label] * len(mem))
            p_out.extend(int(x) for x in mem)
        w.write_unit(gid, {
            "venue_id": v_out, "geo_unit_id": g_out,
            "subset": sub_out, "person_id": p_out,
        })

    w.close()
    return w.path.name, w.index
