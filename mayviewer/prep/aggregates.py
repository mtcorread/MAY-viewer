"""Precomputed per-geo-unit aggregates at every level.

One memory-flat sweep over leaf units (via the partition_index) accumulates
additive per-leaf statistics; those roll up the geo tree to every coarser
level by simple group-sum (mean age is reconstructed from a summed numerator).
Because everything stored is a count or a sum, rollup is exact and the browser
never has to touch raw data to draw a map or a panel.

All labels are schema-driven: sex labels from the ``sex`` registry, venue types
from ``venue_types``, and categorical person properties are discovered from a
sample (JSON-valued, relational, or high-cardinality properties are detected
and left out of the breakdown rather than hardcoded in or out).
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa

# Default age bands (left-inclusive). Not a domain registry in MAY, so this is
# a presentation default; counts at every band are exact and re-bin-able later.
AGE_BANDS = [(0, 18), (18, 30), (30, 45), (45, 65), (65, 1000)]
AGE_LABELS = ["0_17", "18_29", "30_44", "45_64", "65_plus"]

# A categorical person property is dropped from the breakdown if a sample shows
# it is JSON/relational or has more distinct values than this (e.g. an id-like
# field) — keeps the aggregate table bounded without naming any property.
MAX_CATEGORIES = 64
_SAMPLE = 4000


def _dec(v) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)


def _classify_properties(reader, schema) -> list[str]:
    """Person properties suitable for a categorical breakdown (sample-based)."""
    keep: list[str] = []
    for prop in schema.person_properties:
        if prop in schema.person_relations:
            continue  # a graph, not a category (handled by drill-down)
        path = f"population/properties/{prop}"
        if path not in reader:
            continue
        sample = reader.slice(path, 0, _SAMPLE)
        vals = [_dec(v).strip() for v in sample]
        if any(v[:1] in ("[", "{") for v in vals):
            continue  # JSON-valued (e.g. comorbidities, friendships)
        if len(set(vals)) > min(MAX_CATEGORIES, max(8, len(vals) // 4)):
            continue  # id-like / free-text, not a useful breakdown
        keep.append(prop)
    return keep


class _LeafRow:
    __slots__ = ("people", "sum_age", "age", "sex", "prop", "vcount", "occ")

    def __init__(self, n_sex: int, props: list[str], n_vtype: int):
        self.people = 0
        self.sum_age = 0.0
        self.age = np.zeros(len(AGE_BANDS), dtype=np.int64)
        self.sex = np.zeros(n_sex, dtype=np.int64)
        self.prop: dict[str, dict[str, int]] = {p: {} for p in props}
        self.vcount = np.zeros(n_vtype, dtype=np.int64)  # venues by type
        self.occ = np.zeros(n_vtype, dtype=np.int64)  # members by venue type

    def add_people(self, ages, sexes, prop_slices):
        self.people += len(ages)
        self.sum_age += float(np.nansum(ages))
        idx = np.clip(np.searchsorted(
            np.array([b[1] for b in AGE_BANDS]), ages, side="right"),
            0, len(AGE_BANDS) - 1)
        self.age += np.bincount(idx, minlength=len(AGE_BANDS))
        for code, cnt in zip(*np.unique(sexes, return_counts=True)):
            if 0 <= int(code) < len(self.sex):
                self.sex[int(code)] += int(cnt)
        for prop, sl in prop_slices.items():
            d = self.prop[prop]
            cats, cnts = np.unique([_dec(v) for v in sl], return_counts=True)
            for c, n in zip(cats, cnts):
                d[c] = d.get(c, 0) + int(n)


def compute(reader, schema, geo) -> tuple[dict[int, "_LeafRow"], list[str]]:
    """One sweep → per-leaf rows keyed by leaf geo id, plus kept-property list."""
    code_to_sex = {v: k for k, v in schema.sex_mapping.items()}
    n_sex = (max(code_to_sex) + 1) if code_to_sex else 1
    vtypes = schema.venue_types
    n_vtype = len(vtypes)
    props = _classify_properties(reader, schema)

    rows: dict[int, _LeafRow] = {}

    def row(gid: int) -> _LeafRow:
        r = rows.get(gid)
        if r is None:
            r = rows[gid] = _LeafRow(n_sex, props, n_vtype)
        return r

    # People.
    for gid, s, c in reader.partition("population"):
        if c == 0:
            continue
        ages = reader.slice("population/ages", s, c)
        sexes = reader.slice("population/sexes", s, c)
        prop_sl = {
            p: reader.slice(f"population/properties/{p}", s, c) for p in props
        }
        row(gid).add_people(ages, sexes, prop_sl)

    # Venues by type, per leaf, and a vid→type map for occupancy.
    vid_type: dict[int, int] = {}
    for gid, s, c in reader.partition("venues"):
        if c == 0:
            continue
        vids = reader.slice("venues/ids", s, c)
        vtys = reader.slice("venues/types", s, c)
        r = row(gid)
        for t, n in zip(*np.unique(vtys, return_counts=True)):
            if 0 <= int(t) < n_vtype:
                r.vcount[int(t)] += int(n)
        vid_type.update(zip(vids.tolist(), vtys.tolist()))

    # Occupancy: sum subset member_counts grouped by their venue's type.
    for gid, s, c in reader.partition("subsets"):
        if c == 0:
            continue
        s_vid = reader.slice("venues/subsets/venue_ids", s, c)
        s_mc = reader.slice("venues/subsets/member_counts", s, c)
        r = row(gid)
        for vid, mc in zip(s_vid.tolist(), s_mc.tolist()):
            t = vid_type.get(vid)
            if t is not None:
                r.occ[t] += int(mc)

    return rows, props


def to_tables(rows, props, schema, geo) -> dict[int, pa.Table]:
    """Roll leaf rows up to every geo level → one Arrow table per level."""
    code_to_sex = {v: k for k, v in schema.sex_mapping.items()}
    sex_labels = [code_to_sex.get(i, str(i))
                  for i in range(max(code_to_sex) + 1 if code_to_sex else 1)]
    vtypes = schema.venue_types

    # Stable category set per kept property (union across all leaves).
    cats: dict[str, list[str]] = {}
    for p in props:
        seen: set[str] = set()
        for r in rows.values():
            seen.update(r.prop[p])
        cats[p] = sorted(seen)

    # Source rows are keyed by their *actual* geo unit, which may sit at any
    # level (households at the leaf SGU; companies/offices at a coarser unit).
    # Roll each up to its ancestor at the target level; units already coarser
    # than the target get ROOT_SENTINEL and are correctly not placed there.
    src_ids = np.array(sorted(rows), dtype=np.int64)
    tables: dict[int, pa.Table] = {}
    for lv in geo.level_values:
        anc = geo.ancestor_at(src_ids, lv)
        agg: dict[int, _LeafRow] = {}
        for src_gid, unit in zip(src_ids.tolist(), anc.tolist()):
            if unit < 0:
                continue
            src = rows[src_gid]
            dst = agg.get(unit)
            if dst is None:
                dst = agg[unit] = _LeafRow(len(sex_labels), props, len(vtypes))
            dst.people += src.people
            dst.sum_age += src.sum_age
            dst.age += src.age
            dst.sex += src.sex
            dst.vcount += src.vcount
            dst.occ += src.occ
            for p in props:
                d = dst.prop[p]
                for k, n in src.prop[p].items():
                    d[k] = d.get(k, 0) + n

        ids = sorted(agg)
        col: dict[str, list] = {
            "geo_id": ids,
            "geo_name": [geo.name_of(i) for i in ids],
            "level": [lv] * len(ids),
            "level_name": [schema.label_geo_level(lv)] * len(ids),
            "parent_id": [int(geo.parent_of(np.array([i]))[0]) for i in ids],
            "people": [agg[i].people for i in ids],
            "mean_age": [
                round(agg[i].sum_age / agg[i].people, 2) if agg[i].people else None
                for i in ids
            ],
        }
        for bi, bl in enumerate(AGE_LABELS):
            col[f"age:{bl}"] = [int(agg[i].age[bi]) for i in ids]
        for si, sl in enumerate(sex_labels):
            col[f"sex:{sl}"] = [int(agg[i].sex[si]) for i in ids]
        for ti, tl in enumerate(vtypes):
            col[f"venues:{tl}"] = [int(agg[i].vcount[ti]) for i in ids]
            col[f"occ:{tl}"] = [int(agg[i].occ[ti]) for i in ids]
        for p in props:
            for cat in cats[p]:
                label = cat if cat != "" else "(none)"
                col[f"p:{p}={label}"] = [agg[i].prop[p].get(cat, 0) for i in ids]
        tables[lv] = pa.table(col)
    return tables
