"""Phase 2 prep-pipeline correctness + memory-flat invariants.

Skips automatically if the real source world is absent. The full-pipeline
fixture reuses the cache next to the source (prep is idempotent), so only the
first run pays the build cost.
"""

import gzip
import json
import struct
from pathlib import Path

import h5py
import numpy as np
import pyarrow.parquet as pq
import pytest

from mayviewer.prep import aggregates, drilldown, geo_tree, hexbin
from mayviewer.prep.pipeline import cache_dir, prep
from mayviewer.prep.reader import WorldReader
from mayviewer.schema import describe

SOURCE = Path("/Users/marthacorrea/MAY/output/2021/world_state.h5")
pytestmark = pytest.mark.skipif(
    not SOURCE.exists(), reason="real source world not available"
)


@pytest.fixture(scope="module")
def world():
    with h5py.File(SOURCE, "r") as f:
        n_people = int(f["population/ids"].shape[0])
        n_venues = int(f["venues/ids"].shape[0])
    return SOURCE, describe(SOURCE), n_people, n_venues


def test_reader_slices_match_direct_and_cover_all(world):
    src, _sc, n_people, _ = world
    with WorldReader(src) as r:
        pop = r.partition("population")
        total = 0
        for gid, s, c in pop:
            total += c
            if c:  # CSR slice is exactly the direct array slice
                got = r.unit_slice("population", "population/ids", gid)
                assert np.array_equal(got, r.file["population/ids"][s:s + c])
        assert total == n_people  # full coverage, no whole-array read
        assert pop.max_count < n_people  # genuinely bounded per unit


def test_geo_tree_every_leaf_rolls_up(world):
    src, sc, _, _ = world
    with WorldReader(src) as r:
        g = geo_tree.build(r)
    assert g.level_values[0] == g.leaf_level
    for lv in g.level_values:
        anc = g.leaf_to_ancestor(lv)
        assert (anc >= 0).all()  # no leaf detached at any level
        assert set(np.unique(anc).tolist()) == set(g.ids_at(lv).tolist())


def test_aggregates_invariants(world):
    src, sc, n_people, n_venues = world
    with WorldReader(src) as r:
        g = geo_tree.build(r)
        rows, props = aggregates.compute(r, sc, g)
        tabs = aggregates.to_tables(rows, props, sc, g)
    coarsest = g.level_values[-1]
    for lv, t in tabs.items():
        d = t.to_pylist()
        # People conserved at every level.
        assert sum(r["people"] for r in d) == n_people
        # Each breakdown family sums back to the population.
        for fam in ("age:", "sex:"):
            assert sum(r[k] for r in d for k in r if k.startswith(fam)) == n_people
        for p in props:
            assert sum(r[k] for r in d for k in r
                       if k.startswith(f"p:{p}=")) == n_people
    # All venues appear once the level is coarse enough to contain them.
    top = tabs[coarsest].to_pylist()
    assert sum(v for r in top for k, v in r.items()
               if k.startswith("venues:")) == n_venues


def test_hexbin_conserves_counts(world):
    src, sc, n_people, n_venues = world
    with WorldReader(src) as r:
        g = geo_tree.build(r)
        hx = hexbin.build(r, sc, g)
    for layer, expected in (("people", n_people), ("venues", n_venues)):
        totals = {res: sum(hx[layer][res].values())
                  for res in hx["resolutions"]}
        assert set(totals.values()) == {expected}  # conserved across zoom LOD


@pytest.fixture(scope="module")
def manifest(world):
    src, *_ = world
    return prep(src), cache_dir(src)


def test_pipeline_writes_all_artifacts(manifest):
    m, out = manifest
    a = m["artifacts"]
    # Manifest is JSON: dict keys are strings, so consumers index by str(lv).
    for lv in m["geo"]["level_values"]:
        assert (out / a["aggregates"][str(lv)]["path"]).exists()
    for kind in ("people", "venues", "members"):
        assert (out / a["drilldown"][kind]["path"]).exists()
    assert (out / a["hexbin"]["path"]).exists()


def test_drilldown_one_row_group_per_unit_and_roundtrip(world, manifest):
    src, _sc, _, _ = world
    m, out = manifest
    dd = m["artifacts"]["drilldown"]
    pf = pq.ParquetFile(out / dd["people"]["path"])
    idx = dd["people"]["row_groups"]
    assert pf.num_row_groups == len(idx)
    with WorldReader(src) as r:
        gid, s, c = next(iter(r.partition("population")))
        ppl = pf.read_row_group(idx[str(gid)]).to_pylist()
    assert len(ppl) == c  # exactly this unit, one O(1) row-group read
    assert {p["geo_unit_id"] for p in ppl} == {gid}


def test_drilldown_property_typing_lossless(manifest):
    _m, out = manifest
    pf = pq.ParquetFile(out / "people.parquet")
    by = {f.name: str(f.type) for f in pf.schema_arrow}
    # JSON string-array preserved as strings; int-array as ints.
    assert by["comorbidities"] == "list<element: string>"
    assert by["friendships"] == "list<element: int64>"
    found = None
    for rg in range(min(80, pf.num_row_groups)):
        for row in pf.read_row_group(rg, columns=["comorbidities"]).to_pylist():
            if row["comorbidities"]:
                found = row["comorbidities"]
                break
        if found:
            break
    assert found and all(isinstance(x, str) for x in found)


def test_pmtiles_header_and_metadata_valid(manifest):
    _m, out = manifest
    raw = (out / "hexbin.pmtiles").read_bytes()
    assert raw[:7] == b"PMTiles" and raw[7] == 3
    root_off, root_len, meta_off, meta_len = struct.unpack_from("<QQQQ", raw, 8)
    assert raw[97] == 2 and raw[99] == 1  # internal gzip, tile type mvt
    meta = json.loads(gzip.decompress(raw[meta_off:meta_off + meta_len]))
    assert {v["id"] for v in meta["vector_layers"]} == {"people", "venues"}
    # Directory is gzip and non-empty.
    assert len(gzip.decompress(raw[root_off:root_off + root_len])) > 0


def test_prep_is_idempotent(world):
    src, *_ = world
    m1 = prep(src)
    m2 = prep(src)  # second call must be a no-op returning the same manifest
    assert m1["source"]["fingerprint"] == m2["source"]["fingerprint"]
    assert m1["artifacts"] == m2["artifacts"]
