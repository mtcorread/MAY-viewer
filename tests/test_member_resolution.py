"""Cross-unit venue-member resolution: a member living outside the inspected
unit must carry the home unit whose people shard holds its attributes, and that
shard must in fact contain the member.

This is the fix for school/company/pub members rendering blank: drill-down
people are sharded by home unit, but a venue draws members from many units. A
tiny two-unit world (person ids deliberately != array positions, like a real
geo-sorted population) exercises the whole join without the proprietary source.
"""

from pathlib import Path

import h5py
import numpy as np
import pytest

from mayviewer.prep import drilldown as dd
from mayviewer.prep import personindex
from mayviewer.prep.reader import WorldReader
from mayviewer.schema import describe


@pytest.fixture
def two_unit_world(tmp_path) -> Path:
    """Units 0 and 1. Person ids are non-dense and geo-sorted (10,11 in unit 0;
    20,21 in unit 1). Unit 0 holds one venue whose single subset has members
    [10, 20] — i.e. one local member and one who lives in unit 1."""
    p = tmp_path / "world.h5"
    with h5py.File(p, "w") as f:
        # ── population (geo-sorted: unit 0 then unit 1) ──
        f["population/ids"] = np.array([10, 11, 20, 21], np.int32)
        f["population/ages"] = np.array([30, 40, 50, 60], np.float32)
        f["population/sexes"] = np.array([0, 1, 0, 1], np.int8)
        f["population/geo_unit_ids"] = np.array([0, 0, 1, 1], np.int32)
        f["population/partition_index/geo_unit_ids"] = np.array([0, 1], np.int32)
        f["population/partition_index/start_indices"] = np.array([0, 2], np.int64)
        f["population/partition_index/counts"] = np.array([2, 2], np.int64)
        dt = h5py.string_dtype()
        f.create_dataset("population/properties/ethnicity",
                         data=np.array([b"A", b"B", b"C", b"D"], dtype=dt))

        # ── venues: one household venue (id 0) in unit 0 ──
        f["venues/ids"] = np.array([0], np.int32)
        f["venues/types"] = np.array([0], np.int32)
        f["venues/parent_ids"] = np.array([-1], np.int64)
        f["venues/ranks_in_type"] = np.array([0], np.int64)
        f["venues/partition_index/geo_unit_ids"] = np.array([0], np.int32)
        f["venues/partition_index/start_indices"] = np.array([0], np.int64)
        f["venues/partition_index/counts"] = np.array([1], np.int64)

        # ── subsets: venue 0 has one subset with members [10, 20] ──
        f["venues/subsets/venue_ids"] = np.array([0], np.int32)
        f["venues/subsets/subset_indices"] = np.array([0], np.int32)
        f["venues/subsets/members_offsets"] = np.array([0], np.int64)
        f["venues/subsets/member_counts"] = np.array([2], np.int64)
        f["venues/subsets/members_flat"] = np.array([10, 20], np.int32)
        f["venues/subsets/partition_index/geo_unit_ids"] = np.array([0], np.int32)
        f["venues/subsets/partition_index/start_indices"] = np.array([0], np.int64)
        f["venues/subsets/partition_index/counts"] = np.array([1], np.int64)
        f["venues/subsets/members_partition_index/geo_unit_ids"] = np.array([0], np.int32)
        f["venues/subsets/members_partition_index/start_indices"] = np.array([0], np.int64)
        f["venues/subsets/members_partition_index/counts"] = np.array([2], np.int64)
        f.create_dataset("metadata/names/subsets",
                         data=np.array([b"residents"], dtype=dt))

        # ── activity map (per-person offsets, like the reference) ──
        # person 10 (row 0) and 20 (row 2) each do one activity at venue 0.
        f["activity_mappings/activity_map/activity_data"] = np.array(
            [[10, 0, 0, 0], [20, 0, 0, 0]], np.int32)
        f["activity_mappings/activity_map/activity_offsets"] = np.array(
            [0, 1, 1, 2], np.int32)  # indexed by array row: 10→[0:1], 20→[1:2]
        f.create_dataset("activity_mappings/activity_map/activity_names",
                         data=np.array([b"work"], dtype=dt))

        # ── geography + registries ──
        f["geography/ids"] = np.array([0, 1], np.int32)
        f.create_dataset("metadata/registries/geo_levels",
                         data=np.array([b"unit"], dtype=dt))
        f.create_dataset("metadata/registries/venue_types",
                         data=np.array([b"household"], dtype=dt))
        f.create_dataset("metadata/registries/subset_names",
                         data=np.array([b"residents"], dtype=dt))
        sex = f.create_dataset("metadata/registries/sex", data=np.array([0, 1], np.int8))
        sex.attrs["mapping"] = "male:0,female:1"
    return p


def test_home_unit_index_maps_id_to_partition_gid(two_unit_world):
    with WorldReader(two_unit_world) as r:
        n = personindex.build_home_units(r, two_unit_world.parent / personindex.FILENAME)
        home = personindex.load_home_units(two_unit_world.parent / personindex.FILENAME)
    assert n == 22  # max id (21) + 1
    # ids index directly; each maps to its home (partition) unit.
    assert home[10] == 0 and home[11] == 0
    assert home[20] == 1 and home[21] == 1
    assert home[0] == -1  # no such person


def test_members_carry_home_unit_and_resolve_cross_unit(two_unit_world):
    sc = describe(two_unit_world)
    with WorldReader(two_unit_world) as r:
        personindex.build_home_units(r, two_unit_world.parent / personindex.FILENAME)
        home = personindex.load_home_units(two_unit_world.parent / personindex.FILENAME)

        mctx = dd.members_ctx(r, sc, home)
        mcols = dd.members_unit_cols(r, sc, mctx, 0)  # inspect unit 0's members
        by_pid = dict(zip(mcols["person_id"].tolist(), mcols["home_geo_unit"].tolist()))
        # Member 10 lives in the inspected unit; member 20 lives in unit 1.
        assert by_pid == {10: 0, 20: 1}

        # The out-of-unit member (20) is absent from unit 0's people shard …
        pctx = dd.people_ctx(r, sc)
        unit0 = dd.people_unit_cols(r, sc, pctx, 0)
        assert 20 not in set(unit0["person_id"].tolist())
        # … but present in its home unit's shard (1), so the frontend resolves it
        # by reading that feeder unit. This is the exact blank-row fix.
        unit1 = dd.people_unit_cols(r, sc, pctx, 1)
        assert 20 in set(unit1["person_id"].tolist())


def test_by_id_resolution_reads_arbitrary_people(two_unit_world):
    """The lazy fast path: resolve people by id straight from the .h5 in one
    bounded read (no per-feeder-unit fetch). Ids are non-dense, so this also
    checks the person_id→row index handles the geo-sorted permutation."""
    sc = describe(two_unit_world)
    with WorldReader(two_unit_world) as r:
        row = personindex.build_row_index(r)
        assert row[10] == 0 and row[20] == 2 and row[21] == 3  # id → array row
        ctx = dd.people_ctx(r, sc)
        # Resolve a local (10) and an out-of-unit (20) member together.
        cols = dd.people_by_rows(r, sc, ctx, row[[20, 10]])
        got = dict(zip(cols["person_id"].tolist(), cols["sex"].tolist()))
        assert got == {10: "male", 20: "male"}
        assert dict(zip(cols["person_id"].tolist(),
                        [int(a) for a in cols["age"]])) == {10: 30, 20: 50}


def test_lazy_inspector_people_by_ids(two_unit_world):
    """End-to-end via LazyInspector (what /inspect/people_by_id calls)."""
    from mayviewer.serve import LazyInspector
    insp = LazyInspector(two_unit_world)
    try:
        rows = insp.people_by_ids([20, 10, 999])  # 999 is out of range → dropped
        by = {r["person_id"]: r for r in rows}
        assert set(by) == {10, 20}
        assert by[20]["ethnicity"] == "C" and by[20]["age"] == 50
    finally:
        insp.close()


def test_activities_resolve_by_id_cross_unit(two_unit_world):
    """A venue member's activities must resolve by id even though their
    activities shard lives in their home unit, not the inspected one."""
    sc = describe(two_unit_world)
    with WorldReader(two_unit_world) as r:
        row = personindex.build_row_index(r)
        vmeta = dd.venue_index(r, sc)
        ctx = dd.activities_ctx(r, sc, vmeta)
        # Person 20 lives in unit 1; resolve their activity by id.
        cols = dd.activities_by_rows(r, sc, ctx, row[[20]])
        assert cols["person_id"].tolist() == [20]
        assert cols["activity"].tolist() == ["work"]
        assert cols["venue_id"].tolist() == [0]
        assert cols["venue_type"].tolist() == ["household"]


def test_lazy_inspector_activities_by_ids(two_unit_world):
    from mayviewer.serve import LazyInspector
    insp = LazyInspector(two_unit_world)
    try:
        acts = insp.activities_by_ids([20])
        assert len(acts) == 1
        assert acts[0]["person_id"] == 20 and acts[0]["activity"] == "work"
    finally:
        insp.close()


def test_members_without_index_degrade_to_minus_one(two_unit_world):
    """A pre-v2 cache (no index) still builds members — home_geo_unit is -1, and
    the frontend simply can't cross-resolve (falls back to blanks, no crash)."""
    sc = describe(two_unit_world)
    with WorldReader(two_unit_world) as r:
        mctx = dd.members_ctx(r, sc, None)
        mcols = dd.members_unit_cols(r, sc, mctx, 0)
    assert set(mcols["home_geo_unit"].tolist()) == {-1}
