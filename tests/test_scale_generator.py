"""Schema-fidelity and offset-correctness checks for the scale generator.

Skips automatically if the real source world is not present.
"""

import json
from pathlib import Path

import h5py
import numpy as np
import pytest

from mayviewer.synth import generate

SOURCE = Path("/Users/marthacorrea/MAY/output/2021/world_state.h5")
pytestmark = pytest.mark.skipif(not SOURCE.exists(), reason="real source world not available")


@pytest.fixture(scope="module")
def tiled(tmp_path_factory):
    out = tmp_path_factory.mktemp("synth") / "scaled.h5"
    with h5py.File(SOURCE, "r") as f:
        P = int(f.attrs["num_people"])
    stats = generate(SOURCE, P + 1, out)  # forces exactly K=2 replicas
    return out, stats, P


def test_replica_count_and_attrs(tiled):
    out, stats, P = tiled
    assert stats["replicas"] == 2
    with h5py.File(out, "r") as f:
        assert f.attrs["num_people"] == 2 * P
        assert f["population/ids"].shape[0] == 2 * P
        assert f.attrs["synth_replicas"] == 2


def test_ids_are_disjoint_across_replicas(tiled):
    out, _, P = tiled
    with h5py.File(out, "r") as f:
        ids = f["population/ids"][:]
    assert ids.min() >= 0
    assert len(np.unique(ids)) == len(ids), "person ids must stay globally unique"


def test_partition_index_matches_geo_column(tiled):
    """Every person in a partition range must carry that partition's geo id."""
    out, _, _ = tiled
    with h5py.File(out, "r") as f:
        gids = f["population/partition_index/geo_unit_ids"][:]
        starts = f["population/partition_index/start_indices"][:]
        counts = f["population/partition_index/counts"][:]
        pop_geo = f["population/geo_unit_ids"][:]
    for g, s, c in zip(gids, starts, counts):
        assert np.all(pop_geo[s:s + c] == g)


def test_friendships_csr_consistent(tiled):
    out, _, P = tiled
    with h5py.File(out, "r") as f:
        flat = f["population/friendships/flat"][:]
        off = f["population/friendships/offsets"][:]
        cnt = f["population/friendships/counts"][:]
        n_people = f["population/ids"].shape[0]
    assert len(off) == len(cnt) == n_people
    assert off[0] == 0
    assert np.all(off[1:] == np.cumsum(cnt)[:-1])
    assert int(cnt.sum()) == len(flat)
    assert flat.max() < n_people  # edges reference valid, rebased person ids

    # Replica 2's CSR must equal replica 1's, shifted by P (id) and edge count.
    with h5py.File(SOURCE, "r") as f:
        first = json.loads(f["population/properties/friendships"][0].decode())
    assert list(flat[off[0]:off[0] + cnt[0]]) == first
    r2_start = off[P]
    assert list(flat[r2_start:r2_start + cnt[P]]) == [x + P for x in first]


def test_activity_data_ids_in_range(tiled):
    out, _, _ = tiled
    with h5py.File(out, "r") as f:
        ad = f["activity_mappings/activity_map/activity_data"]
        n_people = f["population/ids"].shape[0]
        n_venues = f["venues/ids"].shape[0]
        sample = ad[:] if ad.shape[0] < 5_000_000 else ad[:5_000_000]
    assert sample[:, 0].max() < n_people
    assert sample[:, 2].max() < n_venues
