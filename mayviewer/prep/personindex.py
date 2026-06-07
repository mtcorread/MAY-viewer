"""``person_id -> home geo unit`` index — the join that resolves a venue
member who lives outside the unit being inspected.

Drill-down people are sharded by each person's *home* geo unit (the population
partition). A venue's members, however, are drawn from many surrounding units
(a school, a company, a pub), so most members' attribute rows live in other
units' shards. To resolve them we need to know, for any ``person_id``, which
unit's people shard holds them. That is exactly this index.

It is a single flat array keyed by ``person_id`` whose value is the person's
home geo unit id (the partition gid — the same key the people shard is indexed
by, so ``readGeoUnit(people, home_unit)`` is guaranteed to contain the row).
``-1`` marks an id with no person.

The array is the one unavoidably O(num_people) structure in the pipeline, so it
is built and stored as a memory-mapped ``.npy``: construction scatters into the
mmap (peak RAM bounded by the OS page cache, not the array size) and the lazy
server later ``np.load(..., mmap_mode='r')``s it so only touched pages are
resident. At 60M people this is a ~240 MB int32 file — small beside the shards,
and the price of cross-unit member resolution.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

# Population is read in bounded chunks (same cap the drill-down sweep uses) so
# building the index never holds more than one chunk of ids in memory.
_CHUNK_ROWS = 250_000

FILENAME = "person_home_unit.npy"


def build_home_units(reader, out_path: str | Path) -> int:
    """Write ``person_id -> home geo unit`` to ``out_path`` (an ``.npy``).

    Returns the array length (``max person_id + 1``). Memory-flat: two bounded
    passes over ``population/ids`` — one to size the array, one to scatter each
    unit's gid onto its members' ids.
    """
    part = reader.partition("population")

    # Pass 1: largest person id → array length. Reading ids per coalesced span
    # keeps peak memory at one span, never the whole id column.
    max_id = -1
    for sp in part.spans(_CHUNK_ROWS):
        ids = reader.slice("population/ids", sp.start, sp.count)
        if len(ids):
            max_id = max(max_id, int(ids.max()))
    n = max_id + 1

    # int32 is enough while geo unit ids stay below 2**31 (millions of units);
    # widen only if a world ever exceeds that.
    gid_max = int(part.geo_unit_ids.max()) if len(part.geo_unit_ids) else 0
    dtype = np.int32 if gid_max < 2**31 - 1 else np.int64

    out_path = Path(out_path)
    arr = np.lib.format.open_memmap(out_path, mode="w+", dtype=dtype, shape=(n,))
    arr[:] = -1  # ids with no person stay -1

    # Pass 2: scatter the partition gid onto each member's id. The gid is the
    # people-shard key, so the value points straight at the shard that holds the
    # person's attribute row.
    for sp in part.spans(_CHUNK_ROWS):
        ids = reader.slice("population/ids", sp.start, sp.count).astype(np.int64)
        for gid, lo, cnt in sp.units:
            arr[ids[lo:lo + cnt]] = gid
    arr.flush()
    del arr
    return n


def load_home_units(path: str | Path) -> np.ndarray:
    """Memory-map the index read-only (only touched pages become resident)."""
    return np.load(path, mmap_mode="r")


def build_row_index(reader) -> np.ndarray:
    """``person_id -> population array row`` (``-1`` if absent), in memory.

    The lazy server uses this to resolve a venue's members by id in a single
    bounded fancy-read of the population arrays — ~30 student rows instead of
    fetching every feeder unit whole. Built memory-flat from ``population/ids``
    (two bounded passes); cheap (sub-second at ~9M) and held resident (~35 MB at
    that scale), so the first member resolve is immediate, not trickled.
    """
    part = reader.partition("population")
    max_id = -1
    for sp in part.spans(_CHUNK_ROWS):
        ids = reader.slice("population/ids", sp.start, sp.count)
        if len(ids):
            max_id = max(max_id, int(ids.max()))
    n = max_id + 1
    dtype = np.int32 if n < 2**31 - 1 else np.int64
    arr = np.full(n, -1, dtype)
    for sp in part.spans(_CHUNK_ROWS):
        ids = reader.slice("population/ids", sp.start, sp.count).astype(np.int64)
        # ids within a span sit at contiguous rows [start, start+count).
        arr[ids] = np.arange(sp.start, sp.start + sp.count)
    return arr


def home_unit_of(home_lut: np.ndarray, person_ids: np.ndarray) -> np.ndarray:
    """Vectorized lookup with bounds guard → home gid per id, ``-1`` if absent."""
    pids = np.asarray(person_ids, dtype=np.int64)
    out = np.full(len(pids), -1, np.int64)
    if len(home_lut):
        ok = (pids >= 0) & (pids < len(home_lut))
        out[ok] = home_lut[pids[ok]]
    return out
