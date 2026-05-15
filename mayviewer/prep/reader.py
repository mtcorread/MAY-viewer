"""Memory-flat reader: slice one leaf geo unit at a time via partition_index.

This is the foundation of Phase 2. The MAY serializer writes a CSR
``partition_index`` (``geo_unit_ids -> start_indices -> counts``) next to every
bulk container (population, venues, venues/subsets, activity map). It maps a
leaf geo unit to the *contiguous* row range its rows occupy in the flat
structure-of-arrays datasets, so any single unit's data is one O(1) slice and
the whole file can be swept unit-by-unit with peak memory bounded by the
largest single unit — never the whole array. Nothing here reads a bulk dataset
in full; that is the non-negotiable that lets the pipeline run at 60M agents.

Geography metadata (``geography/*``) is the one thing read whole: it is
O(num_geo_units), which is ~180k even for all-England — small and needed in
full to build the level rollup tree.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import h5py
import numpy as np

# Containers that carry a CSR partition_index, mapped to (index group path,
# flat datasets indexed by it). Members get their own index because
# members_flat is a second CSR level under venues/subsets.
PARTITIONS: dict[str, str] = {
    "population": "population/partition_index",
    "venues": "venues/partition_index",
    "subsets": "venues/subsets/partition_index",
    "members": "venues/subsets/members_partition_index",
    "activity": "activity_mappings/activity_map/partition_index",
}


@dataclass(frozen=True)
class Partition:
    """One container's CSR geo index. ``bounds(gid)`` is the O(1) slice."""

    container: str
    geo_unit_ids: np.ndarray  # leaf geo unit id per stored block
    start_indices: np.ndarray  # row offset into the flat datasets
    counts: np.ndarray  # rows for that geo unit
    _pos: dict[int, int]  # geo_unit_id -> row in the arrays above

    def has(self, geo_unit_id: int) -> bool:
        return int(geo_unit_id) in self._pos

    def bounds(self, geo_unit_id: int) -> tuple[int, int] | None:
        """(start, count) for this leaf unit, or None if it has no rows."""
        i = self._pos.get(int(geo_unit_id))
        if i is None:
            return None
        return int(self.start_indices[i]), int(self.counts[i])

    def __iter__(self):
        """Yield (geo_unit_id, start, count) in the file's stored order."""
        for gid, s, c in zip(self.geo_unit_ids, self.start_indices, self.counts):
            yield int(gid), int(s), int(c)

    def __len__(self) -> int:
        return len(self.geo_unit_ids)

    @property
    def max_count(self) -> int:
        """Largest single-unit slice — the pipeline's peak-memory bound."""
        return int(self.counts.max()) if len(self.counts) else 0


class WorldReader:
    """Read-only handle over a MAY world that only ever slices by geo unit."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._f = h5py.File(self.path, "r")
        self._parts: dict[str, Partition] = {}

    def __enter__(self) -> "WorldReader":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        self._f.close()

    @property
    def file(self) -> h5py.File:
        return self._f

    def __contains__(self, path: str) -> bool:
        return path in self._f

    # -- partition indices ------------------------------------------------ #
    def partition(self, container: str) -> Partition:
        """Cached CSR index for a container (see ``PARTITIONS``)."""
        if container in self._parts:
            return self._parts[container]
        base = PARTITIONS[container]
        gids = self._f[f"{base}/geo_unit_ids"][:].astype(np.int64)
        starts = self._f[f"{base}/start_indices"][:].astype(np.int64)
        counts = self._f[f"{base}/counts"][:].astype(np.int64)
        pos = {int(g): i for i, g in enumerate(gids)}
        p = Partition(container, gids, starts, counts, pos)
        self._parts[container] = p
        return p

    # -- bounded reads ---------------------------------------------------- #
    def slice(self, dataset: str, start: int, count: int) -> np.ndarray:
        """One bounded block of a flat dataset — the only way bulk data is read."""
        return self._f[dataset][start:start + count]

    def unit_slice(self, container: str, dataset: str, geo_unit_id: int):
        """A leaf unit's rows of ``dataset``, or None if the unit is empty."""
        b = self.partition(container).bounds(geo_unit_id)
        if b is None:
            return None
        return self.slice(dataset, b[0], b[1])

    def geo_full(self, name: str) -> np.ndarray:
        """A whole ``geography/<name>`` array. Safe: O(num_geo_units)."""
        return self._f[f"geography/{name}"][:]

    def names(self, kind: str) -> np.ndarray | None:
        """A whole ``metadata/names/<kind>`` array, or None if absent."""
        p = f"metadata/names/{kind}"
        return self._f[p][:] if p in self._f else None
