"""The geography hierarchy — drives aggregate rollup.

MAY's ``partition_index`` only exists at the *leaf* geo level (the finest unit,
e.g. OA/SGU). Aggregates at coarser levels (MSOA/LAD/region…) are produced by
rolling leaf values up the ``geography/parent_ids`` tree. This module turns the
flat ``geography/{ids,levels,parent_ids}`` arrays into that tree and exposes the
leaf→ancestor mapping the rollup needs.

Level *names* come from the schema (``geo_levels`` registry); this module never
hardcodes one. The numeric ``levels`` value indexes that registry directly, and
a larger value is a finer unit (leaf = max level value), which is the only
structural fact assumed — it is the MAY serializer contract.
"""

from __future__ import annotations

import numpy as np

ROOT_SENTINEL = -1  # parent_id of the top-level node


class GeoTree:
    """Geography tree with vectorised leaf→ancestor lookup for rollup."""

    def __init__(
        self,
        ids: np.ndarray,
        levels: np.ndarray,
        parent_ids: np.ndarray,
        names: np.ndarray | None = None,
    ):
        self.ids = ids.astype(np.int64)
        self.levels = levels.astype(np.int64)
        self.parent_ids = parent_ids.astype(np.int64)
        # id -> row position (handles non-contiguous ids without assuming any).
        self._pos = np.full(int(self.ids.max()) + 1, -1, dtype=np.int64)
        self._pos[self.ids] = np.arange(len(self.ids))
        self._names = (
            [n.decode() if isinstance(n, bytes) else str(n) for n in names[:]]
            if names is not None
            else None
        )
        # Finer unit == larger level value; leaf is the finest.
        self.leaf_level = int(self.levels.max())
        # Distinct level values, leaf first up to root.
        self.level_values = sorted(
            {int(v) for v in np.unique(self.levels)}, reverse=True
        )

    # -- basic lookups ---------------------------------------------------- #
    def _row(self, gid: int) -> int:
        return int(self._pos[gid])

    def name_of(self, gid: int) -> str:
        if self._names is None:
            return str(gid)
        return self._names[self._row(gid)]

    def ids_at(self, level_value: int) -> np.ndarray:
        return self.ids[self.levels == level_value]

    @property
    def leaves(self) -> np.ndarray:
        return self.ids_at(self.leaf_level)

    # -- rollup ----------------------------------------------------------- #
    def parent_of(self, gids: np.ndarray) -> np.ndarray:
        """Vectorised parent id (ROOT_SENTINEL stays ROOT_SENTINEL)."""
        out = np.full(len(gids), ROOT_SENTINEL, dtype=np.int64)
        valid = gids >= 0
        out[valid] = self.parent_ids[self._pos[gids[valid]]]
        return out

    def ancestor_at(self, gids: np.ndarray, level_value: int) -> np.ndarray:
        """For each id, its ancestor at ``level_value``.

        Climbs ``parent_ids`` until the node's level matches the target.
        Ids already coarser than the target (or detached) map to
        ``ROOT_SENTINEL`` and are simply not counted at that level.
        """
        cur = gids.astype(np.int64).copy()
        for _ in range(len(self.level_values)):  # tree depth is bounded
            lv = np.full(len(cur), ROOT_SENTINEL, dtype=np.int64)
            valid = cur >= 0
            lv[valid] = self.levels[self._pos[cur[valid]]]
            done = (~valid) | (lv == level_value)
            if done.all():
                break
            climb = valid & (lv > level_value)
            cur[climb] = self.parent_of(cur[climb])
            # Anything finer-but-not-climbable or already coarser is detached.
            cur[valid & (lv < level_value)] = ROOT_SENTINEL
        return cur

    def leaf_to_ancestor(self, level_value: int) -> np.ndarray:
        """Ancestor id at ``level_value`` for every leaf, in ``self.leaves`` order."""
        return self.ancestor_at(self.leaves, level_value)


def build(reader) -> GeoTree:
    """Construct the tree from an open :class:`~mayviewer.prep.reader.WorldReader`."""
    return GeoTree(
        ids=reader.geo_full("ids"),
        levels=reader.geo_full("levels"),
        parent_ids=reader.geo_full("parent_ids"),
        names=reader.names("geography"),
    )
