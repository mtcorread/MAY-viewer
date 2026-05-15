"""
Phase 1 — synthetic scale generator.

Produces a schema-faithful MAY ``world_state.h5`` of arbitrary size by tiling a
real source world into K disjoint, ID-rebased replicas.

Why tiling (vs. synthesising from scratch):
  * Every dataset, partition index and registry the MAY serializer writes is
    reproduced exactly, so the prep pipeline (Phase 2) is exercised against the
    real schema — see MAY/may/serialization/world_serializer.py.
  * Internal consistency is preserved for free: each replica is a self-contained
    world, so person/venue/geo cross-references stay valid after a constant
    offset is applied per replica.
  * It is inherently streaming: replicas are written one at a time, so peak
    memory is ~one source world regardless of the target size.

Replica r gets disjoint id-spaces by offsetting:
    person id   += r * P      geo id   += r * G      venue id += r * V
CSR offset arrays (activity_offsets, members_offsets, friendship offsets) are
rebased by ``r * <source length>`` because every replica has identical lengths.

The one schema improvement: ``population/properties/friendships`` (a JSON string
per person in the source) is converted to a typed CSR triple under
``population/friendships/`` — see ``_friendships_to_csr``.
"""

from __future__ import annotations

import json
import logging
import math
from pathlib import Path

import h5py
import numpy as np

logger = logging.getLogger("mayviewer.synth")

_GZIP = dict(compression="gzip", compression_opts=4, shuffle=True)


def _src_len(src: h5py.File, path: str) -> int:
    return int(src[path].shape[0])


def _friendships_to_csr(raw: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """JSON-string-per-person -> (flat int32, offsets int32[P], counts int32[P])."""
    counts = np.empty(len(raw), dtype=np.int32)
    parsed: list[list[int]] = []
    for i, v in enumerate(raw):
        if isinstance(v, bytes):
            v = v.decode()
        ids = json.loads(v) if v else []
        parsed.append(ids)
        counts[i] = len(ids)
    offsets = np.zeros(len(raw), dtype=np.int32)
    if len(raw):
        offsets[1:] = np.cumsum(counts)[:-1]
    flat = np.fromiter(
        (pid for ids in parsed for pid in ids),
        dtype=np.int32,
        count=int(counts.sum()),
    )
    return flat, offsets, counts


class ScaleGenerator:
    """Tile ``source`` up to at least ``target_people`` people."""

    def __init__(self, source: str | Path, target_people: int):
        self.source = Path(source)
        self.target_people = int(target_people)

    def generate(self, output: str | Path) -> dict:
        output = Path(output)
        with h5py.File(self.source, "r") as src:
            # Use real dataset lengths, not the metadata attrs: the MAY
            # serializer's num_venues attr can differ from the venue array
            # length (duplicate venues), which would break broadcasting.
            P = _src_len(src, "population/ids")
            G = _src_len(src, "geography/ids")
            V = _src_len(src, "venues/ids")
            K = max(1, math.ceil(self.target_people / P))

            logger.info(
                "Tiling %s (P=%d V=%d G=%d) x%d -> %d people",
                self.source.name, P, V, G, K, K * P,
            )

            # Per-type venue counts: ranks_in_type must stay globally unique,
            # so replica r's ranks are offset by r * (count of that type).
            v_types = src["venues/types"][:]
            type_count = np.bincount(v_types).astype(np.int64)

            # Replica coordinate offsets: lay replicas on a square grid so the
            # map shows K distinct clusters instead of one overplotted blob.
            lat = src["geography/latitudes"][:]
            lon = src["geography/longitudes"][:]
            lon_span = float(np.nanmax(lon) - np.nanmin(lon)) or 1.0
            lat_span = float(np.nanmax(lat) - np.nanmin(lat)) or 1.0
            grid_cols = math.ceil(math.sqrt(K))
            dx = lon_span * 1.15
            dy = lat_span * 1.15

            with h5py.File(output, "w") as dst:
                self._copy_invariant(src, dst)
                self._init_attrs(src, dst, K, P, V, G)
                self._tile_friendships(src, dst, K, P)
                self._tile_arrays(src, dst, K, P, V, G, type_count,
                                  grid_cols, dx, dy)

        stats = dict(replicas=K, num_people=K * P,
                     num_venues=K * V, num_geo_units=K * G,
                     output=str(output),
                     size_bytes=output.stat().st_size)
        logger.info("Wrote %s (%.1f MB, %d people)",
                    output.name, stats["size_bytes"] / 1e6, stats["num_people"])
        return stats

    # ------------------------------------------------------------------ #
    # Replica-invariant data: copied once, verbatim.
    # ------------------------------------------------------------------ #
    def _copy_invariant(self, src: h5py.File, dst: h5py.File) -> None:
        src.copy("metadata/registries", dst, name="metadata/registries")
        src.copy("activity_mappings/activity_map/activity_names", dst,
                 name="activity_mappings/activity_map/activity_names")

    def _init_attrs(self, src, dst, K, P, V, G) -> None:
        for k, v in src.attrs.items():
            dst.attrs[k] = v
        dst.attrs["num_people"] = K * P
        dst.attrs["num_venues"] = K * V
        dst.attrs["num_geo_units"] = K * G
        dst.attrs["synth_replicas"] = K
        dst.attrs["synth_source"] = str(self.source.name)

    # ------------------------------------------------------------------ #
    # friendships JSON-string -> typed CSR (the schema improvement).
    # ------------------------------------------------------------------ #
    def _tile_friendships(self, src, dst, K, P) -> None:
        path = "population/properties/friendships"
        if path not in src:
            return
        flat0, off0, cnt0 = _friendships_to_csr(src[path][:])
        F = len(flat0)
        g = dst.create_group("population/friendships")
        d_flat = g.create_dataset("flat", shape=(K * F,), dtype=np.int32, **_GZIP)
        d_off = g.create_dataset("offsets", shape=(K * P,), dtype=np.int32, **_GZIP)
        d_cnt = g.create_dataset("counts", shape=(K * P,), dtype=np.int32, **_GZIP)
        for r in range(K):
            d_flat[r * F:(r + 1) * F] = flat0 + r * P
            d_off[r * P:(r + 1) * P] = off0 + r * F
            d_cnt[r * P:(r + 1) * P] = cnt0
        logger.info("  friendships -> CSR (%d edges/replica)", F)

    # ------------------------------------------------------------------ #
    # Everything else: tiled with per-replica offsets.
    # ------------------------------------------------------------------ #
    def _tile_arrays(self, src, dst, K, P, V, G, type_count,
                     grid_cols, dx, dy) -> None:
        S = _src_len(src, "venues/subsets/venue_ids")
        M = _src_len(src, "venues/subsets/members_flat")
        A = _src_len(src, "activity_mappings/activity_map/activity_data")

        # path -> (per-replica length, transform(arr, r) -> arr)
        ID = lambda off: (lambda a, r, o=off: a + r * o)
        PAR = lambda off: (lambda a, r, o=off: np.where(a < 0, a, a + r * o))
        COPY = lambda a, r: a

        def coord(axis):  # axis 0=lat(y) 1=lon(x)
            def fn(a, r):
                row, col = divmod(r, grid_cols)
                return a + np.float32(row * dy if axis == 0 else col * dx)
            return fn

        plan = {
            # geography
            "geography/ids": (G, ID(G)),
            "geography/parent_ids": (G, PAR(G)),
            "geography/levels": (G, COPY),
            "geography/latitudes": (G, coord(0)),
            "geography/longitudes": (G, coord(1)),
            # population core
            "population/ids": (P, ID(P)),
            "population/geo_unit_ids": (P, ID(G)),
            "population/ages": (P, COPY),
            "population/sexes": (P, COPY),
            "population/partition_index/geo_unit_ids":
                (_src_len(src, "population/partition_index/geo_unit_ids"), ID(G)),
            "population/partition_index/start_indices":
                (_src_len(src, "population/partition_index/start_indices"), ID(P)),
            "population/partition_index/counts":
                (_src_len(src, "population/partition_index/counts"), COPY),
            # venues core
            "venues/ids": (V, ID(V)),
            "venues/parent_ids": (V, PAR(V)),
            "venues/types": (V, COPY),
            "venues/is_residence": (V, COPY),
            "venues/geo_unit_ids": (V, ID(G)),
            "venues/latitudes": (V, coord(0)),
            "venues/longitudes": (V, coord(1)),
            "venues/ranks_in_type": (V, self._ranks_fn(src, type_count)),
            "venues/partition_index/geo_unit_ids":
                (_src_len(src, "venues/partition_index/geo_unit_ids"), ID(G)),
            "venues/partition_index/start_indices":
                (_src_len(src, "venues/partition_index/start_indices"), ID(V)),
            "venues/partition_index/counts":
                (_src_len(src, "venues/partition_index/counts"), COPY),
            # subsets
            "venues/subsets/venue_ids": (S, ID(V)),
            "venues/subsets/subset_indices": (S, COPY),
            "venues/subsets/member_counts": (S, COPY),
            "venues/subsets/members_flat": (M, ID(P)),
            "venues/subsets/members_offsets": (S, ID(M)),
            "venues/subsets/partition_index/geo_unit_ids":
                (_src_len(src, "venues/subsets/partition_index/geo_unit_ids"), ID(G)),
            "venues/subsets/partition_index/start_indices":
                (_src_len(src, "venues/subsets/partition_index/start_indices"), ID(S)),
            "venues/subsets/partition_index/counts":
                (_src_len(src, "venues/subsets/partition_index/counts"), COPY),
            "venues/subsets/members_partition_index/geo_unit_ids":
                (_src_len(src, "venues/subsets/members_partition_index/geo_unit_ids"), ID(G)),
            "venues/subsets/members_partition_index/start_indices":
                (_src_len(src, "venues/subsets/members_partition_index/start_indices"), ID(M)),
            "venues/subsets/members_partition_index/counts":
                (_src_len(src, "venues/subsets/members_partition_index/counts"), COPY),
            # activity map (cols: person_id, activity_idx, venue_id, subset_idx)
            "activity_mappings/activity_map/activity_data": (A, self._activity_fn(P, V)),
            "activity_mappings/activity_map/activity_offsets": (P, ID(A)),
            "activity_mappings/activity_map/partition_index/geo_unit_ids":
                (_src_len(src, "activity_mappings/activity_map/partition_index/geo_unit_ids"), ID(G)),
            "activity_mappings/activity_map/partition_index/start_indices":
                (_src_len(src, "activity_mappings/activity_map/partition_index/start_indices"), ID(A)),
            "activity_mappings/activity_map/partition_index/counts":
                (_src_len(src, "activity_mappings/activity_map/partition_index/counts"), COPY),
        }

        for path, (n, fn) in plan.items():
            self._tile_one(src, dst, path, n, fn, K)

        # String name arrays + venue per-type property groups: pure tiling.
        for path in ("metadata/names/geography", "metadata/names/venues",
                     "metadata/names/subsets"):
            if path in src:
                self._tile_one(src, dst, path, _src_len(src, path), COPY, K)
        self._tile_string_friendship_drop(src, dst, K)  # other pop properties
        self._tile_venue_properties(src, dst, K)

    def _ranks_fn(self, src, type_count):
        types = src["venues/types"][:]
        per_v_count = type_count[types].astype(np.int32)

        def fn(a, r):
            return a + np.int32(r) * per_v_count
        return fn

    def _activity_fn(self, P, V):
        def fn(a, r):
            out = a.copy()
            out[:, 0] += r * P  # person id
            out[:, 2] += r * V  # venue id
            return out
        return fn

    def _tile_one(self, src, dst, path, n, fn, K) -> None:
        sd = src[path]
        shape = (K * n,) + sd.shape[1:]
        is_str = sd.dtype.kind in ("O", "S", "U")
        kw = {} if is_str else _GZIP
        dd = dst.create_dataset(path, shape=shape, dtype=sd.dtype, **kw)
        for r in range(K):
            block = sd[:] if sd.ndim == 1 else sd[:, :]
            dd[r * n:(r + 1) * n] = block if is_str else fn(block, r)

    def _tile_string_friendship_drop(self, src, dst, K) -> None:
        """Copy population/properties/* except friendships (replaced by CSR)."""
        grp = src.get("population/properties")
        if grp is None:
            return
        for name in grp:
            if name == "friendships":
                continue
            self._tile_one(src, dst, f"population/properties/{name}",
                           _src_len(src, f"population/properties/{name}"),
                           lambda a, r: a, K)

    def _tile_venue_properties(self, src, dst, K) -> None:
        grp = src.get("venues/properties")
        if grp is None:
            return
        for vtype in grp:
            for prop in grp[vtype]:
                path = f"venues/properties/{vtype}/{prop}"
                self._tile_one(src, dst, path, _src_len(src, path),
                               lambda a, r: a, K)


def generate(source: str | Path, target_people: int, output: str | Path) -> dict:
    return ScaleGenerator(source, target_people).generate(output)
