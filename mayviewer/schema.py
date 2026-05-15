"""Schema introspection — the only place the viewer learns what a world contains.

Every later phase (prep, server, frontend payloads) is built on top of this so
that no domain term is ever hardcoded. Geography level names, venue types,
subset roles and property names are all read from the world's own
``metadata/registries`` and group layout. A world with no schools, or a New
Zealand world with different geography levels, is described purely by what its
file declares.

The *only* fixed assumption is MAY's container layout (population / venues /
geography / activity_mappings, each with a partition_index) — that is the MAY
serializer contract, not a domain assumption. See
MAY/may/serialization/world_serializer.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import h5py


def _strs(node) -> list[str]:
    """Decode an HDF5 string dataset to a list of str."""
    return [v.decode() if isinstance(v, bytes) else str(v) for v in node[:]]


@dataclass
class WorldSchema:
    """Everything domain-specific about a world, discovered dynamically."""

    num_people: int
    num_venues: int
    num_geo_units: int
    # Registry index -> label, exactly as the world declares them.
    geo_levels: list[str] = field(default_factory=list)
    venue_types: list[str] = field(default_factory=list)
    subset_names: list[str] = field(default_factory=list)
    sex_mapping: dict[str, int] = field(default_factory=dict)
    # Discovered groups — never assumed.
    person_properties: list[str] = field(default_factory=list)
    person_relations: list[str] = field(default_factory=list)  # typed CSR groups
    venue_properties: dict[str, list[str]] = field(default_factory=dict)
    geo_properties: list[str] = field(default_factory=list)
    activity_names: list[str] = field(default_factory=list)
    attrs: dict = field(default_factory=dict)

    def label_geo_level(self, idx: int) -> str:
        return self.geo_levels[idx] if 0 <= idx < len(self.geo_levels) else str(idx)

    def label_venue_type(self, idx: int) -> str:
        return self.venue_types[idx] if 0 <= idx < len(self.venue_types) else str(idx)


def _registry_strs(f: h5py.File, name: str) -> list[str]:
    node = f.get(f"metadata/registries/{name}")
    return _strs(node) if isinstance(node, h5py.Dataset) else []


def _sex_mapping(f: h5py.File) -> dict[str, int]:
    node = f.get("metadata/registries/sex")
    if node is not None and "mapping" in node.attrs:
        raw = node.attrs["mapping"]
        raw = raw.decode() if isinstance(raw, bytes) else raw
        out = {}
        for pair in raw.split(","):
            k, _, v = pair.partition(":")
            if v:
                out[k] = int(v)
        return out
    return {}


def describe(path: str | Path) -> WorldSchema:
    """Open a MAY world read-only and return its discovered schema."""
    with h5py.File(path, "r") as f:
        pop_props = f.get("population/properties")
        relations = f.get("population")
        venue_props = f.get("venues/properties")
        geo_props = f.get("geography/properties")
        return WorldSchema(
            num_people=int(f["population/ids"].shape[0]),
            num_venues=int(f["venues/ids"].shape[0]),
            num_geo_units=int(f["geography/ids"].shape[0]),
            geo_levels=_registry_strs(f, "geo_levels"),
            venue_types=_registry_strs(f, "venue_types"),
            subset_names=_registry_strs(f, "subset_names"),
            sex_mapping=_sex_mapping(f),
            person_properties=sorted(pop_props.keys()) if pop_props else [],
            # Typed CSR relation groups (e.g. friendships): a subgroup with
            # flat/offsets/counts rather than a flat property array.
            person_relations=sorted(
                k for k in (relations.keys() if relations else [])
                if isinstance(relations[k], h5py.Group)
                and {"flat", "offsets"} <= set(relations[k].keys())
            ),
            venue_properties={
                vt: sorted(venue_props[vt].keys()) for vt in venue_props
            } if venue_props else {},
            geo_properties=sorted(geo_props.keys()) if geo_props else [],
            activity_names=_strs(f["activity_mappings/activity_map/activity_names"])
            if "activity_mappings/activity_map/activity_names" in f else [],
            attrs={k: (v.item() if hasattr(v, "item") else v)
                   for k, v in f.attrs.items()},
        )
