"""Phase 2 — streaming prep pipeline.

Turns a MAY ``world_state.h5`` into cached artifacts (aggregates, drill-down
shards, H3 hexbin / PMTiles) without ever loading the whole file: every read
goes through :mod:`mayviewer.prep.reader`, which slices one leaf geo unit at a
time via the file's own ``partition_index``. Peak memory is bounded by the
largest single leaf unit, so the pipeline runs at 60M agents unchanged.

All domain knowledge (geo level names, venue types, property names, the
friendships representation) comes from :mod:`mayviewer.schema`; nothing here
hardcodes a domain term.
"""

from .pipeline import cache_dir, prep

__all__ = ["prep", "cache_dir"]
