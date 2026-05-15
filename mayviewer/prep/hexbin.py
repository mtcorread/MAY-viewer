"""Multi-resolution H3 hexbin pyramid — the map density layer.

Population rows carry no per-person coordinate in MAY; a person is located by
their geo unit. So the *people* layer bins each leaf unit's centroid weighted
by its population — which comes straight from the ``partition_index`` counts
array, i.e. **zero bulk reads**. The *venues* layer bins real venue
coordinates, swept one bounded partition slice at a time.

Cost is O(points) for the finest level plus O(populated cells) to roll up to
coarser zooms via ``cell_to_parent`` — never O(people) at coarse zoom — so the
pyramid is the same size and speed whether the world is 1k or 60M agents. That
is what lets the browser fetch only the cells in view at its current zoom and
never the whole world.
"""

from __future__ import annotations

import math

import h3
import numpy as np

# Coarse→fine. Roughly: r3 ~ continent, r5 ~ county, r7 ~ town, r9 ~ street.
# Only the finest is binned from points; the rest are parent rollups.
DEFAULT_RES = (3, 5, 7, 9)


def _valid(lat: float, lon: float) -> bool:
    return (
        lat is not None and lon is not None
        and not math.isnan(lat) and not math.isnan(lon)
        and -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0
    )


def _pyramid(finest: dict[str, int], resolutions) -> dict[int, dict[str, int]]:
    """Roll a finest-resolution cell→count map up to every coarser res."""
    res = sorted(resolutions)
    fine = res[-1]
    out: dict[int, dict[str, int]] = {fine: finest}
    for r in res[:-1]:
        agg: dict[str, int] = {}
        for cell, n in finest.items():
            p = h3.cell_to_parent(cell, r)
            agg[p] = agg.get(p, 0) + n
        out[r] = agg
    return out


def build(reader, schema, geo, resolutions=DEFAULT_RES) -> dict:
    """Return ``{layer: {res: {h3_cell: count}}}`` for people and venues."""
    res = sorted(resolutions)
    fine = res[-1]

    # -- people: leaf-unit centroid weighted by partition count (no bulk read).
    lat = reader.geo_full("latitudes")
    lon = reader.geo_full("longitudes")
    geo_ids = geo.ids
    pos = {int(g): i for i, g in enumerate(geo_ids)}
    people_fine: dict[str, int] = {}
    for gid, _s, cnt in reader.partition("population"):
        if cnt == 0:
            continue
        i = pos.get(int(gid))
        if i is None:
            continue
        la, lo = float(lat[i]), float(lon[i])
        if not _valid(la, lo):
            continue
        cell = h3.latlng_to_cell(la, lo, fine)
        people_fine[cell] = people_fine.get(cell, 0) + int(cnt)

    # -- venues: use a venue's own coordinate when it has one; otherwise fall
    # back to its geo-unit centroid. In MAY only "destination" venue types
    # carry explicit coords; residences/workplaces are geo-located, exactly
    # like people. Swept one bounded slice at a time.
    venues_fine: dict[str, int] = {}
    for gid, s, c in reader.partition("venues"):
        if c == 0:
            continue
        vlat = reader.slice("venues/latitudes", s, c)
        vlon = reader.slice("venues/longitudes", s, c)
        gi = pos.get(int(gid))
        glat = float(lat[gi]) if gi is not None else float("nan")
        glon = float(lon[gi]) if gi is not None else float("nan")
        unit_cell = (
            h3.latlng_to_cell(glat, glon, fine) if _valid(glat, glon) else None
        )
        for la, lo in zip(vlat.tolist(), vlon.tolist()):
            if _valid(la, lo):
                cell = h3.latlng_to_cell(la, lo, fine)
            elif unit_cell is not None:
                cell = unit_cell
            else:
                continue
            venues_fine[cell] = venues_fine.get(cell, 0) + 1

    return {
        "people": _pyramid(people_fine, res),
        "venues": _pyramid(venues_fine, res),
        "resolutions": res,
    }
