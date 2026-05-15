"""Self-contained PMTiles v3 + Mapbox-Vector-Tile writer (no external binary).

Turns the H3 hexbin pyramid into a single ``.pmtiles`` archive of vector
tiles. PMTiles is one static, range-readable file: the browser (MapLibre via
the pmtiles protocol) fetches only the tiles in view at the current zoom, so
the map never ships the whole world — and a CDN can host it with no server,
which is philosophy non-negotiable #3.

Implemented from the specs directly so the packaged CLI needs nothing beyond
its Python deps:
  * MVT 2.1 (protobuf, integer tile coords, command/zigzag geometry)
  * PMTiles v3 (127-byte header, Hilbert tile ids, varint directory)

Each H3 resolution is rendered at one web-mercator zoom; a hexagon is written
into the tile containing its centroid (small controlled overflow at tile
edges, which MVT permits and renderers clip) so we never clip polygons.
"""

from __future__ import annotations

import gzip
import json
import math
import struct
from pathlib import Path

import h3

# H3 resolution -> web mercator zoom it is rendered at (coarse hex, low zoom).
RES_TO_ZOOM = {3: 4, 5: 7, 7: 10, 9: 13}
EXTENT = 4096
# Boundary polygons are clipped per tile; clipping to the *exact* tile box
# would turn every tile edge into a real polygon edge, which the line layer
# then strokes as a visible grid. Clip to a slightly buffered box instead:
# the artificial cut edges fall outside the visible 0..EXTENT tile area
# (MapLibre clips drawn output to the tile, the neighbouring tile overlaps
# the seam), so only true boundaries are stroked. Fraction of tile span.
BOUNDARY_TILE_BUFFER = 0.0625


# --------------------------------------------------------------------------- #
# protobuf primitives
# --------------------------------------------------------------------------- #
def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _key(field: int, wire: int) -> bytes:
    return _varint((field << 3) | wire)


def _ld(field: int, payload: bytes) -> bytes:
    """A length-delimited (wire type 2) field."""
    return _key(field, 2) + _varint(len(payload)) + payload


def _vfield(field: int, value: int) -> bytes:
    return _key(field, 0) + _varint(value)


def _zigzag(n: int) -> int:
    return (n << 1) ^ (n >> 31)


# --------------------------------------------------------------------------- #
# MVT tile encoding
# --------------------------------------------------------------------------- #
def _project(lon: float, lat: float, z: int) -> tuple[float, float]:
    """lon/lat -> global pixel coords (in tile-extent units) at zoom z."""
    n = 1 << z
    x = (lon + 180.0) / 360.0 * n
    siny = math.sin(math.radians(lat))
    siny = min(max(siny, -0.9999), 0.9999)
    y = (0.5 - math.log((1 + siny) / (1 - siny)) / (4 * math.pi)) * n
    return x * EXTENT, y * EXTENT


def _geometry(ring_px: list[tuple[int, int]]) -> bytes:
    """Encode one exterior polygon ring as MVT command integers."""
    cmds: list[int] = []
    cx = cy = 0
    # MoveTo first vertex.
    fx, fy = ring_px[0]
    cmds.append((1 & 0x7) | (1 << 3))
    cmds.append(_zigzag(fx - cx))
    cmds.append(_zigzag(fy - cy))
    cx, cy = fx, fy
    # LineTo the remaining vertices.
    rest = ring_px[1:]
    cmds.append((2 & 0x7) | (len(rest) << 3))
    for x, y in rest:
        cmds.append(_zigzag(x - cx))
        cmds.append(_zigzag(y - cy))
        cx, cy = x, y
    # ClosePath.
    cmds.append((7 & 0x7) | (1 << 3))
    out = bytearray()
    for c in cmds:
        out += _varint(c)
    return bytes(out)


def _signed_area(ring: list[tuple[int, int]]) -> int:
    """Twice the signed area in tile pixel space (y points down)."""
    a = 0
    n = len(ring)
    for i in range(n):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % n]
        a += x1 * y2 - x2 * y1
    return a


def _ring_cmds(ring: list[tuple[int, int]], cx: int, cy: int,
               exterior: bool) -> tuple[list[int], int, int]:
    """Encode one closed ring continuing from cursor (cx, cy). MVT 2.1: an
    exterior ring must have positive area and a hole negative area in tile
    space — enforce by reversing when the winding is wrong."""
    pts = [p for i, p in enumerate(ring)
           if i == 0 or p != ring[i - 1]]            # drop repeats
    if len(pts) >= 2 and pts[0] == pts[-1]:
        pts = pts[:-1]                                # MVT closes implicitly
    if len(pts) < 3:
        return [], cx, cy
    area = _signed_area(pts)
    if (exterior and area < 0) or (not exterior and area > 0):
        pts = [pts[0]] + pts[1:][::-1]
    cmds: list[int] = []
    fx, fy = pts[0]
    cmds += [(1 & 0x7) | (1 << 3), _zigzag(fx - cx), _zigzag(fy - cy)]
    cx, cy = fx, fy
    rest = pts[1:]
    cmds.append((2 & 0x7) | (len(rest) << 3))
    for x, y in rest:
        cmds += [_zigzag(x - cx), _zigzag(y - cy)]
        cx, cy = x, y
    cmds.append((7 & 0x7) | (1 << 3))                 # ClosePath
    return cmds, cx, cy


def _multi_geometry(rings_px: list[list[tuple[int, int]]]) -> bytes:
    """Encode [exterior, hole, hole, ...] (one polygon) as MVT commands.
    The first ring is the exterior; the rest are holes."""
    cmds: list[int] = []
    cx = cy = 0
    for idx, ring in enumerate(rings_px):
        rc, cx, cy = _ring_cmds(ring, cx, cy, exterior=(idx == 0))
        cmds += rc
    out = bytearray()
    for c in cmds:
        out += _varint(c)
    return bytes(out)


class _LayerBuilder:
    """Accumulates features for one MVT layer in one tile."""

    def __init__(self, name: str):
        self.name = name
        self._feats: list[bytes] = []
        self._keys: list[str] = []
        self._key_idx: dict[str, int] = {}
        self._vals: list[bytes] = []
        self._val_idx: dict[int, int] = {}

    def _k(self, key: str) -> int:
        if key not in self._key_idx:
            self._key_idx[key] = len(self._keys)
            self._keys.append(key)
        return self._key_idx[key]

    def _v(self, value) -> int:
        # Key by (type, value) so an int 1 and string "1" never collide.
        vkey = (type(value).__name__, value)
        if vkey not in self._val_idx:
            self._val_idx[vkey] = len(self._vals)
            if isinstance(value, bool):
                self._vals.append(_key(7, 0) + _varint(1 if value else 0))
            elif isinstance(value, int):
                self._vals.append(_vfield(4, value))  # Value.int_value = 4
            else:
                # Value.string_value = field 1, length-delimited.
                self._vals.append(_ld(1, str(value).encode()))
        return self._val_idx[vkey]

    def add(self, ring_px, props: dict[str, int]) -> None:
        """Single-ring polygon (the hexbin path)."""
        self._emit([ring_px], props)

    def add_polygon(self, rings_px: list[list[tuple[int, int]]], props: dict) -> None:
        """A polygon as [exterior, hole, hole, ...] (one MVT polygon)."""
        if rings_px:
            self._emit(rings_px, props)

    def _emit(self, rings_px: list[list[tuple[int, int]]], props: dict) -> None:
        tags: list[int] = []
        for k, v in props.items():
            tags += [self._k(k), self._v(v)]
        body = bytearray()
        tag_bytes = b"".join(_varint(t) for t in tags)
        body += _ld(2, tag_bytes)             # tags (field 2)
        body += _vfield(3, 3)                 # type = POLYGON (field 3)
        body += _ld(4, _multi_geometry(rings_px))  # geometry (field 4)
        self._feats.append(bytes(body))

    def is_empty(self) -> bool:
        return not self._feats

    def encode(self) -> bytes:
        out = bytearray()
        out += _ld(1, self.name.encode())            # name = 1
        for f in self._feats:
            out += _ld(2, f)                         # features = 2
        for k in self._keys:
            out += _ld(3, k.encode())                # keys = 3
        for v in self._vals:
            out += _ld(4, v)                         # values = 4
        out += _vfield(5, EXTENT)                    # extent = 5
        out += _vfield(15, 2)                        # version = 15
        return bytes(out)


def _encode_tile(layers: list[_LayerBuilder]) -> bytes:
    return b"".join(_ld(3, lb.encode()) for lb in layers if not lb.is_empty())


# --------------------------------------------------------------------------- #
# PMTiles v3 archive
# --------------------------------------------------------------------------- #
def _zxy_to_tileid(z: int, x: int, y: int) -> int:
    acc = ((1 << (2 * z)) - 1) // 3  # tiles in all lower zooms
    n = 1 << z
    d = 0
    s = n >> 1
    tx, ty = x, y
    while s > 0:
        rx = 1 if (tx & s) > 0 else 0
        ry = 1 if (ty & s) > 0 else 0
        d += s * s * ((3 * rx) ^ ry)
        if ry == 0:
            if rx == 1:
                tx = s - 1 - tx
                ty = s - 1 - ty
            tx, ty = ty, tx
        s >>= 1
    return acc + d


def _serialize_directory(entries: list[tuple[int, int, int, int]]) -> bytes:
    """entries: sorted (tile_id, offset, length, run_length) -> v3 directory."""
    out = bytearray()
    out += _varint(len(entries))
    last = 0
    for tid, _o, _l, _r in entries:
        out += _varint(tid - last)
        last = tid
    for _t, _o, _l, r in entries:
        out += _varint(r)
    for _t, _o, l, _r in entries:
        out += _varint(l)
    prev_off = prev_len = 0
    for i, (_t, o, l, _r) in enumerate(entries):
        if i > 0 and o == prev_off + prev_len:
            out += _varint(0)
        else:
            out += _varint(o + 1)
        prev_off, prev_len = o, l
    return bytes(out)


def _header(**f) -> bytes:
    h = bytearray(127)
    h[0:7] = b"PMTiles"
    h[7] = 3
    struct.pack_into(
        "<QQQQQQQQQQQ", h, 8,
        f["root_off"], f["root_len"], f["meta_off"], f["meta_len"],
        f["leaf_off"], f["leaf_len"], f["data_off"], f["data_len"],
        f["n_addr"], f["n_entries"], f["n_contents"],
    )
    h[96] = 1               # clustered
    h[97] = 2               # internal compression = gzip (dirs + metadata)
    h[98] = 1               # tile compression = none
    h[99] = 1               # tile type = mvt
    h[100] = f["min_zoom"]
    h[101] = f["max_zoom"]
    _i32 = lambda v: max(-2147483648, min(2147483647, int(v)))
    struct.pack_into(
        "<iiii", h, 102,
        _i32(f["min_lon_e7"]), _i32(f["min_lat_e7"]),
        _i32(f["max_lon_e7"]), _i32(f["max_lat_e7"]),
    )
    h[118] = f["center_zoom"]
    struct.pack_into("<ii", h, 119, f["center_lon_e7"], f["center_lat_e7"])
    return bytes(h)


def write_pmtiles(hexlayers: dict, out_path: Path,
                  res_to_zoom: dict[int, int] = RES_TO_ZOOM) -> dict:
    """Render the hexbin pyramid to a PMTiles archive. Returns summary stats."""
    resolutions = [r for r in hexlayers["resolutions"] if r in res_to_zoom]
    layer_names = [k for k in hexlayers if k not in ("resolutions",)]

    # tile (z,x,y) -> {layer_name: _LayerBuilder}
    tiles: dict[tuple[int, int, int], dict[str, _LayerBuilder]] = {}
    minlat = minlon = 1e9
    maxlat = maxlon = -1e9

    for layer in layer_names:
        for res in resolutions:
            z = res_to_zoom[res]
            for cell, count in hexlayers[layer][res].items():
                clat, clon = h3.cell_to_latlng(cell)
                minlat, maxlat = min(minlat, clat), max(maxlat, clat)
                minlon, maxlon = min(minlon, clon), max(maxlon, clon)
                gx, gy = _project(clon, clat, z)
                tx, ty = int(gx // EXTENT), int(gy // EXTENT)
                ring = h3.cell_to_boundary(cell)  # [(lat,lon), ...] CCW
                ox, oy = tx * EXTENT, ty * EXTENT
                ring_px = []
                for blat, blon in ring:
                    bx, by = _project(blon, blat, z)
                    ring_px.append((round(bx - ox), round(by - oy)))
                key = (z, tx, ty)
                tl = tiles.setdefault(key, {})
                lb = tl.get(layer)
                if lb is None:
                    lb = tl[layer] = _LayerBuilder(layer)
                lb.add(ring_px, {"count": count, "res": res})

    # Assemble tile blobs in Hilbert tile-id order (clustered).
    blobs: list[tuple[int, bytes]] = []
    for (z, x, y), tl in tiles.items():
        data = _encode_tile([tl[n] for n in layer_names if n in tl])
        blobs.append((_zxy_to_tileid(z, x, y), data))
    blobs.sort(key=lambda t: t[0])

    data_buf = bytearray()
    entries: list[tuple[int, int, int, int]] = []
    for tid, blob in blobs:
        entries.append((tid, len(data_buf), len(blob), 1))
        data_buf += blob

    # Directories and metadata are gzipped (internal_compression = 2), per the
    # PMTiles convention the JS/Python readers expect. Tiles stay uncompressed.
    root_dir = gzip.compress(_serialize_directory(entries))
    zooms = [res_to_zoom[r] for r in resolutions] or [0]
    meta = gzip.compress(json.dumps({
        "vector_layers": [
            {"id": n, "fields": {"count": "Number", "res": "Number"},
             "minzoom": min(zooms), "maxzoom": max(zooms)}
            for n in layer_names
        ],
        "attribution": "MAY-viewer",
    }).encode())

    HLEN = 127
    root_off = HLEN
    meta_off = root_off + len(root_dir)
    data_off = meta_off + len(meta)
    if minlat > maxlat:  # no cells
        minlat = maxlat = minlon = maxlon = 0.0
    header = _header(
        root_off=root_off, root_len=len(root_dir),
        meta_off=meta_off, meta_len=len(meta),
        leaf_off=0, leaf_len=0,
        data_off=data_off, data_len=len(data_buf),
        n_addr=len(entries), n_entries=len(entries), n_contents=len(entries),
        min_zoom=min(zooms), max_zoom=max(zooms),
        min_lon_e7=int(minlon * 1e7), min_lat_e7=int(minlat * 1e7),
        max_lon_e7=int(maxlon * 1e7), max_lat_e7=int(maxlat * 1e7),
        center_zoom=min(zooms),
        center_lon_e7=int((minlon + maxlon) / 2 * 1e7),
        center_lat_e7=int((minlat + maxlat) / 2 * 1e7),
    )

    out_path = Path(out_path)
    with open(out_path, "wb") as fh:
        fh.write(header)
        fh.write(root_dir)
        fh.write(meta)
        fh.write(data_buf)

    return {
        "tiles": len(entries),
        "zooms": sorted(set(zooms)),
        "bytes": out_path.stat().st_size,
        "layers": layer_names,
    }


# --------------------------------------------------------------------------- #
# Boundary polygons -> PMTiles (real shapes, per-tile clipped, per-zoom simp.)
# --------------------------------------------------------------------------- #
def _tilex_to_lon(x: float, z: int) -> float:
    return x / (1 << z) * 360.0 - 180.0


def _tiley_to_lat(y: float, z: int) -> float:
    t = math.pi * (1 - 2 * y / (1 << z))
    return math.degrees(math.atan(math.sinh(t)))


def _rings_to_px(poly, z: int, ox: int, oy: int) -> list[list[tuple[int, int]]]:
    """A shapely Polygon -> [exterior_px, hole_px, ...] for tile origin ox,oy."""
    out: list[list[tuple[int, int]]] = []
    for ring in [poly.exterior, *poly.interiors]:
        pts: list[tuple[int, int]] = []
        for lon, lat in ring.coords:
            px, py = _project(lon, lat, z)
            pts.append((round(px - ox), round(py - oy)))
        out.append(pts)
    return out


def write_boundary_pmtiles(levels: list[dict], out_path: Path) -> dict:
    """Bake matched boundary polygons into one PMTiles archive.

    ``levels`` items: ``{name, bake_zoom, minzoom, maxzoom, features}`` where
    ``features`` yields ``(geo_id:int, level:int, code:str, shapely_geom)``.
    Each level is tiled at one representative ``bake_zoom``; MapLibre overzooms
    its vector tiles losslessly across ``[minzoom, maxzoom]`` so zooming in
    walks level→level while only viewport tiles are ever fetched. Polygons are
    simplified per zoom and clipped to each covered tile (so a region at low
    zoom or an OA at high zoom never ships a whole-world geometry)."""
    from shapely.geometry import box  # bake-only dep (the optional [geo] extra)
    from shapely import set_precision

    tiles: dict[tuple[int, int, int], dict[str, _LayerBuilder]] = {}
    minlat = minlon = 1e9
    maxlat = maxlon = -1e9
    layer_names: list[str] = []
    layer_meta: list[dict] = []
    feat_count = 0

    for spec in levels:
        name = spec["name"]
        z = int(spec["bake_zoom"])
        layer_names.append(name)
        layer_meta.append({
            "id": name,
            "fields": {"geo_id": "Number", "level": "Number", "code": "String"},
            "minzoom": int(spec["minzoom"]),
            "maxzoom": int(spec["maxzoom"]),
        })
        n = 1 << z
        # ~1.5 px simplification tolerance in degrees at this zoom.
        tol = 1.5 * 360.0 / (n * EXTENT)
        for geo_id, level, code, geom in spec["features"]:
            if geom is None or geom.is_empty:
                continue
            g = geom.simplify(tol, preserve_topology=True)
            if g.is_empty:
                g = geom
            g = set_precision(g, 1e-7)  # drop sub-cm noise; fixes slivers
            if g.is_empty:
                continue
            mnx, mny, mxx, mxy = g.bounds
            minlon, maxlon = min(minlon, mnx), max(maxlon, mxx)
            minlat, maxlat = min(minlat, mny), max(maxlat, mxy)
            tx0 = max(0, int((mnx + 180.0) / 360.0 * n))
            tx1 = min(n - 1, int((mxx + 180.0) / 360.0 * n))
            # y is inverted: max latitude -> smallest tile y.
            ty0 = max(0, int(_project(0.0, mxy, z)[1] // EXTENT))
            ty1 = min(n - 1, int(_project(0.0, mny, z)[1] // EXTENT))
            props = {"geo_id": int(geo_id), "level": int(level),
                     "code": "" if code is None else str(code)}
            for tx in range(tx0, tx1 + 1):
                west = _tilex_to_lon(tx, z)
                east = _tilex_to_lon(tx + 1, z)
                for ty in range(ty0, ty1 + 1):
                    north = _tiley_to_lat(ty, z)
                    south = _tiley_to_lat(ty + 1, z)
                    # Buffer the clip box so tile-cut edges land outside the
                    # visible tile (no stroked grid); true edges are unaffected.
                    bx = (east - west) * BOUNDARY_TILE_BUFFER
                    by = (north - south) * BOUNDARY_TILE_BUFFER
                    clip = box(west - bx, south - by, east + bx, north + by)
                    piece = g.intersection(clip)
                    if piece.is_empty:
                        continue
                    polys = (list(piece.geoms)
                             if piece.geom_type.startswith("Multi")
                             or piece.geom_type == "GeometryCollection"
                             else [piece])
                    ox, oy = tx * EXTENT, ty * EXTENT
                    key = (z, tx, ty)
                    tl = tiles.setdefault(key, {})
                    lb = tl.get(name)
                    if lb is None:
                        lb = tl[name] = _LayerBuilder(name)
                    for pg in polys:
                        if getattr(pg, "geom_type", "") != "Polygon" or pg.is_empty:
                            continue
                        lb.add_polygon(_rings_to_px(pg, z, ox, oy), props)
                        feat_count += 1

    blobs: list[tuple[int, bytes]] = []
    for (z, x, y), tl in tiles.items():
        data = _encode_tile([tl[n] for n in tl])
        blobs.append((_zxy_to_tileid(z, x, y), data))
    blobs.sort(key=lambda t: t[0])

    data_buf = bytearray()
    entries: list[tuple[int, int, int, int]] = []
    for tid, blob in blobs:
        entries.append((tid, len(data_buf), len(blob), 1))
        data_buf += blob

    root_dir = gzip.compress(_serialize_directory(entries))
    bake_zooms = [int(s["bake_zoom"]) for s in levels] or [0]
    all_min = min((int(s["minzoom"]) for s in levels), default=0)
    all_max = max((int(s["maxzoom"]) for s in levels), default=0)
    meta = gzip.compress(json.dumps({
        "vector_layers": layer_meta,
        "attribution": "MAY-viewer",
    }).encode())

    HLEN = 127
    root_off = HLEN
    meta_off = root_off + len(root_dir)
    data_off = meta_off + len(meta)
    if minlat > maxlat:
        minlat = maxlat = minlon = maxlon = 0.0
    header = _header(
        root_off=root_off, root_len=len(root_dir),
        meta_off=meta_off, meta_len=len(meta),
        leaf_off=0, leaf_len=0,
        data_off=data_off, data_len=len(data_buf),
        n_addr=len(entries), n_entries=len(entries), n_contents=len(entries),
        min_zoom=min(bake_zooms), max_zoom=all_max,
        min_lon_e7=int(minlon * 1e7), min_lat_e7=int(minlat * 1e7),
        max_lon_e7=int(maxlon * 1e7), max_lat_e7=int(maxlat * 1e7),
        center_zoom=min(bake_zooms),
        center_lon_e7=int((minlon + maxlon) / 2 * 1e7),
        center_lat_e7=int((minlat + maxlat) / 2 * 1e7),
    )

    out_path = Path(out_path)
    with open(out_path, "wb") as fh:
        fh.write(header)
        fh.write(root_dir)
        fh.write(meta)
        fh.write(data_buf)

    return {
        "path": out_path.name,
        "tiles": len(entries),
        "features": feat_count,
        "bake_zooms": sorted(set(bake_zooms)),
        "minzoom": all_min,
        "maxzoom": all_max,
        "bytes": out_path.stat().st_size,
        "layers": layer_names,
    }
