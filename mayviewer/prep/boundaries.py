"""Optional geographic boundary overlay — country/world-agnostic.

The MAY world stores, per geo unit, a *name* in ``metadata/names/geography``.
What that string is depends on the world and the level: in the 2021 dev world
it is an ONS **code** at MGU/SGU (``E02002570``, ``E00062043``) but a **name**
at LGU/XLGU (``Darlington``, ``North East``). So matching user-supplied
polygons to units is intrinsically *per level*, and the first thing we offer
is a **read-only diagnostic** (`mayviewer match`) that reports the match rate
per level for a chosen feature property — before anything is built.

No domain terms are hardcoded: levels come from the world's own registry, and
the property to match on is chosen/auto-detected, never assumed.

Geometry parsing (for the actual PMTiles bake) is lazy and lives in
``load_geometries`` so the diagnostic needs no geo stack — only stdlib +
``pyshp`` for ``.shp`` attribute tables.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from .reader import WorldReader

logger = logging.getLogger("mayviewer.prep.boundaries")


# ── normalisation ────────────────────────────────────────────────────────

_WS = re.compile(r"\s+")
_PUNCT = re.compile(r"[^0-9a-z]+")


def norm_code(s: str) -> str:
    """Exact-ish key: trim + casefold + collapse whitespace (codes/IDs)."""
    return _WS.sub(" ", str(s).strip()).casefold()


def norm_name(s: str) -> str:
    """Loose key for human names: drop punctuation/whitespace, casefold."""
    return _PUNCT.sub("", str(s).strip().casefold())


# ── world side ───────────────────────────────────────────────────────────

@dataclass
class WorldGeo:
    # level value -> level name (from the world's own registry)
    level_names: dict[int, str]
    # level value -> list of (geo_id, name string)
    units: dict[int, list[tuple[int, str]]]


def world_geo(world: str | Path) -> WorldGeo:
    with WorldReader(world) as r:
        ids = r.geo_full("ids")
        levels = r.geo_full("levels")
        names = r.names("geography")
        reg_path = "metadata/registries/geo_levels"
        reg = r.file[reg_path][:] if reg_path in r.file else None
    if names is None:
        raise SystemExit("World has no metadata/names/geography — cannot match boundaries.")

    def dec(x) -> str:
        return x.decode() if isinstance(x, (bytes, bytearray)) else str(x)

    level_names: dict[int, str] = {}
    if reg is not None:
        for i, nm in enumerate(reg):
            level_names[i] = dec(nm)

    units: dict[int, list[tuple[int, str]]] = {}
    for gid, lv, nm in zip(ids.tolist(), levels.tolist(), names):
        units.setdefault(int(lv), []).append((int(gid), dec(nm)))
    for lv in units:
        level_names.setdefault(lv, f"L{lv}")
    return WorldGeo(level_names=level_names, units=units)


# ── feature side (attributes only; geometry is lazy) ─────────────────────

@dataclass
class Features:
    # property name -> list of values, aligned with `count`
    props: dict[str, list[str]]
    count: int
    source: str


def load_feature_props(path: str | Path) -> Features:
    """Read only feature *attributes* — enough for the match diagnostic."""
    p = Path(path)
    suf = p.suffix.lower()
    if suf in (".geojson", ".json"):
        obj = json.loads(p.read_text())
        feats = obj.get("features", []) if isinstance(obj, dict) else []
        if obj.get("type") == "Topology":
            raise SystemExit(
                "TopoJSON detected — convert to GeoJSON first "
                "(e.g. `mapshaper in.topojson -o out.geojson`)."
            )
        props: dict[str, list[str]] = {}
        for f in feats:
            pr = f.get("properties") or {}
            for k, v in pr.items():
                props.setdefault(k, [])
        for f in feats:
            pr = f.get("properties") or {}
            for k in props:
                v = pr.get(k)
                props[k].append("" if v is None else str(v))
        return Features(props=props, count=len(feats), source=str(p))
    if suf == ".shp":
        try:
            import shapefile  # pyshp
        except ImportError:
            raise SystemExit("Reading .shp needs pyshp:  pip install -e \".[geo]\"")
        sf = shapefile.Reader(str(p))
        fields = [f[0] for f in sf.fields[1:]]  # drop DeletionFlag
        props = {k: [] for k in fields}
        n = 0
        for rec in sf.iterRecords():
            for k in fields:
                props[k].append("" if rec[k] is None else str(rec[k]))
            n += 1
        return Features(props=props, count=n, source=str(p))
    raise SystemExit(f"Unsupported boundary file type: {suf} (use .geojson or .shp)")


# ── matching ──────────────────────────────────────────────────────────────

@dataclass
class LevelMatch:
    level: int
    level_name: str
    world_units: int
    matched: int
    strategy: str  # "code" | "name" | "none"
    prop: str | None
    unmatched_world: list[str] = field(default_factory=list)
    unmatched_feat: list[str] = field(default_factory=list)

    @property
    def rate(self) -> float:
        return self.matched / self.world_units if self.world_units else 0.0


def _match_level(
    world_names: list[str], feat_values: list[str]
) -> tuple[int, str, list[str], list[str]]:
    """Best of exact-code vs loose-name matching for one level."""
    best = (0, "none", world_names, feat_values)
    for strat, fn in (("code", norm_code), ("name", norm_name)):
        fset: dict[str, int] = {}
        for v in feat_values:
            k = fn(v)
            if k:
                fset[k] = fset.get(k, 0) + 1
        matched = 0
        miss_w: list[str] = []
        for nm in world_names:
            if fn(nm) in fset:
                matched += 1
            else:
                miss_w.append(nm)
        if matched > best[0]:
            wkeys = {fn(n) for n in world_names}
            miss_f = sorted({v for v in feat_values if fn(v) not in wkeys})
            best = (matched, strat, miss_w, miss_f)
    return best


def match_report(
    world: str | Path,
    boundary_file: str | Path,
    prop: str | None = None,
    per_level_prop: dict[int, str] | None = None,
) -> list[LevelMatch]:
    """Per-level match report. If `prop` is None, auto-pick the best property
    for each level independently (codes live in different columns per level)."""
    wg = world_geo(world)
    feats = load_feature_props(boundary_file)
    per_level_prop = per_level_prop or {}
    out: list[LevelMatch] = []

    for lv in sorted(wg.units):
        wnames = [nm for _, nm in wg.units[lv]]
        chosen = per_level_prop.get(lv, prop)
        candidates = [chosen] if chosen else list(feats.props.keys())
        best: LevelMatch | None = None
        for cand in candidates:
            if cand not in feats.props:
                continue
            m, strat, mw, mf = _match_level(wnames, feats.props[cand])
            lm = LevelMatch(
                level=lv,
                level_name=wg.level_names.get(lv, f"L{lv}"),
                world_units=len(wnames),
                matched=m,
                strategy=strat if m else "none",
                prop=cand if m else None,
                unmatched_world=mw[:8],
                unmatched_feat=mf[:8],
            )
            if best is None or lm.matched > best.matched:
                best = lm
        if best is None:
            best = LevelMatch(
                level=lv,
                level_name=wg.level_names.get(lv, f"L{lv}"),
                world_units=len(wnames),
                matched=0,
                strategy="none",
                prop=None,
            )
        out.append(best)
    return out


# ── per-level boundary config ────────────────────────────────────────────
#
# Nothing about *which* file / property / strategy a level uses is inferred at
# build time — it is read from an explicit JSON config (the diagnostic only
# *suggests* one). Levels absent from the config get no polygons, so the
# overlay stays optional and a shape-less world still renders.

STRATEGIES = {"code": norm_code, "name": norm_name}


@dataclass(frozen=True)
class LevelBoundaryCfg:
    level_name: str
    file: Path           # resolved absolute path
    prop: str
    strategy: str        # "code" | "name"
    crs: str | None = None  # optional override; else read from the file

    @property
    def norm(self):
        return STRATEGIES[self.strategy]


def read_geojson_crs(path: str | Path) -> str | None:
    """The GeoJSON ``crs`` member name (e.g. 'EPSG:27700'), read from the file
    head only. GeoJSON's default when absent is WGS84 lon/lat, so None means
    'already lon/lat'. Boundary files are NOT assumed to be in any one CRS."""
    p = Path(path)
    if p.suffix.lower() not in (".geojson", ".json"):
        prj = p.with_suffix(".prj")          # shapefile sidecar
        return prj.read_text().strip() if prj.exists() else None
    with open(p, "r", encoding="utf-8") as fh:
        head = fh.read(8192)
    m = re.search(r'"crs"\s*:\s*\{.*?"name"\s*:\s*"([^"]+)"', head, re.S)
    return m.group(1) if m else None


def load_boundary_config(path: str | Path) -> dict[str, LevelBoundaryCfg]:
    """Parse the per-level boundary JSON. Paths resolve relative to the config
    file's directory so a config + its shapes are a portable bundle."""
    p = Path(path).resolve()
    try:
        obj = json.loads(p.read_text())
    except json.JSONDecodeError as e:
        raise SystemExit(f"Boundary config is not valid JSON: {p}\n  {e}")
    levels = obj.get("levels")
    if not isinstance(levels, dict) or not levels:
        raise SystemExit(
            f"Boundary config {p} needs a non-empty 'levels' object "
            '(e.g. {"levels": {"SGU": {"file": "...", "prop": "...", '
            '"strategy": "code"}}}).'
        )
    base = p.parent
    out: dict[str, LevelBoundaryCfg] = {}
    for lvl, spec in levels.items():
        if not isinstance(spec, dict):
            raise SystemExit(f"Boundary config level '{lvl}' must be an object.")
        miss = [k for k in ("file", "prop", "strategy") if not spec.get(k)]
        if miss:
            raise SystemExit(
                f"Boundary config level '{lvl}' is missing {miss} "
                "(each level needs file, prop, strategy)."
            )
        strat = str(spec["strategy"]).lower()
        if strat not in STRATEGIES:
            raise SystemExit(
                f"Boundary config level '{lvl}': strategy '{strat}' unknown "
                f"(use one of {sorted(STRATEGIES)})."
            )
        fp = Path(spec["file"])
        if not fp.is_absolute():
            fp = (base / fp).resolve()
        if not fp.exists():
            raise SystemExit(
                f"Boundary config level '{lvl}': file not found: {fp}"
            )
        crs = spec.get("crs")
        out[str(lvl)] = LevelBoundaryCfg(
            str(lvl), fp, str(spec["prop"]), strat,
            str(crs) if crs else None)
    return out


# ── streaming feature reader (size- AND format-agnostic) ─────────────────
#
# The same code path reads every boundary file regardless of size or which
# level it belongs to — there is deliberately no "is this the big one?"
# branch. It never holds more than one feature (plus a fixed read chunk) in
# memory, and works whether features are one-per-line, few-line, or the whole
# array on a single line. Geometry is JSON-parsed *only* for features whose
# join key matches a wanted world unit (cheap regex pre-filter first), so a
# 27 GB national file costs the same as a tiny one when few units match.

_CHUNK = 1 << 20  # 1 MiB read window


def _scan_object_end(s: str, start: int) -> int | None:
    """Index just past the JSON object that begins at ``s[start] == '{'``,
    or None if ``s`` ends mid-object (caller must read more). String-aware so
    braces inside string values don't miscount; jumps over the huge numeric
    coordinate runs via ``str.find`` (C speed) instead of a Python char loop."""
    depth = 0
    i = start
    n = len(s)
    while i < n:
        nb = s.find("{", i)
        ne = s.find("}", i)
        nq = s.find('"', i)
        cand = [x for x in (nb, ne, nq) if x != -1]
        if not cand:
            return None  # structural char not yet in buffer
        j = min(cand)
        c = s[j]
        if c == '"':  # skip the whole string literal (handle escapes)
            k = j + 1
            while k < n:
                d = s[k]
                if d == "\\":
                    k += 2
                    continue
                if d == '"':
                    break
                k += 1
            else:
                return None  # string not terminated in buffer yet
            i = k + 1
            continue
        if c == "{":
            depth += 1
        else:  # '}'
            depth -= 1
            if depth == 0:
                return j + 1
        i = j + 1
    return None


def iter_features(
    path: str | Path, prop: str, norm, wanted: set[str]
) -> Iterator[tuple[str, dict]]:
    """Yield ``(normalized_key, geometry_dict)`` for each feature whose
    ``prop`` value normalises into ``wanted``. Streaming + bounded memory for
    any file size/layout. ``.shp`` is read via pyshp's record/shape iterator
    (also streaming)."""
    p = Path(path)
    suf = p.suffix.lower()

    if suf == ".shp":
        try:
            import shapefile  # pyshp
        except ImportError:
            raise SystemExit('Reading .shp needs pyshp:  pip install -e ".[geo]"')
        sf = shapefile.Reader(str(p))
        for sr in sf.iterShapeRecords():
            raw = sr.record[prop] if prop in sr.record.as_dict() else None
            if raw is None:
                continue
            k = norm(str(raw))
            if k in wanted:
                yield k, sr.shape.__geo_interface__
        return

    if suf not in (".geojson", ".json"):
        raise SystemExit(f"Unsupported boundary file type: {suf} (use .geojson or .shp)")

    # Quick regex to pull this level's join value out of a feature substring
    # before committing to a full JSON parse of its (possibly huge) geometry.
    pat = re.compile(
        r'"' + re.escape(prop) + r'"\s*:\s*(?:"((?:[^"\\]|\\.)*)"|([^,}\s]+))'
    )

    with open(p, "r", encoding="utf-8") as fh:
        buf = fh.read(_CHUNK)
        # Locate the start of the features array.
        while True:
            m = re.search(r'"features"\s*:\s*\[', buf)
            if m:
                buf = buf[m.end():]
                break
            more = fh.read(_CHUNK)
            if not more:
                return  # no features array at all
            buf = buf[-32:] + more

        eof = False
        while True:
            i = 0
            while i < len(buf) and buf[i] in " \t\r\n,":
                i += 1
            if i >= len(buf):
                if eof:
                    return
                nxt = fh.read(_CHUNK)
                eof = not nxt
                buf = buf[i:] + nxt
                continue
            if buf[i] == "]":
                return  # end of features array
            if buf[i] != "{":
                # Unexpected; skip a char to stay robust rather than crash.
                buf = buf[i + 1:]
                continue
            end = _scan_object_end(buf, i)
            while end is None:
                if eof:
                    return  # truncated file; stop cleanly
                nxt = fh.read(_CHUNK)
                eof = not nxt
                buf = buf[i:] + nxt
                i = 0
                end = _scan_object_end(buf, i)
            feat_src = buf[i:end]
            buf = buf[end:]
            mm = pat.search(feat_src)
            if mm:
                raw = mm.group(1) if mm.group(1) is not None else mm.group(2)
                key = norm(raw)
                if key in wanted:
                    try:
                        geom = json.loads(feat_src).get("geometry")
                    except json.JSONDecodeError:
                        continue
                    if geom:
                        yield key, geom


def print_report(report: list[LevelMatch], feats_count: int, src: str) -> None:
    print(f"\nBoundary match — {src}  ({feats_count:,} features)\n")
    print(f"  {'level':<8}{'units':>7}{'matched':>9}{'rate':>7}  strategy / property")
    print("  " + "-" * 60)
    for r in report:
        flag = "✓" if r.rate >= 0.98 else ("~" if r.rate >= 0.5 else "✗")
        prop = f"{r.strategy}:{r.prop}" if r.prop else "—"
        print(
            f"  {r.level_name:<8}{r.world_units:>7}{r.matched:>9}"
            f"{r.rate*100:>6.0f}% {flag}  {prop}"
        )
    print()
    for r in report:
        if r.rate < 0.98 and r.unmatched_world:
            print(f"  {r.level_name} unmatched world (sample): {r.unmatched_world}")
            if r.unmatched_feat:
                print(f"  {r.level_name} unmatched {r.prop or 'feature'} (sample): {r.unmatched_feat}")
    print()
