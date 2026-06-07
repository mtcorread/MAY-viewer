"""``mayviewer serve`` — the entire Phase-3 backend.

A dependency-free, **Range-capable** static file server. It serves two roots:

* ``/cache/*``  → the prep cache (``manifest.json``, ``*.parquet``,
  ``hexbin.pmtiles``, ``aggregates/``).
* everything else → the bundled single-page app in ``mayviewer/web/dist``.

The browser does *all* data access through HTTP range requests
(hyparquet for Parquet row groups, pmtiles.js for vector tiles), so the
only thing this layer must do correctly is honour ``Range:`` with
``206 Partial Content``. The exact same cache directory can therefore be
dropped onto any static CDN unchanged — there is no bespoke API surface.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import socketserver
import sys
import threading
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger("mayviewer.serve")

# Parquet/PMTiles must be served as opaque bytes; .wasm needs its own type
# so the browser can streaming-compile hyparquet's decompressors.
_MIME = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".mjs": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".wasm": "application/wasm",
    ".parquet": "application/octet-stream",
    ".pmtiles": "application/octet-stream",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".ico": "image/x-icon",
    ".woff2": "font/woff2",
}

_RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)$")

# Optional online basemap (non-negotiable #3: opt-in, never default). The
# core viewer makes ZERO external requests; a basemap appears only when the
# operator explicitly passes `serve --basemap …`. The world file still never
# leaves the machine — only map raster tiles are fetched *from* the provider,
# and only once the user also turns the basemap on in the UI. Mind each
# provider's tile-usage policy; attribution is rendered in the UI.
_BASEMAP_PRESETS: dict[str, dict] = {
    "osm": {
        "name": "OpenStreetMap",
        "tiles": ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
        "tileSize": 256,
        "maxzoom": 19,
        "attribution": "© OpenStreetMap contributors",
    },
    "carto-dark": {
        "name": "Carto Dark Matter",
        "tiles": ["https://basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png"],
        "tileSize": 256,
        "maxzoom": 20,
        "attribution": "© OpenStreetMap contributors, © CARTO",
    },
    "carto-light": {
        "name": "Carto Positron",
        "tiles": ["https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png"],
        "tileSize": 256,
        "maxzoom": 20,
        "attribution": "© OpenStreetMap contributors, © CARTO",
    },
}


def _basemap_spec(arg: str | None) -> dict | None:
    """Resolve ``--basemap`` to a raster-source spec the SPA can consume.

    Accepts a preset name (see ``_BASEMAP_PRESETS``) or a raw XYZ raster URL
    template containing ``{z}/{x}/{y}``. None ⇒ no basemap at all.
    """
    if not arg:
        return None
    if arg in _BASEMAP_PRESETS:
        return _BASEMAP_PRESETS[arg]
    if "{z}" in arg and "{x}" in arg and "{y}" in arg:
        return {
            "name": "Custom basemap",
            "tiles": [arg],
            "tileSize": 256,
            "maxzoom": 22,
            "attribution": "",
        }
    raise SystemExit(
        f"--basemap: expected one of {sorted(_BASEMAP_PRESETS)} or an XYZ "
        f"raster URL template containing {{z}}/{{x}}/{{y}}; got {arg!r}"
    )


def resolve_cache(path: str | Path) -> Path:
    """Accept a cache dir, a world ``.h5``, or a dir holding ``.mayviewer_cache``.

    Returns the directory that contains ``manifest.json``.
    """
    p = Path(path).expanduser().resolve()
    if p.is_dir() and (p / "manifest.json").is_file():
        return p
    if p.is_file() and p.suffix == ".h5":
        # Mirror prep.pipeline.cache_dir without importing the heavy stack.
        c = p.parent / ".mayviewer_cache" / p.stem
        if (c / "manifest.json").is_file():
            return c
        raise SystemExit(
            f"No cache for {p.name} — run `mayviewer prep {p}` first "
            f"(looked in {c})."
        )
    if p.is_dir() and (p / ".mayviewer_cache").is_dir():
        stems = sorted(
            d for d in (p / ".mayviewer_cache").iterdir()
            if (d / "manifest.json").is_file()
        )
        if len(stems) == 1:
            return stems[0]
        if not stems:
            raise SystemExit(f"No prepped worlds under {p / '.mayviewer_cache'}.")
        raise SystemExit(
            "Multiple prepped worlds; point serve at one:\n  "
            + "\n  ".join(str(s) for s in stems)
        )
    raise SystemExit(f"Not a cache, a world .h5, or a world dir: {p}")


def _web_root() -> Path:
    return Path(__file__).parent / "web" / "dist"


_INSPECT_KINDS = ("people", "venues", "members", "activities")


class LazyInspector:
    """Serves per-unit drill-down read live from the source ``.h5``. Holds one
    open :class:`WorldReader`, caches per-kind build context on first use, and
    locks around reads so concurrent requests don't race the single h5py
    handle."""

    def __init__(self, source: Path, home_unit_path: Path | None = None):
        # h5py/pyarrow are imported here, only when a lazy cache is served.
        from .prep.reader import WorldReader
        from .schema import describe
        from .prep import drilldown as dd
        from .prep import personindex

        self._dd = dd
        self._reader = WorldReader(source)
        self._schema = describe(source)
        self._lock = threading.Lock()
        self._ctx: dict[str, object] = {}
        self._vmeta = None  # venue index for activities, built once on demand
        self._personindex = personindex
        self._source = Path(source)
        # person_id -> array row, for resolving venue members by id. Primed in
        # the background (own file handle, so it never blocks navigation reads)
        # so the first member resolve is immediate, not a half-second stall.
        self._person_row = None
        threading.Thread(target=self._prime_row_index, daemon=True).start()
        # Per-kind build context (property classification, schemas, and — for
        # activities — the venue index, a full venue sweep). Built lazily on the
        # first inspect of each kind, which makes that first click a stall.
        # Warm them now on a private handle (like the row index) so the first
        # Inspect of any kind reads only its unit, not the one-time setup.
        threading.Thread(target=self._prime_contexts, daemon=True).start()
        # person_id -> home geo unit, memory-mapped (only touched pages resident)
        # so each member built live can be tagged with the unit whose people
        # shard the frontend should read to resolve it. Absent on pre-v2 caches.
        self._home_lut = (personindex.load_home_units(home_unit_path)
                          if home_unit_path and home_unit_path.is_file() else None)
        self._builders = {
            "people": dd.people_unit_cols,
            "venues": dd.venues_unit_cols,
            "members": dd.members_unit_cols,
            "activities": dd.activities_unit_cols,
        }

    def _ctx_for(self, kind: str):
        ctx = self._ctx.get(kind)
        if ctx is not None:
            return ctx
        dd = self._dd
        if kind == "activities":
            if self._vmeta is None:
                logger.info("  building venue index for activity resolution "
                            "(one-time)…")
                self._vmeta = dd.venue_index(self._reader, self._schema)
            ctx = dd.activities_ctx(self._reader, self._schema, self._vmeta)
        elif kind == "members":
            ctx = dd.members_ctx(self._reader, self._schema, self._home_lut)
        else:
            ctx = {"people": dd.people_ctx,
                   "venues": dd.venues_ctx}[kind](self._reader, self._schema)
        self._ctx[kind] = ctx
        return ctx

    def rows(self, kind: str, gid: int) -> list:
        with self._lock:  # serialize reads on the single h5py handle
            cols = self._builders[kind](self._reader, self._schema,
                                        self._ctx_for(kind), gid)
        return self._dd.unit_records(cols) if cols else []

    def _prime_row_index(self) -> None:
        """Build person_id→row on a private read handle (no lock contention)."""
        try:
            from .prep.reader import WorldReader
            with WorldReader(self._source) as r:
                idx = self._personindex.build_row_index(r)
            if self._person_row is None:
                self._person_row = idx  # benign race: identical array wins
        except Exception:
            logger.exception("person row-index prime failed")

    def _prime_contexts(self) -> None:
        """Pre-build each kind's one-time context on a private handle so the
        first inspect of any kind is immediate, not a stall. The activity venue
        index (a full venue sweep) is the costly one, so it — and the activities
        context — are skipped for worlds with no activity map. Benign races with
        a concurrent first request: both build identical context, last write
        wins (same as ``_prime_row_index``)."""
        try:
            from .prep.reader import WorldReader
            dd = self._dd
            with WorldReader(self._source) as r:
                if "people" not in self._ctx:
                    self._ctx["people"] = dd.people_ctx(r, self._schema)
                if "venues" not in self._ctx:
                    self._ctx["venues"] = dd.venues_ctx(r, self._schema)
                if "members" not in self._ctx:
                    self._ctx["members"] = dd.members_ctx(
                        r, self._schema, self._home_lut)
                if self._schema.activity_names:
                    if self._vmeta is None:
                        self._vmeta = dd.venue_index(r, self._schema)
                    if "activities" not in self._ctx:
                        self._ctx["activities"] = dd.activities_ctx(
                            r, self._schema, self._vmeta)
        except Exception:
            logger.exception("inspector context prime failed")

    def people_by_ids(self, ids: list[int]) -> list:
        """Resolve people by id in one bounded fancy-read — the fast path that
        makes a venue's out-of-unit members appear immediately, instead of the
        frontend fetching every feeder unit whole."""
        pr = self._person_row
        with self._lock:  # serialize reads on the single h5py handle
            if pr is None:  # prime not done yet — build now on the shared handle
                if self._person_row is None:
                    logger.info("  building person_id→row index (one-time)…")
                    self._person_row = self._personindex.build_row_index(self._reader)
                pr = self._person_row
            sel = [int(i) for i in ids if 0 <= int(i) < len(pr)]
            rows = pr[sel] if sel else pr[:0]
            cols = self._dd.people_by_rows(
                self._reader, self._schema, self._ctx_for("people"), rows)
        return self._dd.unit_records(cols) if cols else []

    def activities_by_ids(self, ids: list[int]) -> list:
        """Resolve people's activities by id (per-person ``activity_offsets``
        slice), so a venue member's activities show even when they live outside
        the inspected unit. Builds the venue index once on first use."""
        pr = self._person_row
        with self._lock:
            if pr is None:
                if self._person_row is None:
                    self._person_row = self._personindex.build_row_index(self._reader)
                pr = self._person_row
            sel = [int(i) for i in ids if 0 <= int(i) < len(pr)]
            rows = pr[sel] if sel else pr[:0]
            cols = self._dd.activities_by_rows(
                self._reader, self._schema, self._ctx_for("activities"), rows)
        return self._dd.unit_records(cols) if cols else []

    def close(self) -> None:
        self._reader.close()


class _Handler(BaseHTTPRequestHandler):
    # Set per-server in serve(); class attributes are fine for a single server.
    cache_root: Path
    web_root: Path
    basemap_spec: dict | None = None
    inspector: "LazyInspector | None" = None

    server_version = "mayviewer/0.1"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # quieter than the noisy default
        logger.debug("%s - %s", self.address_string(), fmt % args)

    # --- routing -----------------------------------------------------------
    def _resolve(self) -> Path | None:
        """Map a URL path to a file on disk, or None if it escapes a root."""
        url = self.path.split("?", 1)[0].split("#", 1)[0]
        if url.startswith("/cache/"):
            root = self.cache_root
            rel = url[len("/cache/"):]
        else:
            root = self.web_root
            rel = url.lstrip("/")
            if rel == "":
                rel = "index.html"
        target = (root / rel).resolve()
        try:
            target.relative_to(root.resolve())
        except ValueError:
            return None  # path traversal attempt
        if target.is_dir():
            target = target / "index.html"
        if not target.is_file():
            # SPA client-side routing: unknown non-cache path → index.html.
            if not url.startswith("/cache/"):
                idx = (self.web_root / "index.html").resolve()
                return idx if idx.is_file() else None
            return None
        return target

    # --- verbs -------------------------------------------------------------
    def do_HEAD(self):
        self._serve(head_only=True)

    def do_GET(self):
        self._serve(head_only=False)

    def _serve_inspect(self, url: str, head_only: bool) -> None:
        """`/inspect/<kind>/<geo_id>` → JSON rows, read live from the .h5.
        Only mounted when the cache is lazy and the source .h5 is present."""
        parts = url.split("/")  # ['', 'inspect', kind, gid]
        insp = self.inspector
        if insp is None or len(parts) != 4 or parts[2] not in _INSPECT_KINDS:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        try:
            gid = int(parts[3])
        except ValueError:
            self.send_error(HTTPStatus.BAD_REQUEST, "geo id must be an integer")
            return
        try:
            rows = insp.rows(parts[2], gid)
        except Exception:
            logger.exception("inspect %s/%s failed", parts[2], gid)
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, "inspect failed")
            return
        body = json.dumps(rows).encode()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def _serve_by_id(self, kind: str, head_only: bool) -> None:
        """`/inspect/{people,activities}_by_id?ids=1,2,3` → rows resolved by id in
        one shot. The fast path for a venue's out-of-unit members (read by id,
        not by fetching whole feeder units). Only mounted for a lazy cache."""
        insp = self.inspector
        if insp is None:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        q = parse_qs(urlparse(self.path).query)
        raw = q.get("ids", [""])[0]
        try:
            ids = [int(x) for x in raw.split(",") if x]
        except ValueError:
            self.send_error(HTTPStatus.BAD_REQUEST, "ids must be integers")
            return
        resolver = (insp.people_by_ids if kind == "people"
                    else insp.activities_by_ids)
        try:
            rows = resolver(ids)
        except Exception:
            logger.exception("%s_by_id failed", kind)
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, "resolve failed")
            return
        body = json.dumps(rows).encode()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def _serve(self, head_only: bool):
        url0 = self.path.split("?", 1)[0].split("#", 1)[0]
        if url0 == "/inspect/people_by_id":
            self._serve_by_id("people", head_only)
            return
        if url0 == "/inspect/activities_by_id":
            self._serve_by_id("activities", head_only)
            return
        if url0.startswith("/inspect/"):
            self._serve_inspect(url0, head_only)
            return
        # Tiny runtime config the SPA reads once (like manifest.json — app
        # config, not world data). Reports whether a basemap was opted into.
        if self.path.split("?", 1)[0].split("#", 1)[0] == "/app-config.json":
            body = json.dumps({"basemap": self.basemap_spec}).encode()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            if not head_only:
                self.wfile.write(body)
            return

        target = self._resolve()
        if target is None:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        size = target.stat().st_size
        ctype = _MIME.get(target.suffix.lower(), "application/octet-stream")
        rng = self.headers.get("Range")

        start, end = 0, size - 1
        partial = False
        if rng:
            m = _RANGE_RE.match(rng.strip())
            if not m or (not m.group(1) and not m.group(2)):
                self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self.send_header("Content-Range", f"bytes */{size}")
                self.end_headers()
                return
            if m.group(1) == "":  # suffix range: bytes=-N (last N bytes)
                length = int(m.group(2))
                start = max(0, size - length)
                end = size - 1
            else:
                start = int(m.group(1))
                end = int(m.group(2)) if m.group(2) else size - 1
            end = min(end, size - 1)
            if start > end or start >= size:
                self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self.send_header("Content-Range", f"bytes */{size}")
                self.end_headers()
                return
            partial = True

        length = end - start + 1
        self.send_response(
            HTTPStatus.PARTIAL_CONTENT if partial else HTTPStatus.OK
        )
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(length))
        self.send_header("Accept-Ranges", "bytes")
        # Same-origin in practice, but explicit CORS keeps the CDN story honest.
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        if partial:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()

        if head_only:
            return
        with open(target, "rb") as f:
            f.seek(start)
            remaining = length
            while remaining > 0:
                chunk = f.read(min(64 * 1024, remaining))
                if not chunk:
                    break
                try:
                    self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    return  # client navigated away mid-stream
                remaining -= len(chunk)


class _Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def handle_error(self, request, client_address):
        """Swallow the routine connection drops this viewer provokes.

        The browser opens speculative keep-alive connections and aborts in-flight
        range requests it no longer needs (hyparquet, pmtiles, and the .npy index
        all issue many, and cancel freely). Each closed/reset socket surfaces in
        ``http.server``'s read/write loop as a connection error — harmless, so we
        stay quiet and only let the default handler report anything unexpected.
        """
        if isinstance(sys.exc_info()[1], (ConnectionError, TimeoutError)):
            return
        super().handle_error(request, client_address)


def serve(
    path: str | Path,
    port: int = 8000,
    host: str = "127.0.0.1",
    open_browser: bool = True,
    basemap: str | None = None,
) -> None:
    cache = resolve_cache(path)
    web = _web_root()
    if not (web / "index.html").is_file():
        raise SystemExit(
            "Frontend not built. Run:  cd frontend && npm install && npm run build"
        )

    spec = _basemap_spec(basemap)
    _Handler.cache_root = cache
    _Handler.web_root = web
    _Handler.basemap_spec = spec

    # Lazy cache (built with `prep --no-drilldown`): mount the live inspector,
    # which reads per-unit drill-down from the source .h5 on demand.
    inspector = None
    try:
        manifest = json.loads((cache / "manifest.json").read_text())
    except (OSError, ValueError):
        manifest = {}
    if manifest.get("drilldown_lazy"):
        src = Path(manifest.get("source", {}).get("path", ""))
        if src.is_file():
            home_rel = manifest.get("person_home_unit")
            home_path = (cache / home_rel) if home_rel else None
            inspector = LazyInspector(src, home_path)
            logger.info("  drill-down: lazy — reading %s on demand", src.name)
        else:
            logger.warning(
                "  drill-down is lazy but the source world was not found at %s\n"
                "  → inspect will be empty. Put the .h5 there, or rebuild the "
                "cache with `mayviewer prep` (no --no-drilldown).", src)
    _Handler.inspector = inspector

    httpd = _Server((host, port), _Handler)
    url = f"http://{host}:{port}/"
    logger.info("MAY-viewer serving %s", cache)
    if spec:
        logger.info("  basemap: %s (opt-in online tiles)", spec["name"])
    logger.info("  → %s   (Ctrl-C to stop)", url)
    if open_browser:
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("\nstopped.")
    finally:
        httpd.server_close()


def add_cli(subparsers: "argparse._SubParsersAction") -> None:
    sv = subparsers.add_parser(
        "serve",
        help="Serve the cached viewer (Range-capable static server) and open it.",
    )
    sv.add_argument("world", help="A prepped world .h5, its cache dir, or a dir containing .mayviewer_cache")
    sv.add_argument("--port", type=int, default=8000)
    sv.add_argument("--host", default="127.0.0.1")
    sv.add_argument("--no-open", action="store_true", help="Do not open a browser.")
    sv.add_argument(
        "--basemap",
        metavar="PRESET|URL",
        default=None,
        help=(
            "Opt into an online basemap (default: none — zero external "
            f"requests). Preset: {', '.join(sorted(_BASEMAP_PRESETS))}; or an "
            "XYZ raster URL template with {z}/{x}/{y}. The world file never "
            "leaves the machine; only map tiles are fetched, once enabled "
            "in the UI."
        ),
    )
