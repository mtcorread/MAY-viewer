// Confirm the pinned-zoom hexbin tiles exist & decode via the pmtiles JS lib
// (same path MapLibre uses), over the running server.
import { PMTiles } from "pmtiles";

const BASE = process.argv[2] ?? "http://127.0.0.1:8731";
const m = await (await fetch(`${BASE}/cache/manifest.json`)).json();
const zooms = [...m.artifacts.hexbin.zooms].sort((a, b) => a - b);
const srcZoom = zooms[Math.min(1, zooms.length - 1)];

const p = new PMTiles(`${BASE}/cache/${m.artifacts.hexbin.path}`);
const h = await p.getHeader();
console.log("zooms in manifest:", zooms, "→ pinned srcZoom:", srcZoom);
console.log("header zoom range:", h.minZoom, "-", h.maxZoom);

// Walk the tile address space at srcZoom over the archive bounds.
function lon2x(lon, z) { return Math.floor(((lon + 180) / 360) * 2 ** z); }
function lat2y(lat, z) {
  const r = (lat * Math.PI) / 180;
  return Math.floor(((1 - Math.log(Math.tan(r) + 1 / Math.cos(r)) / Math.PI) / 2) * 2 ** z);
}
let found = 0, firstLen = 0;
for (let x = lon2x(h.minLon, srcZoom); x <= lon2x(h.maxLon, srcZoom); x++)
  for (let y = lat2y(h.maxLat, srcZoom); y <= lat2y(h.minLat, srcZoom); y++) {
    const t = await p.getZxy(srcZoom, x, y);
    if (t?.data && t.data.byteLength) {
      if (!found) { firstLen = t.data.byteLength; console.log(`first tile z${srcZoom}/${x}/${y}: ${firstLen} bytes`); }
      found++;
    }
  }
console.log(found ? `PASS: ${found} tiles present at z${srcZoom}` : "FAIL: no tiles at pinned zoom");
process.exit(found ? 0 : 1);
