// Confirm boundary tiles exist & decode (geo_id/level/code props) per level,
// via the pmtiles JS lib + MVT decoder — the same path MapLibre uses, over
// the running server. Validates task #9's data contract end to end.
import { PMTiles } from "pmtiles";
import { VectorTile } from "@mapbox/vector-tile";
import Pbf from "pbf";

const BASE = process.argv[2] ?? "http://127.0.0.1:8731";
const m = await (await fetch(`${BASE}/cache/manifest.json`)).json();
const b = m.artifacts.boundaries;
if (!b) {
  console.log("FAIL: manifest has no artifacts.boundaries");
  process.exit(1);
}
const p = new PMTiles(`${BASE}/cache/${b.path}`);
const h = await p.getHeader();
console.log("boundaries:", b.path, "layers:", b.layers.join(","));
console.log("header zoom range:", h.minZoom, "-", h.maxZoom);

function lon2x(lon, z) { return Math.floor(((lon + 180) / 360) * 2 ** z); }
function lat2y(lat, z) {
  const r = (lat * Math.PI) / 180;
  return Math.floor(((1 - Math.log(Math.tan(r) + 1 / Math.cos(r)) / Math.PI) / 2) * 2 ** z);
}

let ok = true;
for (const lv of b.levels) {
  const z = lv.bake_zoom;
  let found = 0;
  let sample = null;
  for (let x = lon2x(h.minLon, z); x <= lon2x(h.maxLon, z); x++)
    for (let y = lat2y(h.maxLat, z); y <= lat2y(h.minLat, z); y++) {
      const t = await p.getZxy(z, x, y);
      if (!t?.data || !t.data.byteLength) continue;
      found++;
      if (sample) continue;
      const tile = new VectorTile(new Pbf(new Uint8Array(t.data)));
      const layer = tile.layers[lv.level_name];
      if (layer && layer.length) {
        const f = layer.feature(0);
        sample = { z, x, y, props: f.properties, type: f.type };
      }
    }
  const pass = found > 0 && sample &&
    "geo_id" in sample.props && "level" in sample.props && "code" in sample.props &&
    Number(sample.props.level) === lv.level;
  ok = ok && pass;
  console.log(
    `${pass ? "PASS" : "FAIL"} ${lv.level_name} (lvl ${lv.level}, z${z}): ` +
      `${found} tiles; sample=${sample ? JSON.stringify(sample.props) : "none"}`,
  );
}
console.log(ok ? "ALL PASS" : "SOME FAIL");
process.exit(ok ? 0 : 1);
