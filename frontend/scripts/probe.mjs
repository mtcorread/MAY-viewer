// End-to-end bounded-read probe: read ONE people row group for a geo unit
// over HTTP range, exactly as the browser will. Asserts we never pull the
// whole file by counting bytes fetched vs. file size.
import { asyncBufferFromUrl, parquetMetadataAsync, parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";

const BASE = process.argv[2] ?? "http://127.0.0.1:8731";
const m = await (await fetch(`${BASE}/cache/manifest.json`)).json();
const dd = m.artifacts.drilldown.people;
const gid = Object.keys(dd.row_groups)[5];
const rgIdx = dd.row_groups[gid];

let bytes = 0;
const base = await asyncBufferFromUrl({ url: `${BASE}/cache/${dd.path}` });
const file = {
  byteLength: base.byteLength,
  slice: async (s, e) => {
    const b = await base.slice(s, e);
    bytes += b.byteLength;
    return b;
  },
};
const meta = await parquetMetadataAsync(file);
const off = [];
let acc = 0;
for (const rg of meta.row_groups) { off.push(acc); acc += Number(rg.num_rows); }
const rowStart = off[rgIdx];
const rowEnd = rowStart + Number(meta.row_groups[rgIdx].num_rows);
const rows = await parquetReadObjects({ file, metadata: meta, compressors, rowStart, rowEnd });

console.log(`geo_unit ${gid} -> row group ${rgIdx}`);
console.log(`rows: ${rows.length}  (file ${file.byteLength} bytes, fetched ${bytes} bytes = ${(100*bytes/file.byteLength).toFixed(2)}%)`);
console.log("columns:", Object.keys(rows[0]).join(", "));
const s = rows[0];
console.log("sample:", JSON.stringify({ person_id: s.person_id, age: s.age, sex: s.sex, ethnicity: s.ethnicity, friendships: s.friendships }, (_, v) => typeof v === "bigint" ? Number(v) : v));
if (bytes > file.byteLength * 0.5) { console.error("FAIL: fetched too much"); process.exit(1); }
console.log("PASS: bounded read");
