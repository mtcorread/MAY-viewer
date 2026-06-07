// Resolve any person's HOME geo unit by id by range-reading a single element of
// the cache's `person_home_unit.npy` index (prep writes it for both modes). This
// is what lets the network list open a friend who lives in another unit: one
// bounded byte read → their home unit → drill there. Mode-agnostic — the .npy is
// served under /cache with Range in both lazy and static caches, exactly like
// the Parquet shards.

import { CACHE } from "./manifest";

interface NpyMeta {
  dataOffset: number; // byte offset where element 0 begins
  itemsize: number; // bytes per element (numpy int8/16/32/64)
  length: number; // element count, for a bounds check
}

const META = new Map<string, Promise<NpyMeta>>();

async function readRange(url: string, start: number, end: number): Promise<Uint8Array> {
  const r = await fetch(url, { headers: { Range: `bytes=${start}-${end}` } });
  if (!r.ok) throw new Error(`${url}: HTTP ${r.status}`);
  return new Uint8Array(await r.arrayBuffer());
}

// Parse the .npy header (https://numpy.org/doc/stable/reference/generated/numpy.lib.format.html):
// 6-byte magic, 2 version bytes, header length (u16 v1 / u32 v2), then an ASCII
// dict literal {'descr','fortran_order','shape'}. The data follows immediately.
async function readMeta(url: string): Promise<NpyMeta> {
  const head = await readRange(url, 0, 511); // header is tiny for a 1-D array
  const major = head[6];
  let headerLen: number;
  let headerStart: number;
  if (major >= 2) {
    headerLen = head[8] | (head[9] << 8) | (head[10] << 16) | (head[11] << 24);
    headerStart = 12;
  } else {
    headerLen = head[8] | (head[9] << 8);
    headerStart = 10;
  }
  const dict = new TextDecoder().decode(
    head.subarray(headerStart, headerStart + headerLen),
  );
  const descr = /'descr'\s*:\s*'([^']+)'/.exec(dict)?.[1] ?? "<i4";
  const length = Number(/'shape'\s*:\s*\(\s*(\d+)/.exec(dict)?.[1] ?? 0);
  const itemsize = Number(descr.replace(/\D/g, "")) || 4;
  return { dataOffset: headerStart + headerLen, itemsize, length };
}

/**
 * Home geo unit id for `pid` (the people-shard partition key — `readGeoUnit`
 * on the people artifact with this id is guaranteed to hold the row), or -1 if
 * the id has no person or is out of range. The header read is cached per file,
 * so each subsequent lookup is a single one-element range request.
 */
export async function personHomeUnit(npyPath: string, pid: number): Promise<number> {
  if (pid < 0) return -1;
  const url = `${CACHE}/${npyPath}`;
  let m = META.get(url);
  if (!m) META.set(url, (m = readMeta(url)));
  const meta = await m;
  if (pid >= meta.length) return -1;
  const start = meta.dataOffset + pid * meta.itemsize;
  const buf = await readRange(url, start, start + meta.itemsize - 1);
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  switch (meta.itemsize) {
    case 8:
      return Number(dv.getBigInt64(0, true)); // little-endian, as numpy writes
    case 2:
      return dv.getInt16(0, true);
    case 1:
      return dv.getInt8(0);
    default:
      return dv.getInt32(0, true);
  }
}
