// Bounded Parquet access — non-negotiable #1 lives or dies here.
//
// We NEVER fetch a whole drill-down Parquet. hyparquet's asyncBuffer issues
// HTTP range requests; we read exactly one row group (one geo unit) by
// turning the manifest's `row_groups[geo_id] -> rowGroupIndex` into the
// [rowStart, rowEnd) span for that group and reading only those bytes.

import {
  asyncBufferFromUrl,
  parquetMetadataAsync,
  parquetReadObjects,
  type AsyncBuffer,
  type FileMetaData,
} from "hyparquet";
import { compressors } from "hyparquet-compressors";
import { CACHE, type DrilldownArtifact } from "./manifest";

type Row = Record<string, unknown>;

// hyparquet returns Parquet INT64 columns as BigInt. Every id / level /
// count / person_id in this cache is INT64, so without this the whole app
// would compare BigInt against the plain numbers in manifest.json (e.g.
// `0n === 0` is false) and every geo/inspector lookup silently misses.
// Geo ids and counts are far below 2^53, so Number is lossless here.
function denumber(v: unknown): unknown {
  if (typeof v === "bigint") return Number(v);
  if (Array.isArray(v)) return v.map(denumber);
  return v;
}

function normalizeRows(rows: Row[]): Row[] {
  for (const r of rows)
    for (const k in r) {
      const v = r[k];
      if (typeof v === "bigint" || Array.isArray(v)) r[k] = denumber(v);
    }
  return rows;
}

interface Handle {
  file: AsyncBuffer;
  metadata: FileMetaData;
  // cumulative first-row offset per row group
  rowOffsets: number[];
}

const handles = new Map<string, Promise<Handle>>();

function open(path: string): Promise<Handle> {
  let h = handles.get(path);
  if (!h) {
    h = (async () => {
      const file = await asyncBufferFromUrl({ url: `${CACHE}/${path}` });
      const metadata = await parquetMetadataAsync(file);
      const rowOffsets: number[] = [];
      let acc = 0;
      for (const rg of metadata.row_groups) {
        rowOffsets.push(acc);
        acc += Number(rg.num_rows);
      }
      return { file, metadata, rowOffsets };
    })();
    handles.set(path, h);
  }
  return h;
}

/**
 * Pre-open a drill-down Parquet so its footer (file length + metadata) is
 * fetched and cached now, not on the first read. This is the static-cache
 * twin of the server-side context prime in lazy mode: it moves the one-time
 * "range-reading…" cost off the first Inspect click and onto page load.
 * Fire-and-forget — a failed warm just leaves the real read to open it (and
 * surface the error) later. No-op when `path` is absent (lazy artifacts read
 * server-side, so there is no client footer to warm).
 */
export function warmHandle(path: string | undefined): void {
  if (path) void open(path).catch(() => {});
}

/** Read exactly one row group by its index (the manifest drill-down unit). */
export async function readRowGroup(
  path: string,
  rowGroupIndex: number,
  columns?: string[],
): Promise<Row[]> {
  const { file, metadata, rowOffsets } = await open(path);
  if (rowGroupIndex < 0 || rowGroupIndex >= rowOffsets.length) return [];
  const rowStart = rowOffsets[rowGroupIndex];
  const rowEnd = rowStart + Number(metadata.row_groups[rowGroupIndex].num_rows);
  return normalizeRows(
    (await parquetReadObjects({
      file,
      metadata,
      compressors,
      columns,
      rowStart,
      rowEnd,
    })) as Row[],
  );
}

// Bounded LRU of whole-unit drill-down reads (no column projection). It makes
// navigating BACK to a recently visited unit free in either mode: static would
// otherwise re-read the row-group bytes and lazy would re-hit the server, and
// `serve` sends `Cache-Control: no-cache` so the browser caches neither for us.
// Keyed by artifact (path in static, endpoint in lazy) + geo id. The world file
// is immutable for a session, so a cached unit never goes stale. Promises are
// cached (not arrays) to also collapse concurrent reads of the same unit.
const UNIT_CACHE = new Map<string, Promise<Row[]>>();
const UNIT_CACHE_CAP = 24; // ~6 units × 4 artifacts kept warm

function fetchGeoUnit(
  artifact: DrilldownArtifact,
  geoId: number,
  columns?: string[],
): Promise<Row[]> {
  if (artifact.lazy) {
    return fetch(`/inspect/${artifact.endpoint}/${geoId}`).then((r) =>
      r.ok ? (r.json() as Promise<Row[]>) : [],
    );
  }
  return readRowGroup(artifact.path!, artifact.row_groups[String(geoId)], columns);
}

/**
 * Drill-down for one geo unit. Static cache: O(1) row-group read via the
 * manifest index. Lazy cache (prep --no-drilldown): one fetch of the server's
 * /inspect endpoint, which reads the unit from the .h5 live. Whole-unit reads
 * are memoized (see UNIT_CACHE) so back-navigation is instant in both modes.
 */
export function readGeoUnit(
  artifact: DrilldownArtifact,
  geoId: number,
  columns?: string[],
): Promise<Row[]> {
  // row_groups doubles as the presence gate in both modes.
  if (artifact.row_groups[String(geoId)] === undefined) return Promise.resolve([]);
  // Only the unprojected whole-unit read (what the Inspector re-issues on
  // back-navigation) is cached; a projected read bypasses the cache.
  if (columns) return fetchGeoUnit(artifact, geoId, columns);

  const key = `${artifact.path ?? artifact.endpoint}:${geoId}`;
  const hit = UNIT_CACHE.get(key);
  if (hit) {
    UNIT_CACHE.delete(key); // LRU: re-insert as most-recent
    UNIT_CACHE.set(key, hit);
    return hit;
  }
  const p = fetchGeoUnit(artifact, geoId);
  p.catch(() => UNIT_CACHE.delete(key)); // never pin a failed read
  UNIT_CACHE.set(key, p);
  if (UNIT_CACHE.size > UNIT_CACHE_CAP) {
    const oldest = UNIT_CACHE.keys().next().value; // insertion order = LRU
    if (oldest !== undefined) UNIT_CACHE.delete(oldest);
  }
  return p;
}

/**
 * Drill-down for several geo units at once, concatenated. Used to resolve a
 * venue's members who live outside the inspected unit: each member's
 * `home_geo_unit` names the unit whose people shard holds its attribute row,
 * so we read those distinct feeder units' people and merge. Deduped, and bound
 * by the number of distinct feeder units (not the member count); works in both
 * serving modes because `readGeoUnit` already branches static vs lazy.
 */
export async function readGeoUnits(
  artifact: DrilldownArtifact,
  geoIds: number[],
  columns?: string[],
): Promise<Row[]> {
  const uniq = [...new Set(geoIds)].filter(
    (g) => artifact.row_groups[String(g)] !== undefined,
  );
  const chunks = await Promise.all(uniq.map((g) => readGeoUnit(artifact, g, columns)));
  return chunks.flat();
}

/**
 * Resolve people by id in one request — the lazy-cache fast path for a venue's
 * out-of-unit members. The server reads just those rows from the .h5 (~tens of
 * tiny reads), so members appear immediately instead of the browser fetching
 * every feeder unit whole. Chunked to keep the query string bounded; chunks run
 * concurrently and are concatenated, so the caller still applies them in one
 * state update (no trickle). Only valid on a lazy artifact.
 */
async function fetchByIds(endpoint: string, ids: number[]): Promise<Row[]> {
  const uniq = [...new Set(ids)].filter((i) => i >= 0);
  if (uniq.length === 0) return [];
  const CH = 400;
  const chunks: number[][] = [];
  for (let i = 0; i < uniq.length; i += CH) chunks.push(uniq.slice(i, i + CH));
  const res = await Promise.all(
    chunks.map(async (c) => {
      const r = await fetch(`/inspect/${endpoint}?ids=${c.join(",")}`);
      return r.ok ? ((await r.json()) as Row[]) : [];
    }),
  );
  return res.flat();
}

export function resolvePeopleByIds(
  artifact: DrilldownArtifact,
  ids: number[],
): Promise<Row[]> {
  return artifact.lazy ? fetchByIds("people_by_id", ids) : Promise.resolve([]);
}

/** Activities for people by id — the lazy fast path so an out-of-unit member's
 *  activities resolve in one request (their activities shard is another unit's). */
export function resolveActivitiesByIds(
  artifact: DrilldownArtifact | undefined,
  ids: number[],
): Promise<Row[]> {
  return artifact?.lazy ? fetchByIds("activities_by_id", ids) : Promise.resolve([]);
}

/**
 * Aggregates are the precomputed summary layer (one row per geo unit at a
 * level) — the intended whole-artifact read. Still range-fetched column-wise
 * by hyparquet; they are orders of magnitude smaller than the drill-down.
 */
export async function readAggregates(path: string, columns?: string[]): Promise<Row[]> {
  const { file, metadata } = await open(path);
  return normalizeRows(
    (await parquetReadObjects({ file, metadata, compressors, columns })) as Row[],
  );
}
