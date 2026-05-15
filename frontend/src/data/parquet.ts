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

/** Drill-down for one geo unit: O(1) via the manifest row-group index. */
export async function readGeoUnit(
  artifact: DrilldownArtifact,
  geoId: number,
  columns?: string[],
): Promise<Row[]> {
  const idx = artifact.row_groups[String(geoId)];
  if (idx === undefined) return [];
  return readRowGroup(artifact.path, idx, columns);
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
