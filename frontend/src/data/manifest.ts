// The manifest is the single source of domain knowledge (non-negotiable #2).
// It is the precomputed *index* — the only file fetched whole. Everything
// else is read through bounded HTTP-range requests (see parquet.ts).

export interface WorldSchema {
  num_people: number;
  num_venues: number;
  num_geo_units: number;
  geo_levels: string[];
  venue_types: string[];
  subset_names: string[];
  sex_mapping: Record<string, number>;
  person_properties: string[];
  person_relations: string[];
  venue_properties: Record<string, string[]>;
  geo_properties: string[];
  activity_names: string[];
  attrs: Record<string, unknown>;
}

export interface GeoBlock {
  level_values: number[]; // leaf-first, e.g. [3,2,1,0]
  level_names: Record<string, string>; // {"3":"SGU",...}
  leaf_level: number;
  units_per_level: Record<string, number>;
}

export interface DrilldownArtifact {
  // Static cache only: the Parquet shard path. Absent in a lazy cache.
  path?: string;
  // { "<geo_unit_id>": <row_group_index> } — the O(1) drill-down index. In a
  // lazy cache this is a presence map (value unused): which units have data.
  row_groups: Record<string, number>;
  // Lazy cache (prep --no-drilldown): read each unit live from the server's
  // /inspect/<endpoint>/<geo_id>.
  lazy?: boolean;
  endpoint?: string; // inspect kind: "people" | "venues" | "members" | "activities"
}

/** One geo level inside `artifacts.boundaries` (the prep match report). */
export interface BoundaryLevel {
  level: number; // numeric geo level (matches aggregate `level`)
  level_name: string; // also the PMTiles source-layer name
  prop: string;
  strategy: string;
  src_crs: string;
  world_units: number;
  matched: number;
  rate: number;
  bake_zoom: number; // the single zoom tiles are baked at
  minzoom: number; // contiguous band start (MapLibre overzooms within it)
  maxzoom: number; // contiguous band end
  file: string;
}

export interface BoundaryArtifact {
  path: string;
  tiles: number;
  features: number;
  bake_zooms: number[];
  minzoom: number;
  maxzoom: number;
  bytes: number;
  layers: string[]; // source-layer names, coarse → fine
  levels: BoundaryLevel[];
}

/**
 * Transit (train/tube) layer. `lines` is a PMTiles vector layer of route
 * geometry (contiguous zoom pyramid, one `lines` source-layer); each feature
 * carries `line_id`, `venue_id`, `mode`, `rider_count`. `riders` and `chains`
 * are drill-down Parquets read by bounded row group: riders keyed by line
 * `venue_id` (click a line → its riders, or intersect two lines), chains keyed
 * by each rider's home geo unit (select a rider → their ordered legs). Present
 * only when prep was given the line-geometry CSVs.
 */
export interface TransitArtifact {
  lines: {
    path: string;
    layer: string; // PMTiles source-layer name ("lines")
    minzoom: number; // archive's lowest baked zoom (source pin)
    maxzoom: number; // archive's highest baked zoom (overzoomed above)
    bake_zooms: number[];
    tiles: number;
    features: number;
    bytes: number;
  };
  riders: { path: string; row_groups: Record<string, number> }; // venue_id → rg
  // chains.fields lists the per-leg metadata columns this world recorded
  // (mirrors the h5 membership_metadata registry — e.g. t_board_min, or
  // origin/dest/board/alight unit ids in worlds that persist them). Absent in
  // caches baked before the field passthrough.
  chains: { path: string; row_groups: Record<string, number>; fields?: string[] }; // home_unit → rg
  summary: {
    lines: number;
    train: number;
    tube: number;
    rider_memberships: number;
    riders_with_chains: number;
  };
}

export interface Manifest {
  manifest_version: number;
  source: {
    name: string;
    path: string;
    fingerprint: { size: number; mtime: number };
    boundary_fingerprint?: unknown;
  };
  // False ⇒ world has no geography/latitudes; prep skipped hexbin and
  // boundaries; the frontend hides the map and renders Inspect only.
  spatial: boolean;
  schema: WorldSchema;
  geo: GeoBlock;
  artifacts: {
    aggregates: Record<string, { level_name: string; path: string; units: number }>;
    drilldown: {
      people: DrilldownArtifact;
      venues: DrilldownArtifact;
      members: DrilldownArtifact;
      // Per-person activity → venue assignments. Omitted for worlds whose
      // source .h5 carries no activity map.
      activities?: DrilldownArtifact;
    };
    // Both omitted in mapless caches.
    hexbin?: { path: string; tiles: number; zooms: number[]; layers: string[]; bytes?: number };
    boundaries?: BoundaryArtifact;
    // Present only when prep was given the transit-geometry CSVs.
    transit?: TransitArtifact;
  };
  peak_unit_rows: Record<string, number>;
  build_seconds: number;
  // `person_id -> home geo unit` index (a flat .npy), served under /cache and
  // range-read one element at a time to resolve a person by id (e.g. opening a
  // network friend who lives in another unit). Present in both serving modes.
  person_home_unit?: string;
}

export const CACHE = "/cache";

export async function loadManifest(): Promise<Manifest> {
  const r = await fetch(`${CACHE}/manifest.json`);
  if (!r.ok) throw new Error(`manifest.json: HTTP ${r.status}`);
  return (await r.json()) as Manifest;
}

/** Levels ordered coarsest → finest, e.g. [{v:0,name:"XLGU"}, ... {v:3,name:"SGU"}]. */
export function levelsCoarseToFine(g: GeoBlock): { value: number; name: string }[] {
  return [...g.level_values]
    .sort((a, b) => a - b)
    .map((v) => ({ value: v, name: g.level_names[String(v)] ?? `L${v}` }));
}
