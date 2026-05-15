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
  path: string;
  // { "<geo_unit_id>": <row_group_index> } — the O(1) drill-down index.
  row_groups: Record<string, number>;
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

export interface Manifest {
  manifest_version: number;
  source: {
    name: string;
    path: string;
    fingerprint: { size: number; mtime: number };
    boundary_fingerprint?: unknown;
  };
  schema: WorldSchema;
  geo: GeoBlock;
  artifacts: {
    aggregates: Record<string, { level_name: string; path: string; units: number }>;
    drilldown: {
      people: DrilldownArtifact;
      venues: DrilldownArtifact;
      members: DrilldownArtifact;
    };
    hexbin: { path: string; tiles: number; zooms: number[]; layers: string[]; bytes?: number };
    // Optional: only present when prep was run with --boundary-config.
    boundaries?: BoundaryArtifact;
  };
  peak_unit_rows: Record<string, number>;
  build_seconds: number;
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
