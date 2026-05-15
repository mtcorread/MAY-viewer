import { create } from "zustand";
import {
  loadManifest,
  levelsCoarseToFine,
  type Manifest,
} from "../data/manifest";
import { readAggregates } from "../data/parquet";
import { FIXED_COLS } from "../data/columns";
import { loadAppConfig, type BasemapSpec } from "../data/appConfig";

export interface AggRow {
  geo_id: number;
  geo_name: string;
  level: number;
  level_name: string;
  parent_id: number;
  [k: string]: unknown;
}

export type Mode = "map" | "inspect";
export type MapMode = "boundaries" | "hexbin";

interface NavNode {
  geo_id: number;
  geo_name: string;
  level: number;
  level_name: string;
}

interface LevelData {
  rows: AggRow[];
  byId: Map<number, AggRow>;
  byParent: Map<number, AggRow[]>;
}

interface State {
  manifest: Manifest | null;
  error: string | null;
  mode: Mode;
  mapMode: MapMode; // boundaries (clickable polygons) vs hexbin density
  basemap: BasemapSpec | null; // opt-in online basemap (null = not enabled)
  basemapOn: boolean; // user toggle; only meaningful when basemap != null
  hexLayer: string; // which pmtiles layer is shown
  levels: { value: number; name: string }[]; // coarse → fine
  levelCache: Map<number, LevelData>;
  /** Breadcrumb from the coarsest root down to the selected unit. */
  path: NavNode[];
  selected: NavNode | null;
  // Inspector selections
  inspectVenueId: number | null;
  inspectPersonId: number | null;

  init: () => Promise<void>;
  setMode: (m: Mode) => void;
  setMapMode: (m: MapMode) => void;
  setBasemapOn: (on: boolean) => void;
  setHexLayer: (l: string) => void;
  ensureLevel: (level: number) => Promise<LevelData>;
  /** Synchronous read of one cached aggregate row (geo:* extent/centroid). */
  aggRow: (level: number, geoId: number) => AggRow | undefined;
  drillTo: (node: NavNode) => Promise<void>;
  /** Map-driven drill: resolve a unit by (geoId, level) and rebuild the
   *  full ancestor breadcrumb via parent_id. Fixes the no-geo_id gap. */
  drillToGeo: (geoId: number, level: number) => Promise<void>;
  drillUpTo: (index: number) => void;
  inspectVenue: (id: number | null) => void;
  inspectPerson: (id: number | null) => void;
}

export const useStore = create<State>((set, get) => ({
  manifest: null,
  error: null,
  mode: "map",
  mapMode: "hexbin",
  basemap: null,
  basemapOn: false,
  hexLayer: "",
  levels: [],
  levelCache: new Map(),
  path: [],
  selected: null,
  inspectVenueId: null,
  inspectPersonId: null,

  init: async () => {
    try {
      const [manifest, appConfig] = await Promise.all([
        loadManifest(),
        loadAppConfig(),
      ]);
      const levels = levelsCoarseToFine(manifest.geo);
      set({
        manifest,
        levels,
        hexLayer: manifest.artifacts.hexbin.layers[0] ?? "",
        // Boundaries are the click target when shapes were baked; otherwise
        // the hexbin density backdrop is the only map (non-negotiable #6).
        mapMode: manifest.artifacts.boundaries ? "boundaries" : "hexbin",
        // The operator passing `serve --basemap …` IS the explicit opt-in,
        // so show it on by default; "None" in the switcher turns it off.
        basemap: appConfig.basemap,
        basemapOn: !!appConfig.basemap,
      });
      // Load the coarsest level and seat the breadcrumb at its root unit.
      const root = await get().ensureLevel(levels[0].value);
      const first = root.rows[0];
      if (first) {
        const node: NavNode = {
          geo_id: first.geo_id,
          geo_name: first.geo_name,
          level: first.level,
          level_name: first.level_name,
        };
        set({ path: [node], selected: node });
      }
    } catch (e) {
      set({ error: e instanceof Error ? e.message : String(e) });
    }
  },

  setMode: (mode) => set({ mode }),
  setMapMode: (mapMode) => set({ mapMode }),
  setBasemapOn: (basemapOn) => set({ basemapOn }),
  setHexLayer: (hexLayer) => set({ hexLayer }),

  aggRow: (level, geoId) => get().levelCache.get(level)?.byId.get(geoId),

  ensureLevel: async (level) => {
    const cache = get().levelCache;
    const hit = cache.get(level);
    if (hit) return hit;
    const m = get().manifest!;
    const art = m.artifacts.aggregates[String(level)];
    const rows = (await readAggregates(art.path)) as unknown as AggRow[];
    const byId = new Map<number, AggRow>();
    const byParent = new Map<number, AggRow[]>();
    for (const r of rows) {
      byId.set(r.geo_id, r);
      const list = byParent.get(r.parent_id);
      if (list) list.push(r);
      else byParent.set(r.parent_id, [r]);
    }
    const data: LevelData = { rows, byId, byParent };
    const next = new Map(cache);
    next.set(level, data);
    set({ levelCache: next });
    return data;
  },

  drillTo: async (node) => {
    set((s) => {
      // Replace the tail of the path at this node's level.
      const trimmed = s.path.filter((n) => n.level < node.level);
      return { path: [...trimmed, node], selected: node, inspectVenueId: null, inspectPersonId: null };
    });
  },

  drillToGeo: async (geoId, level) => {
    const { levels, ensureLevel } = get();
    const idx = levels.findIndex((l) => l.value === level);
    if (idx < 0) return;
    const node = async (lv: number, id: number): Promise<NavNode | null> => {
      const d = await ensureLevel(lv);
      const r = d.byId.get(id);
      return r
        ? { geo_id: r.geo_id, geo_name: r.geo_name, level: r.level, level_name: r.level_name }
        : null;
    };
    const target = await node(level, geoId);
    if (!target) return;
    // Walk parent_id up the cached coarser levels to rebuild the breadcrumb.
    const chain: NavNode[] = [target];
    let pid = (await ensureLevel(level)).byId.get(geoId)?.parent_id;
    for (let i = idx - 1; i >= 0 && pid != null; i--) {
      const n = await node(levels[i].value, pid);
      if (!n) break;
      chain.unshift(n);
      pid = (await ensureLevel(levels[i].value)).byId.get(n.geo_id)?.parent_id;
    }
    set({ path: chain, selected: target, inspectVenueId: null, inspectPersonId: null });
  },

  drillUpTo: (index) => {
    const s = get();
    const path = s.path.slice(0, index + 1);
    set({ path, selected: path[path.length - 1] ?? null, inspectVenueId: null, inspectPersonId: null });
  },

  inspectVenue: (id) => set({ inspectVenueId: id, inspectPersonId: null }),
  inspectPerson: (id) => set({ inspectPersonId: id }),
}));

/** Columns that are real metrics (drop the geo bookkeeping ones). */
export function metricColumns(row: AggRow): string[] {
  return Object.keys(row).filter((c) => !FIXED_COLS.has(c));
}
