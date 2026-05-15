// Runtime config served by `mayviewer serve` (separate from the manifest,
// which is world data). Currently just the opt-in basemap: `null` unless the
// operator passed `serve --basemap …` (non-negotiable #3 — the core viewer
// makes zero external requests; a basemap is never the default).

export interface BasemapSpec {
  name: string;
  tiles: string[];
  tileSize: number;
  maxzoom: number;
  attribution: string;
}

export interface AppConfig {
  basemap: BasemapSpec | null;
}

const EMPTY: AppConfig = { basemap: null };

export async function loadAppConfig(): Promise<AppConfig> {
  try {
    const r = await fetch("/app-config.json");
    if (!r.ok) return EMPTY;
    const j = (await r.json()) as Partial<AppConfig>;
    const b = j.basemap;
    // Validate defensively: a stray SPA-fallback HTML/JSON must not enable
    // network tiles. Only a well-formed spec counts as opt-in.
    if (
      b &&
      Array.isArray(b.tiles) &&
      b.tiles.length > 0 &&
      typeof b.tiles[0] === "string"
    ) {
      return {
        basemap: {
          name: typeof b.name === "string" ? b.name : "Basemap",
          tiles: b.tiles,
          tileSize: typeof b.tileSize === "number" ? b.tileSize : 256,
          maxzoom: typeof b.maxzoom === "number" ? b.maxzoom : 19,
          attribution: typeof b.attribution === "string" ? b.attribution : "",
        },
      };
    }
    return EMPTY;
  } catch {
    return EMPTY;
  }
}
