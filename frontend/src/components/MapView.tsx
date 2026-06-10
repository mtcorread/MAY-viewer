import { useEffect, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import { Protocol, PMTiles } from "pmtiles";
import { useStore } from "../state/store";
import { CACHE, type BoundaryLevel } from "../data/manifest";
import { compact, num } from "../util/format";

// Offline-first map. Two render modes, both fed by bounded PMTiles tile
// requests — no external basemap, no whole-file fetch (non-negotiable #1):
//
//  • "boundaries" — real geo polygons baked by prep, one source-layer per
//    geo level, pinned to its single bake zoom and overzoomed across the
//    level's band. Polygons are the click target → store.drillToGeo.
//  • "hexbin" — the density backdrop, the no-shapes fallback and a toggle.
//
// The visual language is Direction E "Refined": Nordic paper ground,
// electric-blue overlay polygons, floating glass cards. Still 100% offline.

// Cool, paper-compatible density ramps (low → high).
const RAMP: Record<string, string[]> = {
  _people: ["#dde4f0", "#a8bfe3", "#6f93d7", "#3f6fcc", "#1d4ed8"],
  _venues: ["#dde7e5", "#a3c5c0", "#5f9a93", "#2e7e76", "#0b5b55"],
};
function rampFor(layer: string): string[] {
  return RAMP["_" + layer] ?? RAMP._people;
}

const CORAL = "#2563eb";
const CORAL_D = "#1d4ed8";
const PAPER = "#f4f5f7";
const NONE_FILTER: maplibregl.FilterSpecification = ["==", ["get", "geo_id"], -1];

// Transit line palette (train vs tube). A white casing under each colored line
// keeps the network legible over both the paper ground and the OSM basemap.
const TRAIN = "#e8743b";
const TUBE = "#2f6fed";
const LINE_CASING = "#ffffff";
// Highlight filters key off venue_id (each line feature carries it); -1 selects
// nothing, the resting state for the hover/selected overlays.
const NO_VENUE: maplibregl.FilterSpecification = ["==", ["get", "venue_id"], -1];
// Color a line layer by its mode property.
const LINE_COLOR: maplibregl.DataDrivenPropertyValueSpecification<string> = [
  "match",
  ["get", "mode"],
  "tube",
  TUBE,
  TRAIN, // default (train)
];

interface Tip {
  x: number;
  y: number;
  level: string;
  id: string;
  people: number | null;
  meanAge: number | null;
}

export function MapView() {
  const ref = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const {
    manifest,
    mapMode,
    setMapMode,
    hexLayer,
    setHexLayer,
    selected,
    basemap,
    basemapOn,
    setBasemapOn,
    transitLine,
    transitJourney,
    transitOD,
    transitCompare,
  } = useStore();
  // MapView only mounts in map mode, which the shell hides for mapless
  // caches — hexbin is therefore guaranteed present once `manifest` is set.
  const layers = manifest?.artifacts.hexbin?.layers ?? [];
  const boundaries = manifest?.artifacts.boundaries;
  const transit = manifest?.artifacts.transit;
  const [domain, setDomain] = useState<[number, number]>([1, 1000]);
  const [tip, setTip] = useState<Tip | null>(null);

  // Build the map once.
  useEffect(() => {
    if (!ref.current || mapRef.current || !manifest) return;
    const proto = new Protocol();
    maplibregl.addProtocol("pmtiles", proto.tile);

    const hex = manifest.artifacts.hexbin!;
    const hexUrl = `${CACHE}/${hex.path}`;
    const hexAbs = new URL(hexUrl, location.href).href;
    const zooms = [...hex.zooms].sort((a, b) => a - b);
    const srcZoom = zooms[Math.min(1, zooms.length - 1)] ?? 0;

    const sources: Record<string, maplibregl.SourceSpecification> = {
      hex: {
        type: "vector",
        tiles: [`pmtiles://${hexAbs}/{z}/{x}/{y}`],
        minzoom: srcZoom,
        maxzoom: srcZoom,
      },
    };

    const bm = useStore.getState().basemap;
    const bmOn = useStore.getState().basemapOn;
    const styleLayers: maplibregl.LayerSpecification[] = [
      { id: "bg", type: "background", paint: { "background-color": PAPER } },
    ];
    if (bm) {
      sources.basemap = {
        type: "raster",
        tiles: bm.tiles,
        tileSize: bm.tileSize,
        maxzoom: bm.maxzoom,
        attribution: bm.attribution,
      };
      styleLayers.push({
        id: "basemap",
        type: "raster",
        source: "basemap",
        layout: { visibility: bmOn ? "visible" : "none" },
        paint: { "raster-opacity": 0.85 },
      });
    }
    styleLayers.push(
      ...hex.layers.map(
        (lyr): maplibregl.LayerSpecification => ({
          id: `hex-${lyr}`,
          type: "fill",
          source: "hex",
          "source-layer": lyr,
          layout: { visibility: "none" },
          paint: {
            "fill-color": "#6f93d7",
            "fill-opacity": 0.82,
            "fill-outline-color": "rgba(244,245,247,0.5)",
          },
        }),
      ),
    );

    let bndAbs = "";
    if (boundaries) {
      bndAbs = new URL(`${CACHE}/${boundaries.path}`, location.href).href;
      for (const lv of boundaries.levels) {
        const sid = `bnd-${lv.level_name}`;
        sources[sid] = {
          type: "vector",
          tiles: [`pmtiles://${bndAbs}/{z}/{x}/{y}`],
          minzoom: lv.bake_zoom,
          maxzoom: lv.bake_zoom,
        };
        const lmax = Math.min(24, lv.maxzoom + 1);
        styleLayers.push(
          {
            id: `bfill-${lv.level_name}`,
            type: "fill",
            source: sid,
            "source-layer": lv.level_name,
            minzoom: lv.minzoom,
            maxzoom: lmax,
            layout: { visibility: "none" },
            paint: { "fill-color": CORAL, "fill-opacity": 0.13 },
          },
          {
            id: `bhov-${lv.level_name}`,
            type: "fill",
            source: sid,
            "source-layer": lv.level_name,
            minzoom: lv.minzoom,
            maxzoom: lmax,
            filter: NONE_FILTER,
            layout: { visibility: "none" },
            paint: { "fill-color": CORAL, "fill-opacity": 0.22 },
          },
          {
            id: `bline-${lv.level_name}`,
            type: "line",
            source: sid,
            "source-layer": lv.level_name,
            minzoom: lv.minzoom,
            maxzoom: lmax,
            layout: { visibility: "none", "line-join": "round" },
            paint: {
              "line-color": CORAL,
              "line-opacity": 0.9,
              "line-width": ["interpolate", ["linear"], ["zoom"], 3, 1.2, 9, 1.6, 18, 2.4],
            },
          },
          {
            id: `bsel-${lv.level_name}`,
            type: "fill",
            source: sid,
            "source-layer": lv.level_name,
            minzoom: lv.minzoom,
            maxzoom: lmax,
            filter: NONE_FILTER,
            layout: { visibility: "none" },
            paint: { "fill-color": CORAL, "fill-opacity": 0.24 },
          },
          {
            id: `bselline-${lv.level_name}`,
            type: "line",
            source: sid,
            "source-layer": lv.level_name,
            minzoom: lv.minzoom,
            maxzoom: lmax,
            filter: NONE_FILTER,
            layout: { visibility: "none", "line-join": "round" },
            paint: { "line-color": CORAL_D, "line-width": 2.4 },
          },
        );
      }
    }

    // Transit network: one vector source over the contiguous line pyramid, a
    // white casing + a mode-colored line, plus hover/selected overlays driven by
    // venue_id filters (wired to clicks below). All hidden until transit mode.
    if (transit) {
      const tAbs = new URL(`${CACHE}/${transit.lines.path}`, location.href).href;
      const slayer = transit.lines.layer;
      sources.transit = {
        type: "vector",
        tiles: [`pmtiles://${tAbs}/{z}/{x}/{y}`],
        minzoom: transit.lines.minzoom,
        maxzoom: transit.lines.maxzoom,
      };
      // Zoom-ramped stroke width; `extra` thickens the casing and highlight
      // strokes uniformly above the base line at every zoom.
      const lineW = (
        extra: number,
      ): maplibregl.DataDrivenPropertyValueSpecification<number> => [
        "interpolate",
        ["linear"],
        ["zoom"],
        5, 1.1 + extra,
        9, 2.0 + extra,
        13, 3.4 + extra,
        18, 5.5 + extra,
      ];
      styleLayers.push(
        {
          id: "transit-casing",
          type: "line",
          source: "transit",
          "source-layer": slayer,
          layout: { visibility: "none", "line-join": "round", "line-cap": "round" },
          paint: { "line-color": LINE_CASING, "line-opacity": 0.85, "line-width": lineW(2.2) },
        },
        {
          id: "transit-line",
          type: "line",
          source: "transit",
          "source-layer": slayer,
          layout: { visibility: "none", "line-join": "round", "line-cap": "round" },
          paint: { "line-color": LINE_COLOR, "line-opacity": 0.9, "line-width": lineW(0) },
        },
        {
          id: "transit-hover",
          type: "line",
          source: "transit",
          "source-layer": slayer,
          filter: NO_VENUE,
          layout: { visibility: "none", "line-join": "round", "line-cap": "round" },
          paint: { "line-color": LINE_COLOR, "line-width": lineW(2.4) },
        },
        {
          id: "transit-sel",
          type: "line",
          source: "transit",
          "source-layer": slayer,
          filter: NO_VENUE,
          layout: { visibility: "none", "line-join": "round", "line-cap": "round" },
          paint: { "line-color": "#111827", "line-width": lineW(3.0) },
        },
        {
          // The selected rider's multi-leg journey: their legs across one or more
          // lines, drawn bright on top (filter = in [venue_id, …]).
          id: "transit-journey",
          type: "line",
          source: "transit",
          "source-layer": slayer,
          filter: NO_VENUE,
          layout: { visibility: "none", "line-join": "round", "line-cap": "round" },
          paint: { "line-color": "#f5b301", "line-width": lineW(3.4) },
        },
      );
      // Where the journey starts and ends: a GeoJSON overlay fed from the
      // rider's recorded geo-unit sequence (origin → interchanges → dest),
      // resolved to unit centroids. Empty until a rider with O/D data is
      // selected; old caches without the fields never populate it.
      sources["transit-od"] = {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      };
      styleLayers.push(
        {
          id: "transit-od-path",
          type: "line",
          source: "transit-od",
          filter: ["==", ["geometry-type"], "LineString"],
          layout: { visibility: "none", "line-join": "round", "line-cap": "round" },
          paint: {
            "line-color": "#111827",
            "line-opacity": 0.55,
            "line-width": 1.6,
            "line-dasharray": [1.5, 2.2],
          },
        },
        {
          id: "transit-od-point",
          type: "circle",
          source: "transit-od",
          filter: ["==", ["geometry-type"], "Point"],
          layout: { visibility: "none" },
          paint: {
            // start = hollow ring, end = filled disc, via = small dot.
            "circle-radius": [
              "match", ["get", "role"], "via", 3.5, 6.5,
            ],
            "circle-color": [
              "match", ["get", "role"],
              "start", "#ffffff",
              "end", "#111827",
              "#6b7280",
            ],
            "circle-stroke-color": "#111827",
            "circle-stroke-width": ["match", ["get", "role"], "via", 1, 2],
          },
        },
      );
    }

    const startMode = useStore.getState().mapMode;
    const bndMinZoom = boundaries
      ? Math.min(...boundaries.levels.map((l) => l.bake_zoom))
      : 0;
    const useBnd = startMode === "boundaries" && !!boundaries;

    const map = new maplibregl.Map({
      container: ref.current,
      style: { version: 8, sources, layers: styleLayers },
      minZoom: useBnd ? bndMinZoom : srcZoom,
      maxZoom: useBnd ? 22 : Math.max(...zooms) + 4,
      attributionControl: false,
      dragRotate: false,
    });
    mapRef.current = map;

    const fitArchive = (url: string, max: number) =>
      void new PMTiles(url).getHeader().then((h) => {
        map.fitBounds(
          [
            [h.minLon, h.minLat],
            [h.maxLon, h.maxLat],
          ],
          { padding: 50, animate: false, maxZoom: max },
        );
      });
    // Deterministic initial camera. If a unit is already selected (e.g.
    // returning from Inspect) focus *it* directly from its cached aggregate
    // extent; only fall back to the whole-archive fit on a cold open or if
    // the unit has no usable geometry. Done on `load` so the camera op isn't
    // lost during map init, and there's no async race with fitArchive.
    const focusSelected = async (): Promise<boolean> => {
      const sel = useStore.getState().selected;
      if (!sel) return false;
      await useStore.getState().ensureLevel(sel.level);
      const r = useStore.getState().aggRow(sel.level, sel.geo_id);
      const n = (v: unknown) =>
        typeof v === "number" && isFinite(v) ? v : null;
      const mnx = r && n(r["geo:bbox_minlon"]);
      const mny = r && n(r["geo:bbox_minlat"]);
      const mxx = r && n(r["geo:bbox_maxlon"]);
      const mxy = r && n(r["geo:bbox_maxlat"]);
      const fitMax = useBnd ? 17 : Math.max(...zooms);
      if (mnx != null && mny != null && mxx != null && mxy != null) {
        map.fitBounds(
          [
            [mnx, mny],
            [mxx, mxy],
          ],
          { padding: 60, animate: false, maxZoom: fitMax },
        );
        return true;
      }
      const lon = r && n(r["geo:lon"]);
      const lat = r && n(r["geo:lat"]);
      if (lon != null && lat != null) {
        map.jumpTo({ center: [lon, lat], zoom: fitMax });
        return true;
      }
      return false;
    };
    map.on("load", () => {
      void focusSelected().then((done) => {
        if (done) return;
        if (useBnd) fitArchive(`${CACHE}/${boundaries!.path}`, 9);
        else fitArchive(hexUrl, Math.max(...zooms));
      });
    });

    const recolor = () => {
      if (useStore.getState().mapMode !== "hexbin") return;
      const lyr = useStore.getState().hexLayer;
      const feats = map.queryRenderedFeatures({ layers: [`hex-${lyr}`] });
      let max = 0;
      for (const f of feats) {
        const c = Number(f.properties?.count ?? 0);
        if (c > max) max = c;
      }
      if (max <= 0) return;
      setDomain([1, max]);
      const ramp = rampFor(lyr);
      const stops: (number | string)[] = [];
      ramp.forEach((col, i) => {
        const t = i / (ramp.length - 1);
        const v = Math.max(1, Math.round(Math.pow(max, t)));
        stops.push(v, col);
      });
      map.setPaintProperty(`hex-${lyr}`, "fill-color", [
        "interpolate",
        ["linear"],
        ["to-number", ["get", "count"], 0],
        ...stops,
      ]);
    };
    map.on("idle", recolor);

    if (boundaries) {
      const allFillIds = boundaries.levels.map((l) => `bfill-${l.level_name}`);
      const liveFillIds = () =>
        map.style && map.isStyleLoaded()
          ? allFillIds.filter((id) => map.getLayer(id))
          : [];
      map.on("click", (e) => {
        if (useStore.getState().mapMode !== "boundaries") return;
        const ids = liveFillIds();
        if (!ids.length) return;
        const hit = map.queryRenderedFeatures(e.point, { layers: ids })[0];
        if (!hit) return;
        const p = hit.properties ?? {};
        const gid = Number(p.geo_id);
        const lvl = Number(p.level);
        if (Number.isFinite(gid) && Number.isFinite(lvl))
          void useStore.getState().drillToGeo(gid, lvl);
      });
      map.on("mousemove", (e) => {
        if (useStore.getState().mapMode !== "boundaries") {
          setTip(null);
          return;
        }
        const ids = liveFillIds();
        const hit =
          ids.length > 0
            ? map.queryRenderedFeatures(e.point, { layers: ids })[0]
            : undefined;
        map.getCanvas().style.cursor = hit ? "pointer" : "";
        // A transient empty hit usually means the tile under the cursor is
        // still streaming/overzooming, not that we left the shape. Clearing the
        // highlight here makes it blink, so keep the last highlight and only
        // reset on a real mouseout (below).
        if (!hit) return;
        const p = hit.properties ?? {};
        const gid = Number(p.geo_id);
        const lvl = Number(p.level);
        for (const lv of boundaries.levels)
          if (map.getLayer(`bhov-${lv.level_name}`))
            map.setFilter(
              `bhov-${lv.level_name}`,
              lv.level === lvl ? ["==", ["get", "geo_id"], gid] : NONE_FILTER,
            );
        const agg = useStore.getState().aggRow(lvl, gid);
        setTip({
          x: e.point.x,
          y: e.point.y,
          level: String(p.level_name ?? lvl),
          id: String(p.geo_name ?? gid),
          people: agg ? num(agg.people) : null,
          meanAge: agg && agg.mean_age != null ? num(agg.mean_age) : null,
        });
      });
      map.on("mouseout", () => {
        setTip(null);
        for (const lv of boundaries.levels)
          if (map.getLayer(`bhov-${lv.level_name}`))
            map.setFilter(`bhov-${lv.level_name}`, NONE_FILTER);
      });
    }

    if (transit) {
      // Hover: highlight the line under the cursor (its riders/click land in a
      // later step). queryRenderedFeatures targets the colored line layer; the
      // overlay is filtered to that feature's venue_id.
      const clearHover = () => {
        if (map.getLayer("transit-hover")) map.setFilter("transit-hover", NO_VENUE);
        map.getCanvas().style.cursor = "";
      };
      map.on("mousemove", (e) => {
        if (useStore.getState().mapMode !== "transit") return;
        if (!map.getLayer("transit-line")) return;
        const hit = map.queryRenderedFeatures(e.point, { layers: ["transit-line"] })[0];
        if (!hit) {
          clearHover();
          return;
        }
        map.getCanvas().style.cursor = "pointer";
        const vid = Number(hit.properties?.venue_id);
        if (Number.isFinite(vid))
          map.setFilter("transit-hover", ["==", ["get", "venue_id"], vid]);
      });
      map.on("mouseout", clearHover);
      // Click a line → select it (the panel loads its riders). Reads venue_id /
      // line_id / mode / rider_count straight off the rendered feature.
      map.on("click", (e) => {
        if (useStore.getState().mapMode !== "transit") return;
        if (!map.getLayer("transit-line")) return;
        const hit = map.queryRenderedFeatures(e.point, { layers: ["transit-line"] })[0];
        if (!hit) return;
        const p = hit.properties ?? {};
        const venueId = Number(p.venue_id);
        if (!Number.isFinite(venueId)) return;
        useStore.getState().selectTransitLine({
          venueId,
          lineId: String(p.line_id ?? venueId),
          mode: String(p.mode ?? "train"),
          riderCount: Number(p.rider_count ?? 0),
        });
      });
    }

    return () => {
      map.remove();
      mapRef.current = null;
      maplibregl.removeProtocol("pmtiles");
    };
  }, [manifest, boundaries, transit]);

  // Apply the active render mode: layer visibility + zoom constraints. The three
  // overlays are mutually exclusive — only the active mode's layers are shown.
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const apply = () => {
      const bnd = mapMode === "boundaries" && !!boundaries;
      const tr = mapMode === "transit" && !!transit;
      for (const l of layers)
        if (map.getLayer(`hex-${l}`))
          map.setLayoutProperty(
            `hex-${l}`,
            "visibility",
            mapMode === "hexbin" && l === hexLayer ? "visible" : "none",
          );
      if (boundaries)
        for (const lv of boundaries.levels)
          for (const pfx of ["bfill", "bhov", "bline", "bsel", "bselline"])
            if (map.getLayer(`${pfx}-${lv.level_name}`))
              map.setLayoutProperty(
                `${pfx}-${lv.level_name}`,
                "visibility",
                bnd ? "visible" : "none",
              );
      if (transit)
        for (const id of [
          "transit-casing", "transit-line", "transit-hover", "transit-sel", "transit-journey",
          "transit-od-path", "transit-od-point",
        ])
          if (map.getLayer(id))
            map.setLayoutProperty(id, "visibility", tr ? "visible" : "none");

      const zs = manifest!.artifacts.hexbin?.zooms ?? [0];
      if (tr) {
        map.setMinZoom(transit.lines.minzoom);
        map.setMaxZoom(22);
      } else if (bnd) {
        map.setMinZoom(Math.min(...boundaries!.levels.map((l) => l.bake_zoom)));
        map.setMaxZoom(22);
      } else {
        const sorted = [...zs].sort((a, b) => a - b);
        map.setMinZoom(sorted[Math.min(1, sorted.length - 1)] ?? 0);
        map.setMaxZoom(Math.max(...zs) + 4);
      }
    };
    if (map.isStyleLoaded()) apply();
    else map.once("styledata", apply);
  }, [mapMode, hexLayer, layers, boundaries, transit, manifest]);

  // Basemap on/off (only when the operator opted in).
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !basemap) return;
    const apply = () => {
      if (map.getLayer("basemap"))
        map.setLayoutProperty(
          "basemap",
          "visibility",
          basemapOn ? "visible" : "none",
        );
    };
    if (map.isStyleLoaded()) apply();
    else map.once("styledata", apply);
  }, [basemap, basemapOn]);

  // Selected unit (from path OR map click) → highlight + fit to its extent.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !boundaries) return;
    let alive = true;
    const { selected, ensureLevel } = useStore.getState();
    if (!selected) return;
    const bandByLevel = new Map<number, BoundaryLevel>(
      boundaries.levels.map((l) => [l.level, l]),
    );

    void ensureLevel(selected.level).then(() => {
      if (!alive) return;
      const sel = useStore.getState().selected;
      if (!sel) return;

      const setSel = () => {
        for (const lv of boundaries.levels) {
          const f: maplibregl.FilterSpecification =
            lv.level === sel.level
              ? ["==", ["get", "geo_id"], sel.geo_id]
              : NONE_FILTER;
          if (map.getLayer(`bsel-${lv.level_name}`))
            map.setFilter(`bsel-${lv.level_name}`, f);
          if (map.getLayer(`bselline-${lv.level_name}`))
            map.setFilter(`bselline-${lv.level_name}`, f);
        }
      };
      if (map.isStyleLoaded()) setSel();
      else map.once("styledata", setSel);

      if (useStore.getState().mapMode !== "boundaries") return;
      const r = useStore.getState().aggRow(sel.level, sel.geo_id);
      const n = (v: unknown) => (typeof v === "number" && isFinite(v) ? v : null);
      const mnx = r && n(r["geo:bbox_minlon"]);
      const mny = r && n(r["geo:bbox_minlat"]);
      const mxx = r && n(r["geo:bbox_maxlon"]);
      const mxy = r && n(r["geo:bbox_maxlat"]);
      const band = bandByLevel.get(sel.level);
      const fitMax = Math.min(17, band ? band.maxzoom : 17);
      if (mnx != null && mny != null && mxx != null && mxy != null) {
        map.fitBounds(
          [
            [mnx, mny],
            [mxx, mxy],
          ],
          { padding: 60, maxZoom: fitMax, duration: 600 },
        );
      } else {
        const lon = r && n(r["geo:lon"]);
        const lat = r && n(r["geo:lat"]);
        if (lon != null && lat != null)
          map.easeTo({ center: [lon, lat], zoom: fitMax, duration: 600 });
      }
    });
    return () => {
      alive = false;
    };
  }, [boundaries, selected, mapMode]);

  // Reflect the transit selection onto the map: the selected line (dark stroke)
  // and the selected rider's journey legs (bright stroke). Both are venue_id
  // filters on overlay layers already in the style.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !transit) return;
    // The highlight covers the active line plus every pinned compare line, so
    // a shared-ridership comparison shows all its lines lit at once.
    const selVenues = [
      ...transitCompare.map((l) => l.venueId),
      ...(transitLine ? [transitLine.venueId] : []),
    ];
    const apply = () => {
      if (map.getLayer("transit-sel"))
        map.setFilter(
          "transit-sel",
          selVenues.length
            ? ["in", ["get", "venue_id"], ["literal", selVenues]]
            : NO_VENUE,
        );
      if (map.getLayer("transit-journey"))
        map.setFilter(
          "transit-journey",
          transitJourney.length
            ? ["in", ["get", "venue_id"], ["literal", transitJourney]]
            : NO_VENUE,
        );
    };
    if (map.isStyleLoaded()) apply();
    else map.once("styledata", apply);
  }, [transit, transitLine, transitJourney, transitCompare]);

  // The selected rider's recorded geo-unit sequence → start/end markers and a
  // dashed path through the interchanges. Centroids come from the aggregate
  // rows (geo:lon/geo:lat, baked with the boundary overlay); units that lack
  // them (or a cache without O/D fields) simply contribute nothing.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !transit) return;
    let alive = true;
    const { findUnit } = useStore.getState();
    void Promise.all(transitOD.map((u) => findUnit(u))).then((rows) => {
      if (!alive || !mapRef.current) return;
      const pts: { lon: number; lat: number; name: string }[] = [];
      for (const r of rows) {
        const lon = Number(r?.["geo:lon"]);
        const lat = Number(r?.["geo:lat"]);
        if (r && isFinite(lon) && isFinite(lat))
          pts.push({ lon, lat, name: r.geo_name });
      }
      const features: GeoJSON.Feature[] = pts.map((p, i) => ({
        type: "Feature",
        properties: {
          role: i === 0 ? "start" : i === pts.length - 1 ? "end" : "via",
          name: p.name,
        },
        geometry: { type: "Point", coordinates: [p.lon, p.lat] },
      }));
      if (pts.length >= 2)
        features.push({
          type: "Feature",
          properties: { role: "path" },
          geometry: {
            type: "LineString",
            coordinates: pts.map((p) => [p.lon, p.lat]),
          },
        });
      const apply = () => {
        const src = map.getSource("transit-od") as maplibregl.GeoJSONSource | undefined;
        if (src)
          src.setData({
            type: "FeatureCollection",
            features: pts.length >= 2 ? features : [],
          });
      };
      if (map.isStyleLoaded()) apply();
      else map.once("styledata", apply);
    });
    return () => {
      alive = false;
    };
  }, [transit, transitOD]);

  // Entering transit mode → frame the whole line network (its PMTiles bounds).
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !transit || mapMode !== "transit") return;
    const url = new URL(`${CACHE}/${transit.lines.path}`, location.href).href;
    let alive = true;
    void new PMTiles(url).getHeader().then((h) => {
      if (!alive || !mapRef.current) return;
      map.fitBounds(
        [
          [h.minLon, h.minLat],
          [h.maxLon, h.maxLat],
        ],
        { padding: 48, maxZoom: 9, duration: 500 },
      );
    });
    return () => {
      alive = false;
    };
  }, [transit, mapMode]);

  const inBnd = mapMode === "boundaries" && !!boundaries;
  const inTransit = mapMode === "transit" && !!transit;
  const zoomBy = (d: number) => {
    const m = mapRef.current;
    if (m) m.easeTo({ zoom: m.getZoom() + d, duration: 200 });
  };

  return (
    <div className="maproot">
      <div ref={ref} style={{ position: "absolute", inset: 0 }} />

      <div className="map-hint">
        {inTransit
          ? "train & tube commute lines · hover to highlight a route"
          : inBnd
            ? "click a region to drill in · zoom to walk levels"
            : "density backdrop · navigate the geography in the left panel"}
      </div>

      <div className="zoomctl">
        <button onClick={() => zoomBy(1)} aria-label="zoom in">+</button>
        <button onClick={() => zoomBy(-1)} aria-label="zoom out">−</button>
      </div>

      {tip && (
        <div
          className="efloat maptip"
          style={{
            left: Math.min(tip.x + 14, (ref.current?.clientWidth ?? 9999) - 200),
            top: Math.min(tip.y + 14, (ref.current?.clientHeight ?? 9999) - 90),
          }}
        >
          <div className="c">
            {tip.level} · {tip.id}
          </div>
          {tip.people != null ? (
            <div className="big">
              <span className="n">{compact(tip.people)}</span>
              <span className="u">people</span>
            </div>
          ) : (
            <div className="sub" style={{ marginTop: 6 }}>
              select to load detail
            </div>
          )}
          {tip.meanAge != null && (
            <div className="sub">
              mean age <b>{tip.meanAge.toFixed(1)}</b>
            </div>
          )}
        </div>
      )}

      <div className="efloat layers-card">
        <div className="lh">
          <span className="t">Layers</span>
          <span className="m">
            {boundaries ? `${boundaries.levels.length} levels` : "density"}
          </span>
        </div>

        <div className="scap">Overlay</div>
        <div className="chiprow">
          <button
            className={"chip" + (inBnd ? " on" : "")}
            disabled={!boundaries}
            onClick={() => setMapMode("boundaries")}
          >
            Boundaries
          </button>
          {transit && (
            <button
              className={"chip" + (inTransit ? " on" : "")}
              onClick={() => setMapMode("transit")}
            >
              Transit
            </button>
          )}
          {layers.map((l) => (
            <button
              key={l}
              className={"chip" + (mapMode === "hexbin" && hexLayer === l ? " on" : "")}
              onClick={() => {
                setMapMode("hexbin");
                setHexLayer(l);
              }}
            >
              {l}
            </button>
          ))}
        </div>

        {inTransit && transit && (
          <>
            <div className="scap">Transit lines</div>
            <div className="chiprow" style={{ gap: 14 }}>
              <span className="tlegend">
                <i style={{ background: TRAIN }} /> train · {transit.summary.train}
              </span>
              <span className="tlegend">
                <i style={{ background: TUBE }} /> tube · {transit.summary.tube}
              </span>
            </div>
          </>
        )}

        <div className="scap">Basemap</div>
        <div className="chiprow" style={{ marginBottom: 0 }}>
          <button
            className={"chip" + (!basemapOn ? " on" : "")}
            onClick={() => setBasemapOn(false)}
          >
            Paper
          </button>
          <button
            className={"chip" + (basemap && basemapOn ? " on" : "")}
            disabled={!basemap}
            onClick={() => setBasemapOn(true)}
          >
            {basemap ? basemap.name : "OSM"}
          </button>
        </div>

        <div className="foot">
          <span className="fk">{inTransit ? "Network" : "Color by"}</span>
          <span className="fv mono">
            {inTransit
              ? `${transit!.summary.lines} lines · ${compact(transit!.summary.rider_memberships)} riders`
              : inBnd
                ? "people"
                : `${hexLayer} · ${compact(domain[0])}–${compact(domain[1])}`}
          </span>
        </div>
      </div>

      {basemap && basemapOn && basemap.attribution && (
        <div className="map-attrib">{basemap.attribution}</div>
      )}
    </div>
  );
}
