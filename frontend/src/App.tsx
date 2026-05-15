import { useEffect } from "react";
import { useStore } from "./state/store";
import { compact } from "./util/format";
import { MapView } from "./components/MapView";
import { GeoTree } from "./components/GeoTree";
import { StatsPanel } from "./components/StatsPanel";
import { Inspector } from "./components/Inspector";

export function App() {
  const { manifest, error, mode, setMode, init } = useStore();

  useEffect(() => {
    void init();
  }, [init]);

  if (error)
    return (
      <div className="center-screen">
        <div className="boot">
          <div className="eyebrow" style={{ marginBottom: 10 }}>MAY-viewer</div>
          <div className="error-box">cache unreachable — {error}</div>
        </div>
      </div>
    );

  if (!manifest)
    return (
      <div className="center-screen">
        <div className="boot">
          <div className="ring" />
          <div className="eyebrow">reading manifest</div>
        </div>
      </div>
    );

  const s = manifest.schema;

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          MAY<b>·</b>viewer
        </div>
        <div className="sep" />
        <div className="world-name mono">{manifest.source.name}</div>

        <div className="modes" style={{ marginLeft: 28 }}>
          <button className={mode === "map" ? "on" : ""} onClick={() => setMode("map")}>
            Map
          </button>
          <button className={mode === "inspect" ? "on" : ""} onClick={() => setMode("inspect")}>
            Inspect
          </button>
        </div>

        <div className="counts">
          <div className="stat">
            <span className="v mono">{compact(s.num_people)}</span>
            <span className="k eyebrow">people</span>
          </div>
          <div className="stat">
            <span className="v mono">{compact(s.num_venues)}</span>
            <span className="k eyebrow">venues</span>
          </div>
          <div className="stat">
            <span className="v mono">{compact(s.num_geo_units)}</span>
            <span className="k eyebrow">geo units</span>
          </div>
        </div>
      </header>

      <div className="body">
        <aside className="rail">
          <GeoTree />
        </aside>

        <main className="stage">
          <MapView />
        </main>

        <aside className="rail right">
          {mode === "map" ? <StatsPanel /> : <Inspector />}
        </aside>
      </div>
    </div>
  );
}
