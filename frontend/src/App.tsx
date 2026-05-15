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
    <div className={"app" + (mode === "inspect" ? " inspect" : "")}>
      <header className="topbar">
        <div className="brand">
          <span className="w">May</span>
          <span className="t">· Viewer</span>
        </div>
        <div className="sep" />
        <div className="file mono">{manifest.source.name}</div>

        <div className="modes">
          <button className={mode === "map" ? "on" : ""} onClick={() => setMode("map")}>
            Map
          </button>
          <button className={mode === "inspect" ? "on" : ""} onClick={() => setMode("inspect")}>
            Inspect
          </button>
        </div>

        <div className="kpis">
          <div className="kpi">
            <div className="v">{compact(s.num_people)}</div>
            <div className="k">People</div>
          </div>
          <div className="kpi">
            <div className="v">{compact(s.num_venues)}</div>
            <div className="k">Venues</div>
          </div>
          <div className="kpi">
            <div className="v">{compact(s.num_geo_units)}</div>
            <div className="k">Geo units</div>
          </div>
        </div>
      </header>

      {mode === "map" ? (
        <>
          <GeoTree />
          <MapView />
          <StatsPanel />
        </>
      ) : (
        <Inspector />
      )}
    </div>
  );
}
