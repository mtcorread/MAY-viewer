import { useEffect, useState } from "react";
import { useStore, type AggRow } from "../state/store";
import { num, compact, nf } from "../util/format";

// Left "Geography" panel (Map mode). Three stacked sections:
//   (a) Geography path  — the breadcrumb of selected ancestors, one row per
//       level; clicking a row drills back up to it.
//   (b) Drill / leaf    — child units of the current selection so drilling
//       down still works without the map (hexbin-only worlds); or, at a
//       leaf, the "no finer level → switch to Inspect" explainer.
//   (c) Levels in dataset — the manifest's level reference, pinned bottom.
// Every label is painted from the manifest / aggregate columns at runtime.

export function GeoTree() {
  const { manifest, levels, path, selected, drillTo, drillUpTo, ensureLevel } =
    useStore();
  const [kids, setKids] = useState<AggRow[]>([]);
  const [loading, setLoading] = useState(false);

  const lvIdx = selected ? levels.findIndex((l) => l.value === selected.level) : -1;
  const childLevel = lvIdx >= 0 && lvIdx + 1 < levels.length ? levels[lvIdx + 1] : null;

  useEffect(() => {
    let alive = true;
    if (!selected || !childLevel) {
      setKids([]);
      return;
    }
    setLoading(true);
    void ensureLevel(childLevel.value).then((d) => {
      if (!alive) return;
      const rows = (d.byParent.get(selected.geo_id) ?? [])
        .slice()
        .sort((a, b) => num(b.people) - num(a.people));
      setKids(rows);
      setLoading(false);
    });
    return () => {
      alive = false;
    };
  }, [selected, childLevel, ensureLevel]);

  if (!selected) return <div className="geopanel loading pulse">locating root…</div>;

  const upl = manifest?.geo.units_per_level ?? {};

  return (
    <aside className="geopanel">
      <div>
        <div className="cap">Geography path</div>
        <div className="gpath">
          {path.map((n, i) => {
            const active = i === path.length - 1;
            const isId = n.geo_name === String(n.geo_id) || /^[A-Z]\d{6,}$/.test(n.geo_name);
            return (
              <button
                key={n.geo_id}
                className={"crumbrow" + (active ? " on" : "")}
                onClick={() => drillUpTo(i)}
              >
                <span className="tag">{n.level_name}</span>
                <span className={"val" + (isId ? " mono" : "")}>{n.geo_name}</span>
                <span className="chev">{active ? "●" : "↥"}</span>
              </button>
            );
          })}
        </div>
      </div>

      {childLevel ? (
        <div>
          <div className="cap" style={{ display: "flex", justifyContent: "space-between" }}>
            <span>{childLevel.name}</span>
            <span className="mono" style={{ letterSpacing: 0 }}>
              {loading ? "…" : `${nf.format(kids.length)} units`}
            </span>
          </div>
          {loading ? (
            <div className="loading pulse" style={{ padding: "8px 0" }}>
              reading aggregates…
            </div>
          ) : kids.length === 0 ? (
            <div className="leaf-b">no child units</div>
          ) : (
            <div className="children">
              {kids.map((r) => (
                <button
                  key={r.geo_id}
                  className="childrow"
                  onClick={() =>
                    drillTo({
                      geo_id: r.geo_id,
                      geo_name: r.geo_name,
                      level: r.level,
                      level_name: r.level_name,
                    })
                  }
                >
                  <span className="nm">{r.geo_name}</span>
                  <span className="ct">{compact(num(r.people))}</span>
                  <span className="chev">↧</span>
                </button>
              ))}
            </div>
          )}
        </div>
      ) : (
        <div>
          <div className="cap">Leaf unit</div>
          <div className="leaf-h">No finer level</div>
          <div className="leaf-b">
            This is a leaf unit. Switch to <b>Inspect</b> to open its people and
            venues.
          </div>
        </div>
      )}

      <div className="levels">
        <div className="cap">Levels in dataset</div>
        {levels.map((l) => {
          const count = num(upl[String(l.value)]);
          return (
            <div key={l.value} className="lvrow">
              <span className="lt">{l.name}</span>
              <span className="ld">
                {l.value === selected.level ? "current level" : `level ${l.value}`}
              </span>
              <span className="lc">{count ? nf.format(count) : "—"}</span>
            </div>
          );
        })}
      </div>
    </aside>
  );
}
