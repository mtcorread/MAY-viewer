import { useEffect, useState } from "react";
import { useStore, type AggRow } from "../state/store";
import { num, compact, nf } from "../util/format";

// Tree-driven geo drill-down (the agreed navigation model): the breadcrumb
// + child list are built from the aggregate parquets' parent_id / level
// columns. The map is a synchronized density backdrop, not the click target.

export function GeoTree() {
  const { levels, path, selected, drillTo, drillUpTo, ensureLevel } = useStore();
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

  if (!selected) return <div className="loading pulse">locating root…</div>;

  const maxPeople = kids.reduce((m, r) => Math.max(m, num(r.people)), 0) || 1;

  return (
    <>
      <div className="section">
        <div className="eyebrow" style={{ marginBottom: 12 }}>Geography</div>
        <nav className="crumbs">
          {path.map((n, i) => (
            <span key={n.geo_id} style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
              {i > 0 && <span className="crumb-sep">›</span>}
              <button
                className={"crumb" + (i === path.length - 1 ? " cur" : "")}
                onClick={() => drillUpTo(i)}
              >
                <span className="lv">{n.level_name}</span>
                <span className="nm">{n.geo_name}</span>
              </button>
            </span>
          ))}
        </nav>
      </div>

      <div className="section" style={{ flex: 1, borderBottom: 0 }}>
        <div className="section-h">
          <h3>{childLevel ? childLevel.name : "Leaf unit"}</h3>
          <span className="meta mono">
            {childLevel ? `${nf.format(kids.length)} units` : "no finer level"}
          </span>
        </div>

        {loading ? (
          <div className="loading pulse">reading aggregates…</div>
        ) : !childLevel ? (
          <div className="loading">
            This is a leaf unit. Switch to <b>Inspect</b> to open its people &
            venues.
          </div>
        ) : (
          <div className="child-list">
            {kids.map((r) => {
              const p = num(r.people);
              return (
                <button
                  key={r.geo_id}
                  className="child"
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
                  <span
                    className="bar"
                    style={{ width: Math.max(3, (38 * p) / maxPeople) }}
                  />
                  <span className="ct">{compact(p)}</span>
                  <span className="chev">›</span>
                </button>
              );
            })}
            {kids.length === 0 && <div className="loading">no child units</div>}
          </div>
        )}
      </div>
    </>
  );
}
