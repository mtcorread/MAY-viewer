import { useEffect, useState } from "react";
import { useStore, type AggRow } from "../state/store";
import { parseAggregateColumns, type MetricGroup } from "../data/columns";
import type { Labels } from "../data/labels";
import { num, nf, compact, pct } from "../util/format";

// Right "Stats" panel (Map mode). Hero block + one section per demographic,
// bars alternating coral / pine for visual rhythm. Every label derives from
// the aggregate column tokens / manifest — nothing hardcoded.

export function StatsPanel() {
  const { selected, ensureLevel, labels } = useStore();
  const [row, setRow] = useState<AggRow | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let alive = true;
    if (!selected) return;
    setLoading(true);
    void ensureLevel(selected.level).then((d) => {
      if (!alive) return;
      setRow(d.byId.get(selected.geo_id) ?? null);
      setLoading(false);
    });
    return () => {
      alive = false;
    };
  }, [selected, ensureLevel]);

  if (!selected)
    return (
      <div className="stats">
        <div className="hero-cap">No selection</div>
        <div className="hero-sub" style={{ opacity: 0.6, fontWeight: 500 }}>
          Pick a region on the map.
        </div>
      </div>
    );
  if (loading) return <div className="stats loading pulse">reading aggregate…</div>;
  if (!row) return <div className="stats loading">no aggregate row for this unit</div>;

  const { groups, hasMeanAge } = parseAggregateColumns(Object.keys(row));
  const people = num(row.people);

  return (
    <div className="stats fade-in" key={selected.geo_id}>
      <div>
        <div className="hero-cap">
          {selected.level_name} · {selected.geo_name}
        </div>
        <div className="hero-num">
          <span className="n">{nf.format(people)}</span>
          <span className="u">People</span>
        </div>
        {hasMeanAge && (
          <div className="hero-sub">
            mean age <b>{num(row.mean_age).toFixed(1)}</b>
            <span className="dot">·</span>
            {compact(people)} in {selected.level_name}
          </div>
        )}
      </div>

      {groups.map((g, i) =>
        g.kind === "venues" ? (
          <VenueGroup key={g.key} g={g} row={row} labels={labels} />
        ) : (
          <DistGroup
            key={g.key}
            g={g}
            row={row}
            total={people}
            color={i % 2 === 0 ? "coral" : "pine"}
            labels={labels}
          />
        ),
      )}
    </div>
  );
}

function DistGroup({
  g,
  row,
  total,
  color,
  labels,
}: {
  g: MetricGroup;
  row: AggRow;
  total: number;
  color: "coral" | "pine";
  labels: Labels;
}) {
  // Apply user-supplied labels — only the *raw* token is looked up;
  // humanise()d English labels for unmapped values stay as-is.
  const itemLabel = (raw: string, fallback: string): string => {
    if (g.kind === "age") return fallback; // ranges aren't categorical codes
    const m = labels[g.key];
    return (m && m[raw]) || fallback;
  };
  const vals = g.items.map((it) => ({
    ...it,
    label: itemLabel(it.raw, it.label),
    v: num(row[it.col]),
  }));
  const sum = vals.reduce((a, b) => a + b.v, 0);
  const denom = (g.kind === "age" || g.kind === "sex" ? total || sum : sum) || 1;
  return (
    <section className="statsec">
      <div className="statsec-h">
        <span className="t">{g.label}</span>
        <span className="n">n = {compact(sum)}</span>
      </div>
      {vals.map((x) => (
        <div className="statrow" key={x.col}>
          <span className="lbl" title={x.label}>
            {x.label}
          </span>
          <span className="bar-track">
            <span
              className={"bar-fill " + color}
              style={{ width: `${(100 * x.v) / denom}%` }}
            />
          </span>
          <span className="val">
            {compact(x.v)} <span className="pc">{pct(x.v, denom)}</span>
          </span>
        </div>
      ))}
    </section>
  );
}

function VenueGroup({
  g,
  row,
  labels,
}: {
  g: MetricGroup;
  row: AggRow;
  labels: Labels;
}) {
  const m = labels.venues;
  const rows = g.items
    .map((it) => ({
      label: (m && m[it.raw]) || it.label,
      count: num(row[it.col]),
      occ: g.occ?.[it.raw] ? num(row[g.occ[it.raw]]) : null,
    }))
    .filter((r) => r.count > 0 || (r.occ ?? 0) > 0);
  return (
    <section className="statsec">
      <div className="statsec-h">
        <span className="t">{g.label}</span>
        <span className="n">{rows.length} types</span>
      </div>
      <table className="vtable">
        <thead>
          <tr>
            <th>type</th>
            <th>count</th>
            <th>occupancy</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.label}>
              <td className="k">{r.label}</td>
              <td className="n">{nf.format(r.count)}</td>
              <td className="o">{r.occ == null ? "—" : nf.format(r.occ)}</td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="k" colSpan={3}>
                none in this unit
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </section>
  );
}
