import { useEffect, useState } from "react";
import { useStore, type AggRow } from "../state/store";
import { parseAggregateColumns, type MetricGroup } from "../data/columns";
import { num, nf, compact, pct } from "../util/format";

// Live, schema-labelled breakdown for the selected geo unit. Every label is
// derived from the aggregate column tokens / manifest — nothing hardcoded.

export function StatsPanel() {
  const { selected, ensureLevel } = useStore();
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

  if (!selected) return <div className="insp-empty">No unit selected.</div>;
  if (loading) return <div className="loading pulse">reading aggregate…</div>;
  if (!row) return <div className="loading">no aggregate row for this unit</div>;

  const { groups, hasMeanAge } = parseAggregateColumns(Object.keys(row));
  const people = num(row.people);

  return (
    <div className="fade-in" key={selected.geo_id}>
      <div className="section">
        <div className="eyebrow" style={{ marginBottom: 10 }}>
          {selected.level_name} · {selected.geo_name}
        </div>
        <div className="bignum">
          <span className="n mono">{nf.format(people)}</span>
          <span className="u">people</span>
        </div>
        {hasMeanAge && (
          <div className="subnums">
            <span>
              mean age <span className="v mono">{num(row.mean_age).toFixed(1)}</span>
            </span>
          </div>
        )}
      </div>

      {groups.map((g) =>
        g.kind === "venues" ? (
          <VenueGroup key={g.key} g={g} row={row} />
        ) : (
          <DistGroup key={g.key} g={g} row={row} total={people} />
        ),
      )}
    </div>
  );
}

function DistGroup({ g, row, total }: { g: MetricGroup; row: AggRow; total: number }) {
  const vals = g.items.map((it) => ({ ...it, v: num(row[it.col]) }));
  const sum = vals.reduce((a, b) => a + b.v, 0);
  const denom = g.kind === "age" || g.kind === "sex" ? total || sum : sum;
  const max = vals.reduce((m, x) => Math.max(m, x.v), 0) || 1;
  return (
    <div className="metric-group">
      <div className="mg-h">
        <span className="t">{g.label}</span>
        <span className="s mono">{compact(sum)}</span>
      </div>
      <div className="bars">
        {vals.map((x) => (
          <div className="bar-row" key={x.col}>
            <span className="lbl" title={x.label}>{x.label}</span>
            <span className="bar-track">
              <span className="bar-fill" style={{ width: `${(100 * x.v) / max}%` }} />
            </span>
            <span className="val">
              {compact(x.v)} <span className="pc">{pct(x.v, denom)}</span>
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function VenueGroup({ g, row }: { g: MetricGroup; row: AggRow }) {
  const rows = g.items
    .map((it) => ({
      label: it.label,
      count: num(row[it.col]),
      occ: g.occ?.[it.raw] ? num(row[g.occ[it.raw]]) : null,
    }))
    .filter((r) => r.count > 0 || (r.occ ?? 0) > 0);
  return (
    <div className="metric-group">
      <div className="mg-h">
        <span className="t">{g.label}</span>
        <span className="s mono">{rows.length} types present</span>
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
              <td className="k" colSpan={3}>none in this unit</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
