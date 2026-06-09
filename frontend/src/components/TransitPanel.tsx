import { useEffect, useMemo, useState } from "react";
import { useStore } from "../state/store";
import {
  readRowGroup,
  readGeoUnit,
  resolvePeopleByIds,
} from "../data/parquet";
import { humanize } from "../data/columns";
import { cell, nf, num } from "../util/format";

// Right "Transit" panel (Map mode, transit overlay). Click a line on the map →
// its riders load here (read by row group from transit_riders.parquet, keyed by
// the line's venue_id). Pick a rider → their ordered multi-leg journey loads
// (transit_chains.parquet, keyed by the rider's home geo unit) and the map
// highlights those legs. Reads are bounded row-group fetches — never whole files.

type Row = Record<string, unknown>;
const PAGE = 80; // riders per page

// Per-mode accent (matches the map line colors).
const ACCENT: Record<string, string> = { train: "#e8743b", tube: "#2f6fed" };

interface Leg {
  legIdx: number;
  board: number;
  alight: number;
  venueId: number;
  lineId: string;
  mode: string;
}

// Board/alight are minutes-from-day-start in the simulation's commute window
// (small values like 16, 23, 70 — not clock time), so show the raw minute mark.
function tmin(min: number): string {
  return Number.isFinite(min) ? `${min}′` : "—";
}

export function TransitPanel() {
  const {
    manifest,
    transitLine,
    transitRider,
    selectTransitRider,
    setTransitJourney,
  } = useStore();
  const transit = manifest!.artifacts.transit!;
  const dd = manifest!.artifacts.drilldown;

  const [riders, setRiders] = useState<Row[] | null>(null);
  const [page, setPage] = useState(0);

  // Load the clicked line's riders (one row-group read).
  useEffect(() => {
    setRiders(null);
    setPage(0);
    if (!transitLine) return;
    let alive = true;
    const rg = transit.riders.row_groups[String(transitLine.venueId)];
    if (rg === undefined) {
      setRiders([]);
      return;
    }
    void readRowGroup(transit.riders.path, rg).then((rows) => {
      if (alive) setRiders(rows);
    });
    return () => {
      alive = false;
    };
  }, [transitLine, transit]);

  // home geo unit per rider (carried on the riders row) — used to index the
  // chains shard and resolve the rider's attribute row.
  const homeByPid = useMemo(() => {
    const m = new Map<number, number>();
    for (const r of riders ?? []) m.set(num(r.person_id), num(r.home_geo_unit));
    return m;
  }, [riders]);

  if (!transitLine)
    return (
      <div className="stats">
        <div className="hero-cap">Transit</div>
        <div className="hero-sub" style={{ opacity: 0.6, fontWeight: 500 }}>
          Click a line on the map to see who rides it.
        </div>
        <div className="tp-legend-note">
          {transit.summary.lines} lines · {nf.format(transit.summary.rider_memberships)} riderships
        </div>
      </div>
    );

  const accent = ACCENT[transitLine.mode] ?? ACCENT.train;
  const list = riders ?? [];
  const pageCount = Math.max(1, Math.ceil(list.length / PAGE));
  const clamped = Math.min(page, pageCount - 1);
  const slice = list.slice(clamped * PAGE, clamped * PAGE + PAGE);

  return (
    <div className="stats fade-in" key={transitLine.venueId}>
      <div>
        <div className="hero-cap" style={{ color: accent }}>
          {humanize(transitLine.mode)} line
        </div>
        <div className="ti tp-title">{transitLine.lineId}</div>
        <div className="hero-sub">
          <b>{nf.format(transitLine.riderCount)}</b> riders
          <span className="dot">·</span>venue {transitLine.venueId}
        </div>
      </div>

      {transitRider != null ? (
        <RiderJourney
          pid={transitRider}
          homeUnit={homeByPid.get(transitRider) ?? -1}
          onBack={() => selectTransitRider(null)}
          setJourney={setTransitJourney}
          peopleArt={dd.people}
        />
      ) : (
        <>
          <div className="dsub" style={{ paddingLeft: 0, paddingRight: 0 }}>
            <span className="t">Riders</span>
            <span className="m">
              {riders == null
                ? "reading…"
                : list.length > PAGE
                  ? `${nf.format(clamped * PAGE + 1)}–${nf.format(
                      Math.min((clamped + 1) * PAGE, list.length),
                    )} of ${nf.format(list.length)}`
                  : `${nf.format(list.length)} total`}
            </span>
          </div>
          {riders == null ? (
            <div className="col-empty pulse">range-reading row group…</div>
          ) : (
            <div className="tp-riders">
              {slice.map((r) => {
                const pid = num(r.person_id);
                return (
                  <button
                    key={pid}
                    className="tp-rider"
                    onClick={() => selectTransitRider(pid)}
                  >
                    <span className="tp-dot" style={{ background: accent }} />
                    <span className="tp-pid">{pid}</span>
                    <span className="tp-go">view journey ›</span>
                  </button>
                );
              })}
              {list.length === 0 && (
                <div className="col-empty">No riders recorded for this line.</div>
              )}
              {pageCount > 1 && (
                <div className="pager">
                  <button
                    className="pgbtn"
                    disabled={clamped === 0}
                    onClick={() => setPage(clamped - 1)}
                  >
                    ‹ Prev
                  </button>
                  <span className="pgmeta">
                    page {clamped + 1} / {nf.format(pageCount)}
                  </span>
                  <button
                    className="pgbtn"
                    disabled={clamped >= pageCount - 1}
                    onClick={() => setPage(clamped + 1)}
                  >
                    Next ›
                  </button>
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}

// A selected rider's ordered journey: fetch their legs (chains shard, keyed by
// home unit), push the leg venue_ids to the map highlight, and resolve the
// rider's own attribute row for a small header.
function RiderJourney({
  pid,
  homeUnit,
  onBack,
  setJourney,
  peopleArt,
}: {
  pid: number;
  homeUnit: number;
  onBack: () => void;
  setJourney: (venueIds: number[]) => void;
  peopleArt: import("../data/manifest").DrilldownArtifact;
}) {
  const transit = useStore((s) => s.manifest!.artifacts.transit!);
  const [legs, setLegs] = useState<Leg[] | null>(null);
  const [person, setPerson] = useState<Row | null>(null);

  useEffect(() => {
    let alive = true;
    setLegs(null);
    setPerson(null);
    // Legs from the chains shard (home-unit row group), filtered to this rider.
    const rg =
      homeUnit >= 0 ? transit.chains.row_groups[String(homeUnit)] : undefined;
    const legsP =
      rg === undefined
        ? Promise.resolve<Row[]>([])
        : readRowGroup(transit.chains.path, rg);
    void legsP.then((rows) => {
      if (!alive) return;
      const mine: Leg[] = rows
        .filter((r) => num(r.person_id) === pid)
        .map((r) => ({
          legIdx: num(r.leg_idx),
          board: num(r.t_board_min),
          alight: num(r.t_alight_min),
          venueId: num(r.venue_id),
          lineId: String(r.line_id ?? ""),
          mode: String(r.mode ?? ""),
        }))
        .sort((a, b) => a.legIdx - b.legIdx);
      setLegs(mine);
      setJourney(mine.map((l) => l.venueId));
    });

    // Resolve the rider's attribute row (one id lazily, or one unit read).
    const personP = peopleArt.lazy
      ? resolvePeopleByIds(peopleArt, [pid])
      : homeUnit >= 0
        ? readGeoUnit(peopleArt, homeUnit)
        : Promise.resolve<Row[]>([]);
    void personP.then((rows) => {
      if (alive) setPerson(rows.find((r) => num(r.person_id) === pid) ?? null);
    });

    return () => {
      alive = false;
    };
  }, [pid, homeUnit, transit, setJourney, peopleArt]);

  return (
    <div className="tp-journey">
      <button className="backbar" onClick={onBack}>
        ← All riders
      </button>
      <div className="tp-person">
        <div className="cap">person {pid}</div>
        <div className="ti">
          {person
            ? `${humanize(String(person.sex ?? "person"))} · age ${cell(person.age).text}`
            : `Person ${pid}`}
        </div>
        <div className="sb">
          {legs == null
            ? "reading journey…"
            : `${legs.length} ${legs.length === 1 ? "leg" : "legs"}`}
        </div>
      </div>

      {legs == null ? (
        <div className="col-empty pulse">range-reading chain…</div>
      ) : legs.length === 0 ? (
        <div className="col-empty">No journey legs recorded for this rider.</div>
      ) : (
        <ol className="tp-legs">
          {legs.map((l) => {
            const accent = ACCENT[l.mode] ?? ACCENT.train;
            return (
              <li key={l.legIdx} className="tp-leg">
                <span className="tp-leg-rail" style={{ background: accent }} />
                <span className="tp-leg-body">
                  <span className="tp-leg-line">{l.lineId}</span>
                  <span className="tp-leg-meta">
                    {humanize(l.mode)} · board {tmin(l.board)} → alight {tmin(l.alight)}{" "}
                    <span className="tp-leg-dur">({l.alight - l.board} min)</span>
                  </span>
                </span>
              </li>
            );
          })}
        </ol>
      )}
    </div>
  );
}
