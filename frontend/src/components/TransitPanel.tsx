import { useEffect, useMemo, useState } from "react";
import { useStore } from "../state/store";
import type { TransitSel } from "../state/store";
import { TransitArtifact } from "../data/manifest";
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
// highlights those legs. Pin two or more lines to compare → the panel shows the
// riders shared across every pinned line (rider-set intersection). Reads are
// bounded row-group fetches — never whole files.

type Row = Record<string, unknown>;
const PAGE = 80; // riders per page

// Per-mode accent (matches the map line colors); shared-ridership uses its own.
const ACCENT: Record<string, string> = { train: "#e8743b", tube: "#2f6fed" };
const SHARED_ACCENT = "#7a4fd0";

interface Leg {
  legIdx: number;
  board: number;
  alight: number;
  venueId: number;
  lineId: string;
  mode: string;
  // Geo units recorded by worlds that persist journey O/D (-1 when the cache
  // predates those fields or a stop lies outside the world).
  originUnit: number;
  destUnit: number;
  boardUnit: number;
  alightUnit: number;
}

// Unit-id cell: -1 when the column is absent (old cache) — num() alone would
// turn `undefined` into 0, which can be a real geo id.
function uid(v: unknown): number {
  return v == null ? -1 : num(v);
}

// Board/alight are minutes-from-day-start in the simulation's commute window
// (small values like 16, 23, 70 — not clock time), so show the raw minute mark.
function tmin(min: number): string {
  return Number.isFinite(min) ? `${min}′` : "—";
}

// Riders shared across every set (rider-set intersection). One set → itself, so
// the same hook serves a single clicked line and a multi-line comparison.
function intersectRiders(sets: Row[][]): Row[] {
  if (!sets.length) return [];
  if (sets.length === 1) return sets[0];
  const base = new Map<number, Row>();
  for (const r of sets[0]) base.set(num(r.person_id), r);
  for (let i = 1; i < sets.length && base.size; i++) {
    const ids = new Set(sets[i].map((r) => num(r.person_id)));
    for (const pid of [...base.keys()]) if (!ids.has(pid)) base.delete(pid);
  }
  return [...base.values()];
}

// Load (and intersect) the rider sets for the given line venues. Each set is one
// bounded row-group read; `null` while any read is in flight.
function useRiderRows(transit: TransitArtifact, venueIds: number[]): Row[] | null {
  const [rows, setRows] = useState<Row[] | null>(null);
  const key = venueIds.join(",");
  useEffect(() => {
    setRows(null);
    if (!venueIds.length) {
      setRows([]);
      return;
    }
    let alive = true;
    void Promise.all(
      venueIds.map((vid) => {
        const rg = transit.riders.row_groups[String(vid)];
        return rg === undefined
          ? Promise.resolve<Row[]>([])
          : readRowGroup(transit.riders.path, rg);
      }),
    ).then((sets) => {
      if (alive) setRows(intersectRiders(sets));
    });
    return () => {
      alive = false;
    };
    // key captures the venue set; transit is stable for a loaded manifest.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, transit]);
  return rows;
}

export function TransitPanel() {
  const {
    manifest,
    transitLine,
    transitRider,
    selectTransitRider,
    setTransitJourney,
    transitCompare,
    pinTransitLine,
    unpinTransitLine,
    clearTransitCompare,
    gotoPerson,
  } = useStore();
  const transit = manifest!.artifacts.transit!;
  const dd = manifest!.artifacts.drilldown;

  // Active (clicked) line riders, and — when 2+ lines are pinned — the riders
  // shared across the pinned set. Both hooks run every render (hook rules).
  const activeRiders = useRiderRows(transit, transitLine ? [transitLine.venueId] : []);
  const compareVenueIds = useMemo(
    () => transitCompare.map((l) => l.venueId).slice().sort((a, b) => a - b),
    [transitCompare],
  );
  const sharedRiders = useRiderRows(
    transit,
    compareVenueIds.length >= 2 ? compareVenueIds : [],
  );

  // home geo unit per rider (carried on the riders row) — used to index the
  // chains shard and resolve a rider's attribute row, whichever list they came
  // from.
  const homeByPid = useMemo(() => {
    const m = new Map<number, number>();
    for (const r of [...(activeRiders ?? []), ...(sharedRiders ?? [])])
      m.set(num(r.person_id), num(r.home_geo_unit));
    return m;
  }, [activeRiders, sharedRiders]);

  if (!transitLine && transitCompare.length === 0)
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

  // A selected rider's journey takes over the panel, regardless of which list
  // they were picked from.
  if (transitRider != null)
    return (
      <div className="stats fade-in" key={transitRider}>
        <RiderJourney
          pid={transitRider}
          homeUnit={homeByPid.get(transitRider) ?? -1}
          onBack={() => selectTransitRider(null)}
          // Jump to the rider's full profile in Inspect. gotoPerson snapshots
          // this map+transit spot on the back-stack, and the transit selection
          // itself is left untouched — the Inspector's "← Back" returns here
          // with the lines, pins, and this rider's journey still up.
          onProfile={() => void gotoPerson(transitRider, homeByPid.get(transitRider))}
          setJourney={setTransitJourney}
          peopleArt={dd.people}
        />
      </div>
    );

  const isPinned =
    !!transitLine && transitCompare.some((l) => l.venueId === transitLine.venueId);

  return (
    <div className="stats fade-in">
      {/* The comparison is the standing result, so it sits on top — pinning the
          second line surfaces it immediately, with no further clicks. The active
          (clicked) line follows below for continued exploration. */}
      {transitCompare.length > 0 && (
        <CompareTray
          lines={transitCompare}
          shared={compareVenueIds.length >= 2 ? sharedRiders : null}
          // When an unpinned candidate line is expanded below, cap the shared
          // list so the candidate (and its pin button) stays reachable without a
          // long scroll. When it's the only list, let it fill the column.
          capShared={!!transitLine && !isPinned}
          onUnpin={unpinTransitLine}
          onClear={clearTransitCompare}
          onPick={selectTransitRider}
        />
      )}
      {transitLine && (
        <ActiveLine
          line={transitLine}
          riders={activeRiders}
          pinned={isPinned}
          // A pinned line's individual riders add nothing the comparison doesn't
          // already show, so collapse them; an unpinned line stays expanded so
          // its riders can be evaluated before pinning.
          collapsed={isPinned && transitCompare.length > 0}
          onPin={() => pinTransitLine(transitLine)}
          onUnpin={() => unpinTransitLine(transitLine.venueId)}
          onPick={selectTransitRider}
        />
      )}
    </div>
  );
}

// The clicked line: header, a pin/unpin toggle, and (unless collapsed) its
// paginated rider list. Collapsed once the line is pinned — the comparison above
// already covers its riders, so only the header + unpin toggle remain.
function ActiveLine({
  line,
  riders,
  pinned,
  collapsed,
  onPin,
  onUnpin,
  onPick,
}: {
  line: TransitSel;
  riders: Row[] | null;
  pinned: boolean;
  collapsed: boolean;
  onPin: () => void;
  onUnpin: () => void;
  onPick: (pid: number) => void;
}) {
  const accent = ACCENT[line.mode] ?? ACCENT.train;
  return (
    <div key={line.venueId} className={collapsed ? "tp-active sep" : "tp-active"}>
      <div className="hero-cap" style={{ color: accent }}>
        {humanize(line.mode)} line
      </div>
      <div className="ti tp-title">{line.lineId}</div>
      <div className="hero-sub">
        <b>{nf.format(line.riderCount)}</b> riders
        <span className="dot">·</span>venue {line.venueId}
      </div>
      <button
        className={`tp-pin${pinned ? " on" : ""}`}
        onClick={pinned ? onUnpin : onPin}
      >
        {pinned ? "✓ Pinned — unpin" : "+ Pin to compare"}
      </button>

      {!collapsed && (
        <>
          <div className="dsub" style={{ paddingLeft: 0, paddingRight: 0 }}>
            <span className="t">Riders</span>
          </div>
          <RiderList
            key={line.venueId}
            rows={riders}
            accent={accent}
            onPick={onPick}
            emptyText="No riders recorded for this line."
          />
        </>
      )}
    </div>
  );
}

// The compare tray: pinned-line chips + clear, and (when 2+ pinned) the riders
// shared across every pinned line.
function CompareTray({
  lines,
  shared,
  capShared,
  onUnpin,
  onClear,
  onPick,
}: {
  lines: TransitSel[];
  shared: Row[] | null;
  capShared: boolean;
  onUnpin: (venueId: number) => void;
  onClear: () => void;
  onPick: (pid: number) => void;
}) {
  return (
    <div className="tp-compare">
      <div className="dsub" style={{ paddingLeft: 0, paddingRight: 0 }}>
        <span className="t">Compare</span>
        <button className="tp-clear" onClick={onClear}>
          clear
        </button>
      </div>
      <div className="tp-chips">
        {lines.map((l) => (
          <span className="tp-chip" key={l.venueId} title={l.lineId}>
            <span className="tp-dot" style={{ background: ACCENT[l.mode] ?? ACCENT.train }} />
            <span className="tp-chip-label">{l.lineId}</span>
            <button
              className="tp-chip-x"
              onClick={() => onUnpin(l.venueId)}
              aria-label={`Remove ${l.lineId}`}
            >
              ×
            </button>
          </span>
        ))}
      </div>

      {lines.length < 2 ? (
        <div className="tp-legend-note">Pin another line to see shared riders.</div>
      ) : (
        <>
          <div className="dsub" style={{ paddingLeft: 0, paddingRight: 0 }}>
            <span className="t" style={{ color: SHARED_ACCENT }}>
              Shared riders
            </span>
            <span className="m">
              {shared == null ? "reading…" : `${nf.format(shared.length)} on all ${lines.length}`}
            </span>
          </div>
          <RiderList
            key={lines.map((l) => l.venueId).join(",")}
            rows={shared}
            accent={SHARED_ACCENT}
            capped={capShared}
            onPick={onPick}
            emptyText="No riders ride all of these lines."
          />
        </>
      )}
    </div>
  );
}

// A paginated, clickable rider list. Page state resets when remounted (parents
// pass a `key` tied to the rider source).
function RiderList({
  rows,
  accent,
  onPick,
  emptyText,
  capped = false,
}: {
  rows: Row[] | null;
  accent: string;
  onPick: (pid: number) => void;
  emptyText: string;
  capped?: boolean;
}) {
  const [page, setPage] = useState(0);
  if (rows == null) return <div className="col-empty pulse">range-reading row group…</div>;
  if (rows.length === 0) return <div className="col-empty">{emptyText}</div>;

  const pageCount = Math.max(1, Math.ceil(rows.length / PAGE));
  const clamped = Math.min(page, pageCount - 1);
  const slice = rows.slice(clamped * PAGE, clamped * PAGE + PAGE);

  return (
    <div className={capped ? "tp-riders capped" : "tp-riders"}>
      {slice.map((r) => {
        const pid = num(r.person_id);
        return (
          <button key={pid} className="tp-rider" onClick={() => onPick(pid)}>
            <span className="tp-dot" style={{ background: accent }} />
            <span className="tp-pid">{pid}</span>
            <span className="tp-go">view journey ›</span>
          </button>
        );
      })}
      {pageCount > 1 && (
        <div className="pager">
          <button className="pgbtn" disabled={clamped === 0} onClick={() => setPage(clamped - 1)}>
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
  );
}

// A selected rider's ordered journey: fetch their legs (chains shard, keyed by
// home unit), push the leg venue_ids to the map highlight, and resolve the
// rider's own attribute row for a small header.
function RiderJourney({
  pid,
  homeUnit,
  onBack,
  onProfile,
  setJourney,
  peopleArt,
}: {
  pid: number;
  homeUnit: number;
  onBack: () => void;
  onProfile: () => void;
  setJourney: (venueIds: number[], odUnits?: number[]) => void;
  peopleArt: import("../data/manifest").DrilldownArtifact;
}) {
  const transit = useStore((s) => s.manifest!.artifacts.transit!);
  const findUnit = useStore((s) => s.findUnit);
  const [legs, setLegs] = useState<Leg[] | null>(null);
  const [person, setPerson] = useState<Row | null>(null);
  const [od, setOd] = useState<[string, string] | null>(null);

  // Resolve the journey's recorded endpoints to unit names (worlds that
  // persist O/D only; old caches leave this line out).
  useEffect(() => {
    setOd(null);
    const first = legs?.[0];
    if (!first || first.originUnit < 0 || first.destUnit < 0) return;
    let alive = true;
    void Promise.all([findUnit(first.originUnit), findUnit(first.destUnit)]).then(
      ([o, d]) => {
        if (alive)
          setOd([
            o?.geo_name ?? `unit ${first.originUnit}`,
            d?.geo_name ?? `unit ${first.destUnit}`,
          ]);
      },
    );
    return () => {
      alive = false;
    };
  }, [legs, findUnit]);

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
          originUnit: uid(r.origin_unit_id),
          destUnit: uid(r.dest_unit_id),
          boardUnit: uid(r.board_unit_id),
          alightUnit: uid(r.alight_unit_id),
        }))
        .sort((a, b) => a.legIdx - b.legIdx);
      setLegs(mine);
      // The geo-unit sequence travelled: origin, each leg's board/alight
      // (consecutive duplicates collapse — leg N's alight is leg N+1's board),
      // destination. Empty for old caches; the map draws nothing then.
      const seq: number[] = [];
      const push = (u: number) => {
        if (u >= 0 && seq[seq.length - 1] !== u) seq.push(u);
      };
      mine.forEach((l, i) => {
        if (i === 0) push(l.originUnit);
        push(l.boardUnit);
        push(l.alightUnit);
      });
      if (mine.length) push(mine[mine.length - 1].destUnit);
      setJourney(mine.map((l) => l.venueId), seq);
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
        ← Back
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
        {od && (
          <div className="tp-od">
            <span className="tp-od-dot start" />
            {od[0]}
            <span className="tp-od-arrow">→</span>
            <span className="tp-od-dot end" />
            {od[1]}
          </div>
        )}
        <button className="tp-profile" onClick={onProfile}>
          Open profile in Inspect ›
        </button>
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
