import { useEffect, useMemo, useState } from "react";
import { useStore } from "../state/store";
import { readGeoUnit } from "../data/parquet";
import { humanize } from "../data/columns";
import { cell, nf, num } from "../util/format";

// One bounded row-group read per selected geo unit (manifest row_groups
// index). People are partitioned at the leaf; venues/members at their own
// partition level — we surface whatever the manifest exposes for the unit.

type Row = Record<string, unknown>;
const CAP = 400;

export function Inspector() {
  const { manifest, selected, inspectVenueId, inspectPersonId, inspectVenue, inspectPerson } =
    useStore();
  const dd = manifest!.artifacts.drilldown;
  const [tab, setTab] = useState<"venues" | "people">("venues");

  const [venues, setVenues] = useState<Row[] | null>(null);
  const [people, setPeople] = useState<Row[] | null>(null);
  const [members, setMembers] = useState<Row[] | null>(null);
  const [busy, setBusy] = useState(false);

  const gid = selected?.geo_id;
  const hasVenues = gid != null && dd.venues.row_groups[String(gid)] !== undefined;
  const hasPeople = gid != null && dd.people.row_groups[String(gid)] !== undefined;

  // Load the unit's row groups when the selection changes.
  useEffect(() => {
    let alive = true;
    setVenues(null);
    setPeople(null);
    setMembers(null);
    inspectVenue(null);
    if (gid == null) return;
    setBusy(true);
    Promise.all([
      hasVenues ? readGeoUnit(dd.venues, gid) : Promise.resolve<Row[]>([]),
      hasPeople ? readGeoUnit(dd.people, gid) : Promise.resolve<Row[]>([]),
      hasVenues ? readGeoUnit(dd.members, gid) : Promise.resolve<Row[]>([]),
    ]).then(([v, p, m]) => {
      if (!alive) return;
      setVenues(v);
      setPeople(p);
      setMembers(m);
      setBusy(false);
      setTab(hasVenues ? "venues" : "people");
    });
    return () => {
      alive = false;
    };
  }, [gid, hasVenues, hasPeople, dd, inspectVenue]);

  const peopleById = useMemo(() => {
    const m = new Map<number, Row>();
    for (const r of people ?? []) m.set(num(r.person_id), r);
    return m;
  }, [people]);

  if (!selected) return <div className="insp-empty">Select a geo unit in the left rail to inspect its members.</div>;

  if (inspectPersonId != null) {
    return (
      <PersonDetail
        person={peopleById.get(inspectPersonId) ?? null}
        id={inspectPersonId}
        peopleById={peopleById}
        onBack={() => inspectPerson(null)}
        onOpen={(pid) => inspectPerson(pid)}
      />
    );
  }

  if (inspectVenueId != null) {
    const v = (venues ?? []).find((r) => num(r.venue_id) === inspectVenueId) ?? null;
    return (
      <VenueDetail
        venue={v}
        members={(members ?? []).filter((m) => num(m.venue_id) === inspectVenueId)}
        peopleById={peopleById}
        onBack={() => inspectVenue(null)}
        onPerson={(pid) => inspectPerson(pid)}
      />
    );
  }

  return (
    <div className="fade-in" key={gid}>
      <div className="section" style={{ borderBottom: 0, paddingBottom: 12 }}>
        <div className="eyebrow" style={{ marginBottom: 8 }}>
          {selected.level_name} · {selected.geo_name}
        </div>
        <h3 style={{ fontSize: 15 }}>Inspect</h3>
      </div>

      <div className="tabs">
        <button
          className={tab === "venues" ? "on" : ""}
          disabled={!hasVenues}
          onClick={() => setTab("venues")}
        >
          Venues {venues ? `· ${nf.format(venues.length)}` : ""}
        </button>
        <button
          className={tab === "people" ? "on" : ""}
          disabled={!hasPeople}
          onClick={() => setTab("people")}
        >
          People {people ? `· ${nf.format(people.length)}` : ""}
        </button>
      </div>

      {busy && <div className="loading pulse">range-reading row group…</div>}

      {!busy && tab === "venues" &&
        (!hasVenues ? (
          <div className="insp-empty">
            No venues are partitioned at this unit. Drill to a finer unit in the
            left rail.
          </div>
        ) : (
          <VenueList rows={venues ?? []} onOpen={(id) => inspectVenue(id)} />
        ))}

      {!busy && tab === "people" &&
        (!hasPeople ? (
          <div className="insp-empty">
            People are partitioned at the leaf level. Drill down to a leaf unit
            to open individuals.
          </div>
        ) : (
          <PeopleList rows={people ?? []} onOpen={(id) => inspectPerson(id)} />
        ))}
    </div>
  );
}

function VenueList({ rows, onOpen }: { rows: Row[]; onOpen: (id: number) => void }) {
  const shown = rows.slice(0, CAP);
  return (
    <div>
      {shown.map((r) => (
        <button key={num(r.venue_id)} className="row-item" onClick={() => onOpen(num(r.venue_id))}>
          <span className="tag">{String(r.type ?? "venue")}</span>
          <span className="main">
            <div className="t">{String(r.name ?? `#${num(r.venue_id)}`)}</div>
            <div className="s">id {num(r.venue_id)}</div>
          </span>
          <span className="chev">›</span>
        </button>
      ))}
      {rows.length > CAP && (
        <div className="loading">+{nf.format(rows.length - CAP)} more in this unit</div>
      )}
    </div>
  );
}

function PeopleList({ rows, onOpen }: { rows: Row[]; onOpen: (id: number) => void }) {
  const shown = rows.slice(0, CAP);
  return (
    <div>
      {shown.map((r) => (
        <button key={num(r.person_id)} className="row-item" onClick={() => onOpen(num(r.person_id))}>
          <span className="tag">person</span>
          <span className="main">
            <div className="t">
              {humanize(String(r.sex ?? ""))} · age {cell(r.age).text}
            </div>
            <div className="s">id {num(r.person_id)}</div>
          </span>
          <span className="chev">›</span>
        </button>
      ))}
      {rows.length > CAP && (
        <div className="loading">+{nf.format(rows.length - CAP)} more in this unit</div>
      )}
    </div>
  );
}

const HIDE = new Set(["person_id", "geo_unit_id", "venue_id"]);

function AttrRows({ row }: { row: Row }) {
  return (
    <div className="attr-grid">
      {Object.entries(row)
        .filter(([k]) => !HIDE.has(k))
        .map(([k, v]) => {
          const c = cell(v);
          return (
            <div className="attr" key={k}>
              <div className="k">{humanize(k)}</div>
              <div className={"v" + (c.empty ? " empty" : "")}>{c.text}</div>
            </div>
          );
        })}
    </div>
  );
}

function VenueDetail({
  venue,
  members,
  peopleById,
  onBack,
  onPerson,
}: {
  venue: Row | null;
  members: Row[];
  peopleById: Map<number, Row>;
  onBack: () => void;
  onPerson: (id: number) => void;
}) {
  const groups = useMemo(() => {
    const g = new Map<string, number[]>();
    for (const m of members) {
      const s = String(m.subset ?? "—");
      if (!g.has(s)) g.set(s, []);
      g.get(s)!.push(num(m.person_id));
    }
    return [...g.entries()].sort((a, b) => b[1].length - a[1].length);
  }, [members]);

  if (!venue) return <div className="insp-empty">venue not in the loaded row group</div>;

  // Drop all-null per-type property columns so only this venue's apply.
  const trimmed: Row = {};
  for (const [k, v] of Object.entries(venue))
    if (!(v == null && k.includes("."))) trimmed[k] = v;

  return (
    <div className="fade-in">
      <div className="detail-h">
        <button className="back" onClick={onBack}>‹ back to list</button>
        <h2>{String(venue.name ?? `Venue ${num(venue.venue_id)}`)}</h2>
        <div className="sub">
          {String(venue.type ?? "")} · venue {num(venue.venue_id)} ·{" "}
          {nf.format(members.length)} members
        </div>
      </div>

      <AttrRows row={trimmed} />

      {groups.map(([subset, ids]) => (
        <div className="roster-grp" key={subset}>
          <div className="gh">
            <span className="t">{humanize(subset)}</span>
            <span className="c mono">{nf.format(ids.length)}</span>
          </div>
          <div className="chips">
            {ids.slice(0, 120).map((pid) => (
              <span
                key={pid}
                className={"chip" + (peopleById.has(pid) ? " link" : " ext")}
                onClick={peopleById.has(pid) ? () => onPerson(pid) : undefined}
                title={peopleById.has(pid) ? "open person" : "in another unit"}
              >
                {pid}
              </span>
            ))}
            {ids.length > 120 && <span className="chip ext">+{nf.format(ids.length - 120)}</span>}
          </div>
        </div>
      ))}
    </div>
  );
}

function PersonDetail({
  person,
  id,
  peopleById,
  onBack,
  onOpen,
}: {
  person: Row | null;
  id: number;
  peopleById: Map<number, Row>;
  onBack: () => void;
  onOpen: (id: number) => void;
}) {
  if (!person)
    return (
      <div className="fade-in">
        <div className="detail-h">
          <button className="back" onClick={onBack}>‹ back</button>
          <h2>Person {id}</h2>
          <div className="sub">not in the loaded unit — open their geo unit to resolve</div>
        </div>
      </div>
    );

  // Split list-valued relations (friendships etc.) from scalar attributes so
  // the social graph renders as navigable chips.
  const scalars: Row = {};
  const lists: [string, number[]][] = [];
  for (const [k, v] of Object.entries(person)) {
    if (k === "person_id" || k === "geo_unit_id") continue;
    if (Array.isArray(v) && v.every((x) => typeof x === "number" || typeof x === "bigint")) {
      lists.push([k, (v as unknown[]).map((x) => Number(x))]);
    } else {
      scalars[k] = v;
    }
  }

  return (
    <div className="fade-in">
      <div className="detail-h">
        <button className="back" onClick={onBack}>‹ back</button>
        <h2>
          {humanize(String(person.sex ?? "person"))} · age {cell(person.age).text}
        </h2>
        <div className="sub">person {num(person.person_id)}</div>
      </div>

      <AttrRows row={scalars} />

      {lists.map(([k, ids]) => (
        <div className="roster-grp" key={k}>
          <div className="gh">
            <span className="t">{humanize(k)}</span>
            <span className="c mono">{nf.format(ids.length)}</span>
          </div>
          {ids.length === 0 ? (
            <div className="loading" style={{ padding: 0 }}>none</div>
          ) : (
            <div className="chips">
              {ids.slice(0, 200).map((pid, i) => {
                const here = peopleById.has(pid);
                return (
                  <span
                    key={`${pid}-${i}`}
                    className={"chip" + (here ? " link" : " ext")}
                    onClick={here ? () => onOpen(pid) : undefined}
                    title={here ? "open" : "in another unit (id only)"}
                  >
                    {pid}
                  </span>
                );
              })}
              {ids.length > 200 && <span className="chip ext">+{nf.format(ids.length - 200)}</span>}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
