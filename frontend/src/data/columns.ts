// Turn the self-describing aggregate column names into labelled metric
// groups. Every label here derives from the column string or the manifest
// schema — there are no hardcoded domain terms (non-negotiable #2).

export const FIXED_COLS = new Set([
  "geo_id",
  "geo_name",
  "level",
  "level_name",
  "parent_id",
]);

export interface MetricItem {
  col: string;
  label: string; // human-readable, derived from the raw token
  raw: string; // the exact manifest/category token
}

export interface MetricGroup {
  kind: "age" | "sex" | "venues" | "prop";
  key: string; // e.g. "age", "sex", venue type, or person property name
  label: string;
  items: MetricItem[];
  // venues groups also carry the paired occupancy column per item
  occ?: Record<string, string>;
}

/** Humanise a raw token without inventing meaning: snake/again → spaced. */
export function humanize(token: string): string {
  if (token === "(none)" || token === "") return "—";
  return token
    .replace(/[_]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

export function parseAggregateColumns(columns: string[]): {
  groups: MetricGroup[];
  hasPeople: boolean;
  hasMeanAge: boolean;
} {
  const age: MetricItem[] = [];
  const sex: MetricItem[] = [];
  const venueTypes = new Map<string, MetricItem>();
  const occByType: Record<string, string> = {};
  const props = new Map<string, MetricItem[]>();
  let hasPeople = false;
  let hasMeanAge = false;

  for (const c of columns) {
    if (c === "people") hasPeople = true;
    else if (c === "mean_age") hasMeanAge = true;
    else if (c.startsWith("age:"))
      age.push({ col: c, raw: c.slice(4), label: c.slice(4).replace(/_/g, "–").replace("plus", "+") });
    else if (c.startsWith("sex:"))
      sex.push({ col: c, raw: c.slice(4), label: humanize(c.slice(4)) });
    else if (c.startsWith("venues:")) {
      const t = c.slice(7);
      venueTypes.set(t, { col: c, raw: t, label: humanize(t) });
    } else if (c.startsWith("occ:")) {
      occByType[c.slice(4)] = c;
    } else if (c.startsWith("p:")) {
      const eq = c.indexOf("=");
      if (eq > 2) {
        const prop = c.slice(2, eq);
        const cat = c.slice(eq + 1);
        if (!props.has(prop)) props.set(prop, []);
        props.get(prop)!.push({ col: c, raw: cat, label: humanize(cat) });
      }
    }
  }

  const groups: MetricGroup[] = [];
  if (age.length)
    groups.push({ kind: "age", key: "age", label: "Age", items: age });
  if (sex.length)
    groups.push({ kind: "sex", key: "sex", label: "Sex", items: sex });
  for (const [prop, items] of props)
    groups.push({ kind: "prop", key: prop, label: humanize(prop), items });
  if (venueTypes.size) {
    groups.push({
      kind: "venues",
      key: "venues",
      label: "Venues",
      items: [...venueTypes.values()].sort((a, b) => a.label.localeCompare(b.label)),
      occ: occByType,
    });
  }
  return { groups, hasPeople, hasMeanAge };
}
