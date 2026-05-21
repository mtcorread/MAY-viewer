// Optional user-supplied display labels for raw attribute codes. The file
// lives next to manifest.json in the world's cache directory (so it ships
// alongside the prep output and is served at /cache/labels.json). Absent
// or malformed → no labels applied; the UI falls back to raw codes.
//
// Shape: { [field: string]: { [rawValue: string]: "Display Label" } }

export type Labels = Record<string, Record<string, string>>;

const EMPTY: Labels = {};

export async function loadLabels(): Promise<Labels> {
  try {
    const r = await fetch("/cache/labels.json");
    if (!r.ok) return EMPTY;
    const j = await r.json();
    if (!j || typeof j !== "object" || Array.isArray(j)) return EMPTY;
    const out: Labels = {};
    for (const [field, map] of Object.entries(j)) {
      if (!map || typeof map !== "object" || Array.isArray(map)) continue;
      const m: Record<string, string> = {};
      for (const [k, v] of Object.entries(map as Record<string, unknown>)) {
        if (typeof v === "string" && v.length > 0) m[k] = v;
      }
      if (Object.keys(m).length > 0) out[field] = m;
    }
    return out;
  } catch {
    return EMPTY;
  }
}

/** Map a raw attribute value to its display label, falling back to the
 *  original value. For arrays, each element is mapped individually. */
export function relabel(field: string, value: unknown, labels: Labels): unknown {
  const m = labels[field];
  if (!m) return value;
  if (typeof value === "string") return m[value] ?? value;
  if (Array.isArray(value))
    return value.map((x) => (typeof x === "string" ? m[x] ?? x : x));
  return value;
}
