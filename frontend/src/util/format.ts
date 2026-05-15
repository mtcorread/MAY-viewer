export const nf = new Intl.NumberFormat("en-US");

export function compact(n: number): string {
  if (n == null || !isFinite(n)) return "—";
  const a = Math.abs(n);
  if (a >= 1e9) return (n / 1e9).toFixed(a >= 1e10 ? 0 : 1) + "B";
  if (a >= 1e6) return (n / 1e6).toFixed(a >= 1e7 ? 0 : 1) + "M";
  if (a >= 1e3) return (n / 1e3).toFixed(a >= 1e4 ? 0 : 1) + "k";
  return nf.format(n);
}

export function num(v: unknown): number {
  if (typeof v === "bigint") return Number(v);
  if (typeof v === "number") return v;
  const n = Number(v);
  return isFinite(n) ? n : 0;
}

export function pct(part: number, whole: number): string {
  if (!whole) return "0%";
  return ((100 * part) / whole).toFixed(part / whole >= 0.1 ? 0 : 1) + "%";
}

/** Render any Parquet scalar/list cell for display, distinguishing empty. */
export function cell(v: unknown): { text: string; empty: boolean } {
  if (v == null || v === "" ) return { text: "—", empty: true };
  if (typeof v === "bigint") return { text: v.toString(), empty: false };
  if (typeof v === "number")
    return Number.isInteger(v)
      ? { text: nf.format(v), empty: false }
      : { text: String(+v.toFixed(2)), empty: false };
  if (Array.isArray(v))
    return v.length ? { text: v.join(", "), empty: false } : { text: "—", empty: true };
  return { text: String(v), empty: false };
}
