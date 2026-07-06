/** Shared display formatters — keep number conventions identical across every page. */

/** Batting-rate display: .554 for sub-1 values, 1.284 for OPS-style values, — for missing. */
export const rate = (v: number | null | undefined): string => {
  if (v == null || Number.isNaN(Number(v))) return '—';
  const s = Number(v).toFixed(3);
  return Number(v) >= 1 ? s : s.replace(/^0\./, '.');
};

/** Whole-number counting stat. */
export const int = (v: unknown): string =>
  v == null || v === '' ? '—' : String(Math.round(Number(v)));

/** Game Score, one decimal. */
export const gs = (v: number | null | undefined): string =>
  v == null || Number.isNaN(Number(v)) ? '—' : Number(v).toFixed(1);

/** Games back: dash when level with the leader. */
export const gb = (v: number | null | undefined): string => {
  const n = Number(v ?? 0);
  return n <= 0 ? '—' : String(n % 1 === 0 ? n : n.toFixed(1));
};

/** Signed run differential. */
export const diff = (v: number | null | undefined): string => {
  const n = Math.round(Number(v ?? 0));
  return n > 0 ? `+${n}` : String(n);
};

/** 2026-07-01 -> Jul 1, 2026 */
export const datePretty = (iso: string): string =>
  new Date(`${iso}T12:00:00`).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });

/** 2026-07-01 -> Jul 1 '26 */
export const dateShort = (iso: string): string => {
  const d = new Date(`${iso}T12:00:00`);
  const md = d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  return `${md} '${String(d.getFullYear()).slice(2)}`;
};
