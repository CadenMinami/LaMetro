// Deterministic color per route_id. Avoids maintaining a static palette and
// keeps colors stable across reloads. FNV-1a 32-bit → hue in [0, 360).
export function routeColor(routeId: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < routeId.length; i++) {
    hash ^= routeId.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  const hue = Math.abs(hash) % 360;
  return `hsl(${hue}, 70%, 55%)`;
}

// Phase 4d: color a vehicle pin by its schedule deviation. Buckets match
// the spec — green <1m, yellow 1-3m, orange 3-5m, red >5m. Negative delays
// (vehicle is early) are folded into the same buckets by absolute value,
// since both ends of "out of tolerance" are equally interesting visually.
//
// Returns null when the delay is unknown (deadhead, off-route, pre/post
// trip), so the caller can fall back to a neutral "no data" rendering.
export function delayColor(delaySeconds: number | null | undefined): string | null {
  if (delaySeconds === null || delaySeconds === undefined) return null;
  const abs = Math.abs(delaySeconds);
  if (abs < 60) return '#22c55e';   // green-500
  if (abs < 180) return '#eab308';  // yellow-500
  if (abs < 300) return '#f97316';  // orange-500
  return '#ef4444';                 // red-500
}

// Friendly label for a delay value — used in popups and the route page.
export function delayLabel(delaySeconds: number | null | undefined): string {
  if (delaySeconds === null || delaySeconds === undefined) return '—';
  const abs = Math.abs(delaySeconds);
  const mins = Math.floor(abs / 60);
  const secs = abs % 60;
  const human = mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
  if (delaySeconds > 30) return `${human} late`;
  if (delaySeconds < -30) return `${human} early`;
  return 'on time';
}

// Convert a compass heading in degrees to a 16-point compass label (N, NNE, NE, ...).
// Falls back to "—" when the bearing is missing.
export function bearingCompass(bearing: number | null | undefined): string {
  if (bearing === null || bearing === undefined || Number.isNaN(bearing)) return '—';
  const points = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
                  'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'];
  const idx = Math.round(((bearing % 360) + 360) % 360 / 22.5) % 16;
  return `${points[idx]} ${Math.round(bearing)}°`;
}

// "Xs ago" / "Xm ago" relative time. Used in vehicle popups so the user
// knows how fresh the data is. Computed at popup-open time, not live.
export function timeAgo(iso: string | null | undefined, now: number = Date.now()): string {
  if (!iso) return '—';
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return '—';
  const sec = Math.max(0, Math.round((now - t) / 1000));
  if (sec < 5) return 'just now';
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ${sec % 60}s ago`;
  const hr = Math.floor(min / 60);
  return `${hr}h ${min % 60}m ago`;
}
