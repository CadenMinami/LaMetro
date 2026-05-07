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
