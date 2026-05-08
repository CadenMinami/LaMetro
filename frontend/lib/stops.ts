'use client';

import { fetchStops } from './api';
import type { Stop, StopsResponse } from '@/types/stop';

// LA Metro publishes ~13k stops; the response is ~700 KB raw / ~150 KB
// gzipped. Cache it in sessionStorage keyed by feed_version so a refresh
// (within the same tab) avoids the API hop, but a feed rotation invalidates
// automatically.
const SS_KEY = 'la-metro:stops:v1';

interface CacheShape {
  version: string;
  stops: Stop[];
}

let inflight: Promise<Stop[]> | null = null;
let memoryCache: { version: string; stops: Stop[] } | null = null;

function readSessionCache(): CacheShape | null {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.sessionStorage.getItem(SS_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as CacheShape;
    if (!Array.isArray(parsed.stops) || typeof parsed.version !== 'string') return null;
    return parsed;
  } catch {
    return null;
  }
}

function writeSessionCache(payload: CacheShape) {
  if (typeof window === 'undefined') return;
  try {
    window.sessionStorage.setItem(SS_KEY, JSON.stringify(payload));
  } catch {
    // Quota exceeded or unavailable — fall back to memory cache only.
  }
}

/** Return the stops list, fetching once per session and caching the result. */
export async function getStops(): Promise<Stop[]> {
  if (memoryCache) return memoryCache.stops;
  const cached = readSessionCache();
  if (cached) {
    memoryCache = cached;
    return cached.stops;
  }
  if (inflight) return inflight;
  inflight = (async () => {
    try {
      const resp: StopsResponse | null = await fetchStops();
      if (!resp) return [];
      memoryCache = { version: resp.version, stops: resp.stops };
      writeSessionCache(memoryCache);
      return resp.stops;
    } finally {
      inflight = null;
    }
  })();
  return inflight;
}

/** Build a {stop_id → Stop} index. Useful when the map needs O(1) lookup. */
export function indexStops(stops: Stop[]): Map<string, Stop> {
  const idx = new Map<string, Stop>();
  for (const s of stops) idx.set(s.id, s);
  return idx;
}

/** Filter stops to those falling inside a lat/lon bbox. */
export function stopsInBBox(
  stops: Stop[],
  minLat: number, minLon: number, maxLat: number, maxLon: number,
): Stop[] {
  const out: Stop[] = [];
  for (const s of stops) {
    if (s.lat >= minLat && s.lat <= maxLat && s.lon >= minLon && s.lon <= maxLon) {
      out.push(s);
    }
  }
  return out;
}
