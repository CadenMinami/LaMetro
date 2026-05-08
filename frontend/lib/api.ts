import type { Vehicle, VehiclesResponse } from '@/types/vehicle';
import { SAMPLE_VEHICLES } from './sample-vehicles';

export interface BBox {
  minLon: number;
  minLat: number;
  maxLon: number;
  maxLat: number;
}

export interface RouteAggregateWindow {
  window_start_iso: string;
  vehicle_count: number;
  avg_delay_seconds: number | null;
  p95_delay_seconds: number | null;
  on_time_pct: number | null;
  updated_at_iso?: string;
}

export interface RouteAggregatesResponse {
  route_id: string;
  count: number;
  windows: RouteAggregateWindow[];
}

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL;

function apiBase(): string | null {
  return API_BASE_URL ? API_BASE_URL.replace(/\/$/, '') : null;
}

export async function fetchVehicles(bbox: BBox, signal?: AbortSignal): Promise<Vehicle[]> {
  const base = apiBase();
  if (!base) return SAMPLE_VEHICLES;

  const qs = `bbox=${bbox.minLon},${bbox.minLat},${bbox.maxLon},${bbox.maxLat}`;
  const url = `${base}/vehicles?${qs}`;
  try {
    const res = await fetch(url, { signal, cache: 'no-store' });
    if (!res.ok) throw new Error(`API ${res.status}`);
    const body = (await res.json()) as VehiclesResponse | Vehicle[];
    return Array.isArray(body) ? body : (body.vehicles ?? []);
  } catch (err) {
    if ((err as Error).name === 'AbortError') throw err;
    console.warn('fetchVehicles failed, using sample:', err);
    return SAMPLE_VEHICLES;
  }
}

export async function fetchRouteAggregates(
  routeId: string,
  signal?: AbortSignal,
): Promise<RouteAggregateWindow[]> {
  const base = apiBase();
  if (!base) return [];
  const url = `${base}/routes/${encodeURIComponent(routeId)}/aggregates`;
  const res = await fetch(url, { signal, cache: 'no-store' });
  if (!res.ok) throw new Error(`API ${res.status}`);
  const body = (await res.json()) as RouteAggregatesResponse;
  return body.windows ?? [];
}
