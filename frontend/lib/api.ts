import type { Vehicle, VehiclesResponse } from '@/types/vehicle';
import { SAMPLE_VEHICLES } from './sample-vehicles';

export interface BBox {
  minLon: number;
  minLat: number;
  maxLon: number;
  maxLat: number;
}

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL;

export async function fetchVehicles(bbox: BBox, signal?: AbortSignal): Promise<Vehicle[]> {
  if (!API_BASE_URL) {
    return SAMPLE_VEHICLES;
  }

  const qs = `bbox=${bbox.minLon},${bbox.minLat},${bbox.maxLon},${bbox.maxLat}`;
  const url = `${API_BASE_URL.replace(/\/$/, '')}/vehicles?${qs}`;

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
