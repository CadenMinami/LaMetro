import type { Vehicle } from '@/types/vehicle';

// Fallback used when NEXT_PUBLIC_API_URL is unset or the API is unreachable.
// Lets the frontend render before Phase 2's query API ships. Replace with
// real data by setting NEXT_PUBLIC_API_URL in .env.local.
export const SAMPLE_VEHICLES: Vehicle[] = [
  { vehicle_id: 'sample-1', route_id: '720', lat: 34.0522, lon: -118.2437 },
  { vehicle_id: 'sample-2', route_id: '720', lat: 34.0610, lon: -118.2730 },
  { vehicle_id: 'sample-3', route_id: '733', lat: 34.0470, lon: -118.2580 },
  { vehicle_id: 'sample-4', route_id: '733', lat: 34.0410, lon: -118.2690 },
  { vehicle_id: 'sample-5', route_id: '2',   lat: 34.0700, lon: -118.2900 },
  { vehicle_id: 'sample-6', route_id: '2',   lat: 34.0800, lon: -118.3300 },
  { vehicle_id: 'sample-7', route_id: '4',   lat: 34.0900, lon: -118.3500 },
  { vehicle_id: 'sample-8', route_id: '4',   lat: 34.0810, lon: -118.3050 },
  { vehicle_id: 'sample-9', route_id: '910', lat: 34.0560, lon: -118.2350 },
];
