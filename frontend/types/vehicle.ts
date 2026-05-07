export interface Vehicle {
  vehicle_id: string;
  route_id: string;
  trip_id?: string;
  lat: number;
  lon: number;
  bearing?: number;
  speed_mps?: number;
  delay_seconds?: number | null;
  last_updated?: string;
}

export interface VehiclesResponse {
  count: number;
  as_of: string;
  vehicles: Vehicle[];
}
