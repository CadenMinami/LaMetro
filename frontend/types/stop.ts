export interface Stop {
  id: string;
  name: string;
  lat: number;
  lon: number;
  routes: string[];
}

export interface StopsResponse {
  version: string;
  count: number;
  stops: Stop[];
}

export type ArrivalStatus = 'live' | 'scheduled' | 'due' | 'departed';

export interface Arrival {
  route_id: string;
  trip_id: string;
  scheduled_arrival: string;  // ISO-8601 UTC
  predicted_arrival: string;  // ISO-8601 UTC
  predicted_minutes: number;
  delay_seconds: number | null;
  status: ArrivalStatus;
  vehicle_id: string | null;
  stop_sequence: number;
}

export interface StopArrivalsResponse {
  stop_id: string;
  stop_name: string;
  as_of: string;             // ISO-8601 UTC
  horizon_minutes: number;
  arrivals: Arrival[];
}
