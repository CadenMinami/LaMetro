# LA Metro Reliability — API Contract

This document is the source of truth for the REST API the frontend (Phase 3) consumes from the backend (Phase 2). Use it to build against a stable shape even before the backend is deployed.

## Base URL

- **Dev:** TBD — will be an API Gateway URL like `https://abc123.execute-api.us-west-2.amazonaws.com/prod`. Will be filled in after `cdk deploy ApiStack` and exported as `NEXT_PUBLIC_API_BASE_URL`.
- **Prod:** TBD (custom domain, Phase 9).

## Endpoints

### `GET /vehicles`

Returns the live snapshot of LA Metro vehicles inside a bounding box.

**Query parameters (required):**

| Param   | Type   | Description                                           |
|---------|--------|-------------------------------------------------------|
| `bbox`  | string | `lon_min,lat_min,lon_max,lat_max` — comma-separated.  |

**Optional query parameters:**

| Param      | Type   | Default | Description                                        |
|------------|--------|---------|----------------------------------------------------|
| `route_id` | string | —       | Filter to a single route, e.g. `720`.              |
| `limit`    | int    | `500`   | Maximum vehicles returned. Hard cap at 1000.       |

**Example request:**

```
GET /vehicles?bbox=-118.30,34.02,-118.20,34.10
```

(That bbox covers downtown LA → Koreatown → USC area.)

**Response: `200 OK`**

```json
{
  "count": 42,
  "as_of": "2026-05-06T22:52:09Z",
  "vehicles": [
    {
      "vehicle_id": "5817",
      "route_id": "720",
      "trip_id": "1234567",
      "lat": 34.0772,
      "lon": -118.2637,
      "bearing": 87.5,
      "speed_mps": 8.3,
      "delay_seconds": null,
      "last_updated": "2026-05-06T22:52:00Z"
    }
  ]
}
```

**Field notes for the frontend:**

- `delay_seconds` is **null in Phase 2** — schedule deviation comes online in Phase 4. UI should treat null as "delay unknown" and color the pin grey.
- `route_id` may be `""` (empty string) for vehicles that aren't on an active trip (deadheading, layover). Skip these on the map or render them with a special "out of service" style.
- `bearing` is in degrees, 0 = north, 90 = east.
- `speed_mps` is meters per second. Multiply by 2.23694 for mph.
- `last_updated` is the timestamp from the vehicle's GPS, not when our backend received it. May lag the wall clock by 30-60s.

**Error responses:**

| Code | Body                                | When                                |
|------|-------------------------------------|-------------------------------------|
| 400  | `{"error": "invalid_bbox"}`         | bbox malformed or out of range.     |
| 400  | `{"error": "bbox_too_large"}`       | bbox area > 50km × 50km.            |
| 429  | `{"error": "rate_limited"}`         | More than 60 req/min/IP.            |
| 500  | `{"error": "internal_error"}`       | Backend issue; retry with backoff.  |

## Mock data for frontend dev

While the backend isn't live yet, drop this into `frontend/lib/mock-vehicles.ts` and toggle a `USE_MOCK` env var:

```typescript
export const MOCK_VEHICLES_RESPONSE = {
  count: 5,
  as_of: new Date().toISOString(),
  vehicles: [
    { vehicle_id: '5817', route_id: '720', trip_id: '1', lat: 34.0772, lon: -118.2637, bearing: 87, speed_mps: 8.3, delay_seconds: null, last_updated: new Date().toISOString() },
    { vehicle_id: '6201', route_id: '4',   trip_id: '2', lat: 34.0500, lon: -118.2400, bearing: 180, speed_mps: 5.0, delay_seconds: null, last_updated: new Date().toISOString() },
    { vehicle_id: '7112', route_id: '720', trip_id: '3', lat: 34.0922, lon: -118.3000, bearing: 270, speed_mps: 12.1, delay_seconds: null, last_updated: new Date().toISOString() },
    { vehicle_id: '8003', route_id: '2',   trip_id: '4', lat: 34.0700, lon: -118.2200, bearing: 0,   speed_mps: 0.0, delay_seconds: null, last_updated: new Date().toISOString() },
    { vehicle_id: '9404', route_id: '',    trip_id: '',  lat: 34.0600, lon: -118.2900, bearing: 45,  speed_mps: 3.5, delay_seconds: null, last_updated: new Date().toISOString() }
  ]
};
```

## CORS

API Gateway responses include:

```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, OPTIONS
```

(Will be tightened to specific origins in Phase 9.)

## Auth

**None in Phase 2.** Endpoint is public. Cognito JWT verification is added in Phase 6.

## Future endpoints (not yet implemented — for frontend planning)

These will exist by Phase 6 / 7. Frontend can stub them out:

- `GET /routes/{routeId}/aggregates` — on-time % buckets for the day
- `GET /routes/{routeId}/predictions?stop_id=X` — predicted arrival time (Phase 7)
- `POST /geofences` — create a "alert me when route X is >Y min late" subscription (Phase 6, requires Cognito JWT)
- `GET /equity/route/{routeId}` — demographics for census tracts the route passes through (Phase 8)

## Phase 3 frontend scope (as of Phase 2 backend)

The other Claude should build these against this contract:

1. ArcGIS map of LA, base layer Esri streets
2. Poll `GET /vehicles?bbox=...` every 30 seconds with the current map bbox
3. Render each vehicle as a pin at `(lat, lon)`, oriented by `bearing`
4. Color pin by `route_id` (use a stable hash → color palette, ~20 distinct colors max)
5. Skip pins where `route_id === ''` or render in grey "out of service"
6. Click a pin → popup with `vehicle_id`, `route_id`, `speed_mps × 2.237` mph
7. Use the mock data initially; switch to live API when backend URL is ready

## Backend deployment notes (for context)

- Stack name: `LaMetro-ApiStack`
- After deploy, the API URL will be in CloudFormation outputs as `ApiUrl`. Get with:
  ```bash
  aws cloudformation describe-stacks --stack-name LaMetro-ApiStack --query 'Stacks[0].Outputs[?OutputKey==`ApiUrl`].OutputValue' --output text --region us-west-2
  ```
