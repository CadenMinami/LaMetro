'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { fetchVehicles, type BBox } from '@/lib/api';
import { delayColor, delayLabel, routeColor } from '@/lib/colors';
import { getStops, indexStops, stopsInBBox } from '@/lib/stops';
import type { Vehicle } from '@/types/vehicle';
import type { Stop } from '@/types/stop';
import { StopArrivalsPanel } from './StopArrivalsPanel';

const LA_DOWNTOWN: [number, number] = [-118.2437, 34.0522];
const POLL_MS = 30_000;
// Below this zoom, rendering ~13k stop graphics tanks ArcGIS' GraphicsLayer.
// 13 is roughly "you can see individual streets" — matches Google Maps'
// transit-stop visibility heuristic.
const STOPS_MIN_ZOOM = 13;
// Cap the number of stops rendered at any one time. The bbox at zoom 13
// covers a few hundred stops in dense LA neighborhoods; beyond that you
// can't tell them apart visually anyway.
const MAX_STOPS_RENDERED = 1500;

// Fallback bbox used before the MapView is ready. ~25km × 25km around
// downtown — well under the API's 50km × 50km cap (see API_CONTRACT.md).
const INITIAL_BBOX: BBox = {
  minLon: -118.40,
  minLat: 33.95,
  maxLon: -118.15,
  maxLat: 34.15,
};

export function MetroMap() {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const viewRef = useRef<__esri.MapView | null>(null);
  const layerRef = useRef<__esri.GraphicsLayer | null>(null);
  const stopsLayerRef = useRef<__esri.GraphicsLayer | null>(null);
  const graphicCtorRef = useRef<typeof import('@arcgis/core/Graphic').default | null>(null);
  const projectRef = useRef<
    typeof import('@arcgis/core/geometry/support/webMercatorUtils').webMercatorToGeographic | null
  >(null);
  // The full agency stops list, fetched once. Held in a ref because every
  // viewport change re-derives the visible subset — re-rendering React on
  // each pan would be wasteful.
  const stopsRef = useRef<Stop[] | null>(null);
  const stopsIndexRef = useRef<Map<string, Stop> | null>(null);
  const [count, setCount] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedStopId, setSelectedStopId] = useState<string | null>(null);

  // Init the map once on mount. ArcGIS modules are imported here rather than
  // at module top so they only load in the browser, not at static-export build.
  useEffect(() => {
    if (!containerRef.current) return;
    let cancelled = false;

    (async () => {
      const [
        { default: Map },
        { default: MapView },
        { default: GraphicsLayer },
        { default: Graphic },
        { webMercatorToGeographic },
      ] = await Promise.all([
        import('@arcgis/core/Map'),
        import('@arcgis/core/views/MapView'),
        import('@arcgis/core/layers/GraphicsLayer'),
        import('@arcgis/core/Graphic'),
        import('@arcgis/core/geometry/support/webMercatorUtils'),
      ]);
      await import('@arcgis/core/assets/esri/themes/dark/main.css');

      if (cancelled || !containerRef.current) return;

      // Two layers: stops underneath, vehicles on top. Vehicles have higher
      // information density and should always win z-order ties on click.
      const stopsLayer = new GraphicsLayer({ id: 'stops', visible: false });
      const layer = new GraphicsLayer({ id: 'vehicles' });
      const map = new Map({ basemap: 'dark-gray-vector', layers: [stopsLayer, layer] });
      const view = new MapView({
        container: containerRef.current,
        map,
        center: LA_DOWNTOWN,
        zoom: 11,
      });

      viewRef.current = view;
      layerRef.current = layer;
      stopsLayerRef.current = stopsLayer;
      graphicCtorRef.current = Graphic;
      projectRef.current = webMercatorToGeographic;

      // Click handler: hit-test stops first (they're smaller and easier to
      // miss), fall through to vehicles' default popup. We don't use a
      // popupTemplate on the stop graphic — the side panel is a richer
      // experience than the ArcGIS popup for a polling list.
      view.on('click', async (event) => {
        const hit = await view.hitTest(event, { include: stopsLayer });
        const stopHit = hit.results.find(
          (r) => r.type === 'graphic' && (r as __esri.GraphicHit).graphic.layer === stopsLayer,
        ) as __esri.GraphicHit | undefined;
        if (stopHit) {
          const stopId = stopHit.graphic.attributes?.stop_id as string | undefined;
          if (stopId) {
            // Stop the click from also opening a vehicle popup that may
            // sit underneath the stop dot.
            event.stopPropagation();
            setSelectedStopId(stopId);
          }
        }
      });

      // Re-render stops on viewport stability. `stationary` flips true
      // ~150ms after pan/zoom ends; cheaper than per-frame rendering.
      view.watch('stationary', (stationary: boolean) => {
        if (stationary) renderStops();
      });
    })();

    return () => {
      cancelled = true;
      viewRef.current?.destroy();
      viewRef.current = null;
      layerRef.current = null;
      stopsLayerRef.current = null;
    };
    // renderStops is stable — defined below as useCallback with no deps
    // beyond refs. Including it in deps would re-init the map on every
    // render, which is wrong.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Hydrate the stops list once. Cheap re-runs are guarded by the cache in
  // lib/stops.ts.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const stops = await getStops();
        if (cancelled) return;
        stopsRef.current = stops;
        stopsIndexRef.current = indexStops(stops);
        renderStops();
      } catch (err) {
        // Stops failure shouldn't kill the map — just log and skip the
        // feature. Vehicles still work.
        console.warn('fetchStops failed:', err);
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const renderStops = useCallback(() => {
    const view = viewRef.current;
    const layer = stopsLayerRef.current;
    const Graphic = graphicCtorRef.current;
    const stops = stopsRef.current;
    if (!view || !layer || !Graphic || !stops) return;

    if (view.zoom < STOPS_MIN_ZOOM) {
      if (layer.visible) {
        layer.removeAll();
        layer.visible = false;
      }
      return;
    }

    const ext = view.extent;
    const project = projectRef.current;
    if (!ext || !project) return;
    const geo = project(ext) as __esri.Extent;
    const visible = stopsInBBox(stops, geo.ymin, geo.xmin, geo.ymax, geo.xmax);
    const slice = visible.slice(0, MAX_STOPS_RENDERED);

    layer.removeAll();
    for (const s of slice) {
      // Color by the first route that visits this stop. Multi-route stops
      // get whichever route_id sorts first — good enough as a hint, the
      // panel shows the full list when the user clicks.
      const tint = s.routes[0] ? routeColor(s.routes[0]) : '#9ca3af';
      // Larger transparent hit graphic underneath gives a 12px touch target
      // even though the visible dot is only 5px.
      layer.add(
        new Graphic({
          geometry: { type: 'point', longitude: s.lon, latitude: s.lat },
          symbol: {
            type: 'simple-marker',
            color: [0, 0, 0, 0],
            size: 12,
            outline: { color: [0, 0, 0, 0], width: 0 },
          },
          attributes: { stop_id: s.id },
        }),
      );
      layer.add(
        new Graphic({
          geometry: { type: 'point', longitude: s.lon, latitude: s.lat },
          symbol: {
            type: 'simple-marker',
            color: tint,
            size: 5,
            outline: { color: '#0b0d10', width: 1 },
          },
          attributes: { stop_id: s.id },
        }),
      );
    }
    layer.visible = true;
  }, []);

  // Poll every 30s. Each tick re-derives the bbox from the current viewport
  // so panning/zooming changes which vehicles we ask for.
  useEffect(() => {
    let cancelled = false;
    const ctrl = new AbortController();

    const tick = async () => {
      try {
        const bbox = currentBBox(viewRef.current, projectRef.current) ?? INITIAL_BBOX;
        const vehicles = await fetchVehicles(bbox, ctrl.signal);
        if (cancelled) return;
        renderVehicles(vehicles);
        setCount(vehicles.length);
        setError(null);
      } catch (e) {
        if ((e as Error).name === 'AbortError') return;
        setError((e as Error).message);
      }
    };

    tick();
    const id = setInterval(tick, POLL_MS);
    return () => {
      cancelled = true;
      ctrl.abort();
      clearInterval(id);
    };
  }, []);

  function renderVehicles(vehicles: Vehicle[]) {
    const layer = layerRef.current;
    const Graphic = graphicCtorRef.current;
    if (!layer || !Graphic) return;

    layer.removeAll();
    for (const v of vehicles) {
      // Per API contract: empty route_id = deadhead/layover. Render in grey
      // rather than skipping, so the user sees the vehicle still exists.
      const outOfService = !v.route_id;
      // Delay color when known, else fall back to per-route hue, else grey
      // for out-of-service. This gives schedule reliability the strongest
      // visual signal while still distinguishing routes when no delay is
      // available (off-route / pre/post-trip vehicles).
      const dColor = delayColor(v.delay_seconds);
      const color = outOfService
        ? '#888888'
        : dColor ?? routeColor(v.route_id);
      const symbol = {
        type: 'simple-marker' as const,
        color,
        size: 8,
        outline: { color: '#0b0d10', width: 1 },
      };
      const mph = typeof v.speed_mps === 'number' ? (v.speed_mps * 2.23694).toFixed(1) : '—';
      const delayText = delayLabel(v.delay_seconds);
      // ArcGIS popup supports a clickable HTML link via a custom action.
      // Cleanest is to embed a <a href> in content — opens in same tab and
      // hits the route detail page. ArcGIS sanitizes <script> but allows <a>.
      // Trailing slash matters: Next.js static export writes `route/index.html`,
      // and CloudFront only auto-serves index.html when the path ends in '/'.
      // Without it the request 404s and our error-fallback rule sends users
      // back to the home page.
      const routeHref = v.route_id ? `/route/?id=${encodeURIComponent(v.route_id)}` : '';
      layer.add(
        new Graphic({
          geometry: { type: 'point', longitude: v.lon, latitude: v.lat },
          symbol,
          attributes: {
            vehicle_id: v.vehicle_id,
            route_id: v.route_id || '(out of service)',
            mph,
            delay_text: delayText,
            route_href: routeHref,
          },
          popupTemplate: {
            title: 'Route {route_id}',
            content: routeHref
              ? 'Vehicle {vehicle_id} — {mph} mph — <b>{delay_text}</b><br/><a href="{route_href}">→ route detail</a>'
              : 'Vehicle {vehicle_id} — {mph} mph — {delay_text}',
          },
        }),
      );
    }
  }

  // ESC key closes the panel. Lives in its own effect so the listener
  // attaches/detaches based on `selectedStopId` rather than mount.
  useEffect(() => {
    if (!selectedStopId) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setSelectedStopId(null);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [selectedStopId]);

  const selectedStop = selectedStopId ? stopsIndexRef.current?.get(selectedStopId) ?? null : null;

  return (
    <div className="relative h-screen w-screen">
      <div ref={containerRef} className="h-full w-full" />
      <div className="pointer-events-none absolute left-4 top-4 rounded bg-black/60 px-3 py-2 text-sm">
        <div className="font-semibold">LA Metro — Live</div>
        <div className="opacity-80">
          {count === null ? 'loading…' : `${count} vehicle${count === 1 ? '' : 's'}`}
        </div>
        {error && <div className="text-red-400">err: {error}</div>}
      </div>
      {selectedStopId && (
        <StopArrivalsPanel
          stopId={selectedStopId}
          stop={selectedStop}
          onClose={() => setSelectedStopId(null)}
        />
      )}
    </div>
  );
}

function currentBBox(
  view: __esri.MapView | null,
  toGeographic:
    | typeof import('@arcgis/core/geometry/support/webMercatorUtils').webMercatorToGeographic
    | null,
): BBox | null {
  const ext = view?.extent;
  if (!ext || !toGeographic) return null;
  // The default basemap is Web Mercator (EPSG:3857), so view.extent comes
  // back in meters. Project to WGS84 lon/lat before sending to the API.
  const geo = toGeographic(ext) as __esri.Extent;
  // Clamp to ~0.4° per side so we stay under the API's 50km × 50km cap when
  // the user zooms way out.
  const MAX_SPAN = 0.4;
  const lonSpan = Math.min(geo.xmax - geo.xmin, MAX_SPAN);
  const latSpan = Math.min(geo.ymax - geo.ymin, MAX_SPAN);
  const cx = (geo.xmin + geo.xmax) / 2;
  const cy = (geo.ymin + geo.ymax) / 2;
  return {
    minLon: cx - lonSpan / 2,
    minLat: cy - latSpan / 2,
    maxLon: cx + lonSpan / 2,
    maxLat: cy + latSpan / 2,
  };
}
