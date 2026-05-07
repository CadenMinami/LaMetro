'use client';

import { useEffect, useRef, useState } from 'react';
import { fetchVehicles, type BBox } from '@/lib/api';
import { routeColor } from '@/lib/colors';
import type { Vehicle } from '@/types/vehicle';

const LA_DOWNTOWN: [number, number] = [-118.2437, 34.0522];
const POLL_MS = 30_000;

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
  const graphicCtorRef = useRef<typeof import('@arcgis/core/Graphic').default | null>(null);
  const projectRef = useRef<
    typeof import('@arcgis/core/geometry/support/webMercatorUtils').webMercatorToGeographic | null
  >(null);
  const [count, setCount] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

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

      const layer = new GraphicsLayer();
      const map = new Map({ basemap: 'dark-gray-vector', layers: [layer] });
      const view = new MapView({
        container: containerRef.current,
        map,
        center: LA_DOWNTOWN,
        zoom: 11,
      });

      viewRef.current = view;
      layerRef.current = layer;
      graphicCtorRef.current = Graphic;
      projectRef.current = webMercatorToGeographic;
    })();

    return () => {
      cancelled = true;
      viewRef.current?.destroy();
      viewRef.current = null;
      layerRef.current = null;
    };
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
      const color = outOfService ? '#888888' : routeColor(v.route_id);
      const symbol = {
        type: 'simple-marker' as const,
        color,
        size: 8,
        outline: { color: '#0b0d10', width: 1 },
      };
      const mph = typeof v.speed_mps === 'number' ? (v.speed_mps * 2.23694).toFixed(1) : '—';
      layer.add(
        new Graphic({
          geometry: { type: 'point', longitude: v.lon, latitude: v.lat },
          symbol,
          attributes: {
            vehicle_id: v.vehicle_id,
            route_id: v.route_id || '(out of service)',
            mph,
          },
          popupTemplate: {
            title: 'Route {route_id}',
            content: 'Vehicle {vehicle_id} — {mph} mph',
          },
        }),
      );
    }
  }

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
