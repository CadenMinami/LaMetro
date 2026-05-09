'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { fetchVehicles, type BBox } from '@/lib/api';
import { delayColor, delayLabel, routeColor } from '@/lib/colors';
import { getStops, indexStops, stopsInBBox } from '@/lib/stops';
import { openVehicleSocket, type VehicleSocketHandle } from '@/lib/socket';
import type { Vehicle } from '@/types/vehicle';
import type { Stop } from '@/types/stop';
import { StopArrivalsPanel } from './StopArrivalsPanel';

const LA_DOWNTOWN: [number, number] = [-118.2437, 34.0522];
// Polling stays as a fallback (WebSocket not configured / disconnected). When
// the socket is open and feeding events, we still poll occasionally to
// reconcile any vehicles the bbox missed (e.g., entered via teleport).
const POLL_MS = 30_000;
const POLL_FALLBACK_MS = 5_000;
// How long a position update glides from old → new lat/lon. Vehicles update
// every 3-5s so 1.5s gives a continuous-motion feel without overshooting
// the next event.
const GLIDE_MS = 1500;
// Drop a pin once its server-side `last_updated` is older than this. The
// hot-vehicles TTL is 1 hour, but at the dashboard level "still moving"
// is much stricter — anything > 5 min stale is almost certainly a vehicle
// that left our bbox or finished its trip.
const PIN_STALE_MS = 5 * 60 * 1000;
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

// One pin per active vehicle. Lives outside React state — pin geometry
// updates every animation frame, which would thrash a render loop.
interface AnimatedPin {
  graphic: __esri.Graphic;
  // Glide source (where it currently sits) and target (where the latest
  // event placed it). `startedAt` is null when no animation is in flight.
  fromLon: number;
  fromLat: number;
  toLon: number;
  toLat: number;
  startedAt: number | null;
  // Last-seen color so we don't rebuild the symbol object on every event
  // when nothing changed visually.
  color: string;
  // Most recent vehicle payload, kept around for popup re-rendering on click.
  vehicle: Vehicle;
}

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
  // Per-vehicle pins, keyed by vehicle_id, kept in a ref so animation
  // updates don't trigger React renders.
  const pinsRef = useRef<Map<string, AnimatedPin>>(new Map());
  const rafRef = useRef<number | null>(null);
  const socketRef = useRef<VehicleSocketHandle | null>(null);
  const [count, setCount] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedStopId, setSelectedStopId] = useState<string | null>(null);
  // 'live' = receiving WebSocket frames; 'polling' = fallback. Shown in HUD.
  const [feedMode, setFeedMode] = useState<'live' | 'polling'>('polling');

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

  // Merge a batch of vehicles into the pin map. Existing pins glide from
  // their current geometry to the new position; new pins appear at their
  // first reported lat/lon. Symbol/attribute updates only fire when the
  // visible color or label actually changed — re-creating the SimpleMarker
  // object on every frame causes ArcGIS to re-render unnecessarily.
  const mergeVehicles = useCallback((vehicles: Vehicle[]) => {
    const layer = layerRef.current;
    const Graphic = graphicCtorRef.current;
    if (!layer || !Graphic) return;

    const pins = pinsRef.current;
    const now = performance.now();

    for (const v of vehicles) {
      const id = v.vehicle_id;
      if (!id) continue;

      const outOfService = !v.route_id;
      const dColor = delayColor(v.delay_seconds);
      const color = outOfService ? '#888888' : dColor ?? routeColor(v.route_id);
      const mph = typeof v.speed_mps === 'number' ? (v.speed_mps * 2.23694).toFixed(1) : '—';
      const delayText = delayLabel(v.delay_seconds);
      // Trailing slash matters: Next.js static export writes route/index.html,
      // and CloudFront only auto-serves it when the path ends in '/'.
      const routeHref = v.route_id ? `/route/?id=${encodeURIComponent(v.route_id)}` : '';

      const existing = pins.get(id);
      if (existing) {
        // Glide from wherever the pin currently is — not from its previous
        // target — so a fast event burst doesn't snap mid-animation.
        const t = existing.startedAt
          ? Math.min(1, (now - existing.startedAt) / GLIDE_MS)
          : 1;
        existing.fromLon = existing.fromLon + (existing.toLon - existing.fromLon) * t;
        existing.fromLat = existing.fromLat + (existing.toLat - existing.fromLat) * t;
        existing.toLon = v.lon;
        existing.toLat = v.lat;
        existing.startedAt = now;
        existing.vehicle = v;
        if (existing.color !== color) {
          existing.color = color;
          // SimpleMarkerSymbol exposes `color` as a settable property; mutate
          // it in place rather than replacing the whole symbol object so TS
          // doesn't have to reason about the SymbolProperties union.
          (existing.graphic.symbol as __esri.SimpleMarkerSymbol).color = color as unknown as __esri.Color;
        }
        existing.graphic.attributes = {
          vehicle_id: id,
          route_id: v.route_id || '(out of service)',
          mph,
          delay_text: delayText,
          route_href: routeHref,
        };
        continue;
      }

      const graphic = new Graphic({
        geometry: { type: 'point', longitude: v.lon, latitude: v.lat },
        symbol: {
          type: 'simple-marker',
          color,
          size: 8,
          outline: { color: '#0b0d10', width: 1 },
        },
        attributes: {
          vehicle_id: id,
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
      });
      layer.add(graphic);
      pins.set(id, {
        graphic,
        fromLon: v.lon,
        fromLat: v.lat,
        toLon: v.lon,
        toLat: v.lat,
        startedAt: null,
        color,
        vehicle: v,
      });
    }

    // Sweep stale pins. last_updated is an ISO Z string; anything older
    // than PIN_STALE_MS gets removed from both the map layer and our index.
    // Without this, count drifts upward forever as vehicles enter the
    // bbox, get tracked, leave, and never get replaced.
    const cutoff = Date.now() - PIN_STALE_MS;
    for (const [id, pin] of pins) {
      const lu = pin.vehicle.last_updated;
      if (!lu) continue;
      if (Date.parse(lu) < cutoff) {
        layer.remove(pin.graphic);
        pins.delete(id);
      }
    }

    setCount(pins.size);
  }, []);

  // Run a per-frame loop that lerps each pin's geometry from `from` to
  // `to`. The work is constant-time per pin per frame; for ~1.7k visible
  // vehicles that's ~50k geometry assignments/sec — well within ArcGIS'
  // budget on a modern laptop.
  useEffect(() => {
    function tick() {
      const now = performance.now();
      for (const pin of pinsRef.current.values()) {
        if (pin.startedAt == null) continue;
        const t = Math.min(1, (now - pin.startedAt) / GLIDE_MS);
        const lon = pin.fromLon + (pin.toLon - pin.fromLon) * t;
        const lat = pin.fromLat + (pin.toLat - pin.fromLat) * t;
        pin.graphic.geometry = { type: 'point', longitude: lon, latitude: lat } as __esri.Point;
        if (t >= 1) {
          pin.fromLon = pin.toLon;
          pin.fromLat = pin.toLat;
          pin.startedAt = null;
        }
      }
      rafRef.current = requestAnimationFrame(tick);
    }
    rafRef.current = requestAnimationFrame(tick);
    return () => {
      if (rafRef.current != null) cancelAnimationFrame(rafRef.current);
      rafRef.current = null;
    };
  }, []);

  // Live feed: open a WebSocket, send `subscribe` whenever the viewport
  // settles on a new bbox. Falls back to no-op when WS_URL isn't set.
  useEffect(() => {
    const handle = openVehicleSocket({
      onMessage: (msg) => {
        if (msg.type === 'positions') {
          mergeVehicles(msg.vehicles);
          setError(null);
        }
      },
      onStateChange: (state) => {
        setFeedMode(state === 'open' ? 'live' : 'polling');
      },
    });
    socketRef.current = handle;
    return () => {
      handle.close();
      socketRef.current = null;
    };
  }, [mergeVehicles]);

  // Re-subscribe on viewport stability. The map's `stationary` watcher
  // already fires for stops; we add a separate subscription here rather
  // than trying to share that handler so this hook is independent.
  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    const handle = view.watch('stationary', (stationary: boolean) => {
      if (!stationary) return;
      const bbox = currentBBox(view, projectRef.current) ?? INITIAL_BBOX;
      socketRef.current?.setBBox(bbox);
    });
    return () => handle.remove();
    // viewRef.current is set inside the init effect; we re-run when count
    // first goes non-null which guarantees the view exists.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [count]);

  // Polling: bootstraps initial state and acts as a fallback when WS is
  // unavailable. Faster cadence (5s) when we're in polling mode, slow (30s)
  // when WS is doing the heavy lifting.
  useEffect(() => {
    let cancelled = false;
    const ctrl = new AbortController();

    const tick = async () => {
      try {
        const bbox = currentBBox(viewRef.current, projectRef.current) ?? INITIAL_BBOX;
        const vehicles = await fetchVehicles(bbox, ctrl.signal);
        if (cancelled) return;
        mergeVehicles(vehicles);
        setError(null);
      } catch (e) {
        if ((e as Error).name === 'AbortError') return;
        setError((e as Error).message);
      }
    };

    tick();
    const interval = feedMode === 'live' ? POLL_MS : POLL_FALLBACK_MS;
    const id = setInterval(tick, interval);
    return () => {
      cancelled = true;
      ctrl.abort();
      clearInterval(id);
    };
  }, [feedMode, mergeVehicles]);

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
        <div className="text-xs opacity-60">
          {feedMode === 'live' ? '● ws' : '○ polling'}
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
