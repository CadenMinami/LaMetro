'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { fetchVehicles, type BBox } from '@/lib/api';
import { bearingCompass, delayColor, delayLabel, routeColor, timeAgo } from '@/lib/colors';
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
// Stops layer is intentionally hidden until the user zooms in enough that
// individual stops are useful (zoom 15 ≈ "you can read street names"). At
// zoom 13-14, stops just turn into a 12k-dot grid that dominates the
// vehicles you actually came to see.
const STOPS_MIN_ZOOM = 15;
// Cap rendered stops to keep the GraphicsLayer responsive even on a
// dense viewport.
const MAX_STOPS_RENDERED = 800;

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
  // Two-tone: fill encodes schedule deviation, outline encodes route id.
  // Stored so we skip symbol mutations when neither has changed.
  fillColor: string;
  outlineColor: string;
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
  // Filters. Most of the active fleet is deadhead/layover (parked at the
  // garage, between trips); hide them by default so the map shows what
  // people actually mean by "buses on the road".
  const [showOutOfService, setShowOutOfService] = useState(false);
  const [routeFilter, setRouteFilter] = useState<string>('');
  const [inServiceCount, setInServiceCount] = useState(0);
  const [routes, setRoutes] = useState<string[]>([]);

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
      // Larger transparent hit graphic underneath gives a 12px touch target
      // even though the visible dot is only 4px.
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
      // Stops should *recede* against the dark basemap, not compete with
      // vehicle pins for attention. Small, single neutral fill, no outline.
      // The vehicle pins (8px, saturated colors) win the visual hierarchy.
      layer.add(
        new Graphic({
          geometry: { type: 'point', longitude: s.lon, latitude: s.lat },
          symbol: {
            type: 'simple-marker',
            color: [180, 180, 200, 0.5],
            size: 4,
            outline: { color: [0, 0, 0, 0], width: 0 },
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
      // Two-tone pin: fill = schedule deviation (gray when no data),
      // outline = route id. Lets a single pin encode both signals so the
      // legend stays consistent — every fill on the map matches a swatch.
      const dColor = delayColor(v.delay_seconds);
      const fillColor = outOfService ? '#71717a' : dColor ?? '#71717a';
      const outlineColor = outOfService ? '#3f3f46' : routeColor(v.route_id);
      const mph = typeof v.speed_mps === 'number' ? (v.speed_mps * 2.23694).toFixed(1) : '—';
      const delayText = delayLabel(v.delay_seconds);
      const bearingText = bearingCompass(v.bearing);
      // Trailing slash matters: Next.js static export writes route/index.html,
      // and CloudFront only auto-serves it when the path ends in '/'.
      const routeHref = v.route_id ? `/route/?id=${encodeURIComponent(v.route_id)}` : '';
      const attrs = {
        vehicle_id: id,
        route_id: v.route_id || '(out of service)',
        trip_id: v.trip_id || '',
        mph,
        delay_text: delayText,
        delay_seconds: v.delay_seconds ?? null,
        bearing_text: bearingText,
        last_updated: v.last_updated ?? '',
        route_href: routeHref,
      };

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
        // SimpleMarkerSymbol exposes `color` and `outline` as settable
        // properties; mutate in place rather than rebuilding the symbol so
        // TS doesn't have to reason about the SymbolProperties union.
        const sym = existing.graphic.symbol as __esri.SimpleMarkerSymbol;
        if (existing.fillColor !== fillColor) {
          existing.fillColor = fillColor;
          sym.color = fillColor as unknown as __esri.Color;
        }
        if (existing.outlineColor !== outlineColor) {
          existing.outlineColor = outlineColor;
          sym.outline.color = outlineColor as unknown as __esri.Color;
        }
        existing.graphic.attributes = attrs;
        continue;
      }

      const graphic = new Graphic({
        geometry: { type: 'point', longitude: v.lon, latitude: v.lat },
        symbol: {
          type: 'simple-marker',
          color: fillColor,
          size: 10,
          // Outline is the route signal — wide enough to read at 10px size.
          outline: { color: outlineColor, width: 2 },
        },
        attributes: attrs,
        popupTemplate: {
          title: 'Route {route_id}',
          // The function form lets us recompute "updated Xs ago" each time
          // the popup opens (the static {token} interpolation locks values
          // at graphic-creation time).
          content: vehiclePopupContent,
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
        fillColor,
        outlineColor,
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
    applyFilters();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Filter state is read from a ref inside applyFilters so mergeVehicles
  // (called from both the WS message handler and the polling tick) can
  // call us without us having to be in its deps and rerun on every filter
  // change.
  const filtersRef = useRef({ showOutOfService, routeFilter });
  useEffect(() => {
    filtersRef.current = { showOutOfService, routeFilter };
    applyFilters();
  }, [showOutOfService, routeFilter]); // eslint-disable-line react-hooks/exhaustive-deps

  const applyFilters = useCallback(() => {
    const { showOutOfService: showOOS, routeFilter: rf } = filtersRef.current;
    const pins = pinsRef.current;
    const routeSet = new Set<string>();
    let visibleInService = 0;
    for (const pin of pins.values()) {
      const v = pin.vehicle;
      if (v.route_id) routeSet.add(v.route_id);
      const isInService = !!v.route_id;
      let visible = true;
      if (!isInService && !showOOS) visible = false;
      if (rf && v.route_id !== rf) visible = false;
      pin.graphic.visible = visible;
      if (visible && isInService) visibleInService += 1;
    }
    setInServiceCount(visibleInService);
    // Only refresh the dropdown list when it actually changed — sorting
    // and re-allocating ~240 strings on every batch causes React to
    // re-render the route picker unnecessarily.
    setRoutes((prev) => {
      const next = [...routeSet].sort(routeSort);
      if (prev.length === next.length && prev.every((r, i) => r === next[i])) {
        return prev;
      }
      return next;
    });
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
    <div className="relative h-screen w-screen overflow-hidden">
      <div ref={containerRef} className="h-full w-full" />

      {/* Sidebar panel. Sentence-case sans labels, mono reserved for
          numeric values only — keeps the panel feeling like a tool rather
          than a sci-fi HUD. */}
      <div className="pointer-events-none absolute inset-0 p-4 sm:p-5">
        <div
          className="pointer-events-auto w-[280px] overflow-hidden rounded-lg
                     border border-white/10 bg-black/70 text-zinc-100 shadow-xl
                     shadow-black/40 backdrop-blur-xl"
        >
          {/* Header — wordmark + live state pill. */}
          <div className="flex items-center justify-between gap-2 px-4 pt-4 pb-3">
            <div className="text-[15px] font-semibold leading-none tracking-tight">
              LA Metro Live
            </div>
            <FeedPill mode={feedMode} />
          </div>

          {/* Primary stat — fleet count. */}
          <div className="border-t border-white/[0.06] px-4 py-3.5">
            <div className="font-mono text-[28px] font-medium leading-none tabular-nums tracking-tight">
              {count === null ? '—' : inServiceCount.toLocaleString()}
            </div>
            <div className="mt-1.5 text-[12px] text-zinc-400">
              vehicles in service
              {count !== null && (
                <span className="text-zinc-500">
                  {' '}· <span className="tabular-nums">{count.toLocaleString()}</span> active
                </span>
              )}
            </div>
          </div>

          {/* Filters */}
          <div className="space-y-3 border-t border-white/[0.06] px-4 py-3.5">
            <label className="flex cursor-pointer select-none items-center gap-2 text-[12px]
                              text-zinc-300 transition-colors hover:text-white">
              <input
                type="checkbox"
                checked={showOutOfService}
                onChange={(e) => setShowOutOfService(e.target.checked)}
                className="h-3.5 w-3.5 cursor-pointer accent-zinc-400"
              />
              Show deadhead / layover
            </label>

            <div>
              <label
                htmlFor="route-filter"
                className="mb-1.5 block text-[12px] text-zinc-400"
              >
                Route{' '}
                <span className="text-zinc-500">
                  (<span className="tabular-nums">{routes.length}</span> active)
                </span>
              </label>
              <div className="flex gap-1.5">
                <input
                  id="route-filter"
                  list="route-options"
                  type="text"
                  value={routeFilter}
                  onChange={(e) => setRouteFilter(e.target.value)}
                  placeholder="All routes"
                  className="w-full rounded border border-white/10 bg-white/[0.04] px-2 py-1.5
                             text-[13px] text-zinc-100 transition-colors
                             placeholder:text-zinc-500
                             focus:border-white/25 focus:bg-white/[0.06] focus:outline-none"
                  autoComplete="off"
                  spellCheck={false}
                />
                {routeFilter && (
                  <button
                    type="button"
                    onClick={() => setRouteFilter('')}
                    className="rounded border border-white/10 bg-white/[0.04] px-2 text-zinc-400
                               transition-colors hover:bg-white/10 hover:text-white"
                    aria-label="Clear route filter"
                  >
                    ×
                  </button>
                )}
              </div>
              <datalist id="route-options">
                {routes.map((r) => (
                  <option key={r} value={r} />
                ))}
              </datalist>
            </div>
          </div>

          {/* Delay legend. Pin fill = deviation, pin outline = route id. */}
          <div className="border-t border-white/[0.06] px-4 py-3.5">
            <div className="mb-2 text-[12px] text-zinc-400">Schedule deviation</div>
            <div className="grid grid-cols-2 gap-x-3 gap-y-1.5 text-[12px] text-zinc-300">
              <LegendDot color="#22c55e" label="On time" />
              <LegendDot color="#eab308" label="1–3 min" />
              <LegendDot color="#f97316" label="3–5 min" />
              <LegendDot color="#ef4444" label="5+ min" />
              <LegendDot color="#71717a" label="No data" />
            </div>
            <div className="mt-2 text-[11px] leading-snug text-zinc-500">
              Pin outline color identifies the route.
            </div>
          </div>

          {error && (
            <div className="border-t border-red-500/20 bg-red-500/[0.06] px-4 py-2 text-[12px] text-red-300">
              <span className="text-red-400">Error:</span> {error}
            </div>
          )}
        </div>
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

// Live/Polling indicator pill. Amber when the WebSocket is feeding events
// (the dispatcher-console "active" color), neutral when we've fallen back
// to polling. Pulse dot draws the eye to the state without being noisy.
function FeedPill({ mode }: { mode: 'live' | 'polling' }) {
  const live = mode === 'live';
  return (
    <span
      className={`inline-flex items-center gap-1.5 text-[11px]
                  ${live ? 'text-emerald-300/90' : 'text-zinc-500'}`}
    >
      <span
        className={`h-1.5 w-1.5 rounded-full ${
          live ? 'bg-emerald-400 animate-pulse-dot' : 'bg-zinc-500'
        }`}
        aria-hidden
      />
      {live ? 'live' : 'polling'}
    </span>
  );
}

function LegendDot({ color, label }: { color: string; label: string }) {
  return (
    <div className="flex items-center gap-2">
      <span
        className="h-2 w-2 rounded-full"
        style={{ background: color }}
        aria-hidden
      />
      <span>{label}</span>
    </div>
  );
}

// Build a richer info card for the vehicle popup. Returns a real
// HTMLElement so we can compute relative time at open-time (the {token}
// templating ArcGIS supports is static).
function vehiclePopupContent(target: { graphic: __esri.Graphic }): HTMLElement {
  const a = target.graphic.attributes ?? {};
  const SANS = 'var(--font-sans), ui-sans-serif, system-ui, sans-serif';
  const MONO = 'var(--font-mono), ui-monospace, SFMono-Regular, Menlo, monospace';

  const root = document.createElement('div');
  root.style.cssText =
    `font-family: ${SANS}; font-size: 13px; min-width: 240px; line-height: 1.5; color: #e8eaed;`;

  const delaySec: number | null = typeof a.delay_seconds === 'number' ? a.delay_seconds : null;
  const delayPillColor = delayColor(delaySec) ?? '#52525b';
  const delayPillText = delayLabel(delaySec);

  // Top row: delay pill + speed readout
  const top = document.createElement('div');
  top.style.cssText = 'display: flex; gap: 10px; align-items: center; margin-bottom: 12px;';
  const pill = document.createElement('span');
  pill.textContent = delayPillText;
  pill.style.cssText =
    `background: ${delayPillColor}; color: #0a0c0f; padding: 2px 9px;` +
    `border-radius: 999px; font-weight: 600; font-size: 11px;`;
  top.appendChild(pill);
  const speed = document.createElement('span');
  speed.style.cssText = `font-family: ${MONO}; font-size: 12px; color: #a1a1aa; font-variant-numeric: tabular-nums;`;
  speed.textContent = `${a.mph} mph`;
  top.appendChild(speed);
  root.appendChild(top);

  // Detail rows. Sentence-case sans labels; mono only on identifier values
  // so digits line up.
  const rows: Array<[string, string, boolean]> = [
    ['Vehicle', String(a.vehicle_id ?? '—'), true],
    ['Trip', a.trip_id ? String(a.trip_id) : '—', true],
    ['Heading', String(a.bearing_text ?? '—'), false],
    ['Updated', timeAgo(a.last_updated), false],
  ];
  for (const [k, v, valueIsMono] of rows) {
    const row = document.createElement('div');
    row.style.cssText = 'display: flex; gap: 12px; margin-top: 3px; align-items: baseline;';
    const key = document.createElement('span');
    key.textContent = k;
    key.style.cssText = 'font-size: 12px; color: #a1a1aa; min-width: 64px;';
    const val = document.createElement('span');
    val.textContent = v;
    val.style.cssText = valueIsMono
      ? `font-family: ${MONO}; font-size: 12px; color: #e8eaed; font-variant-numeric: tabular-nums;`
      : `font-size: 13px; color: #e8eaed;`;
    row.appendChild(key);
    row.appendChild(val);
    root.appendChild(row);
  }

  if (a.route_href) {
    const link = document.createElement('a');
    link.href = String(a.route_href);
    link.textContent = 'Route detail →';
    link.style.cssText =
      'display: inline-block; margin-top: 12px; font-size: 13px; color: #93c5fd; text-decoration: none;';
    link.onmouseenter = () => { link.style.textDecoration = 'underline'; };
    link.onmouseleave = () => { link.style.textDecoration = 'none'; };
    root.appendChild(link);
  }

  return root;
}

// Sort by the leading numeric part of the route_id so "2", "10", "720"
// land in human order rather than "10", "2", "720" lexical order. Falls
// back to localeCompare for non-numeric routes (e.g. "Red", "Purple").
function routeSort(a: string, b: string): number {
  const ka = parseInt(a.match(/^(\d+)/)?.[1] ?? '', 10);
  const kb = parseInt(b.match(/^(\d+)/)?.[1] ?? '', 10);
  if (!Number.isNaN(ka) && !Number.isNaN(kb)) return ka - kb || a.localeCompare(b);
  if (!Number.isNaN(ka)) return -1;
  if (!Number.isNaN(kb)) return 1;
  return a.localeCompare(b);
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
