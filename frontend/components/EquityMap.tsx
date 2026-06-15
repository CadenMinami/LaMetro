'use client';

// Phase 8 — Transit equity map. Renders a census-tract median-income
// choropleth + a route-reliability overlay from two static GeoJSON files
// (produced by ml/equity_analysis.py and bundled in public/geojson/), plus a
// floating "finding" panel. Mirrors MetroMap's dynamic-import + dark-theme
// conventions, but static (no sockets / polling).

import { useEffect, useRef, useState } from 'react';
import Link from 'next/link';

const LA_DOWNTOWN: [number, number] = [-118.2437, 34.0522];

// Median household income choropleth (sequential blues; darker = higher
// income). Distinct hue family from the red/green reliability lines so the two
// layers never read as the same scale.
const INCOME_BREAKS = [
  { min: -Infinity, max: 40000, color: '#eff3ff', label: '< $40k' },
  { min: 40000, max: 60000, color: '#bdd7e7', label: '$40–60k' },
  { min: 60000, max: 90000, color: '#6baed6', label: '$60–90k' },
  { min: 90000, max: 120000, color: '#3182bd', label: '$90–120k' },
  { min: 120000, max: Infinity, color: '#08519c', label: '> $120k' },
];

// Route reliability (on-time %): low = red, high = green.
const RELIABILITY_BREAKS = [
  { min: -Infinity, max: 70, color: '#ef4444', label: '< 70%' },
  { min: 70, max: 80, color: '#f97316', label: '70–80%' },
  { min: 80, max: 90, color: '#eab308', label: '80–90%' },
  { min: 90, max: Infinity, color: '#22c55e', label: '> 90%' },
];

interface Finding {
  placeholder?: boolean;
  n_routes: number | null;
  bottom_quartile_on_time_pct: number | null;
  top_quartile_on_time_pct: number | null;
  gap_pct_points: number | null;
  pearson_r: number | null;
  spearman_rho: number | null;
  bottom_quartile_income?: number | null;
  top_quartile_income?: number | null;
}

export default function EquityMap() {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [finding, setFinding] = useState<Finding | null>(null);

  useEffect(() => {
    fetch('/geojson/equity_finding.json', { cache: 'no-store' })
      .then((r) => (r.ok ? r.json() : null))
      .then(setFinding)
      .catch(() => setFinding(null));
  }, []);

  useEffect(() => {
    if (!containerRef.current) return;
    let cancelled = false;
    let view: { destroy: () => void } | null = null;

    (async () => {
      // Browser-only imports (keeps them out of the static-export build).
      const [
        { default: Map },
        { default: MapView },
        { default: GeoJSONLayer },
        { default: ClassBreaksRenderer },
        { default: SimpleFillSymbol },
        { default: SimpleLineSymbol },
      ] = await Promise.all([
        import('@arcgis/core/Map'),
        import('@arcgis/core/views/MapView'),
        import('@arcgis/core/layers/GeoJSONLayer'),
        import('@arcgis/core/renderers/ClassBreaksRenderer'),
        import('@arcgis/core/symbols/SimpleFillSymbol'),
        import('@arcgis/core/symbols/SimpleLineSymbol'),
      ]);
      await import('@arcgis/core/assets/esri/themes/dark/main.css');
      if (cancelled || !containerRef.current) return;

      // Income choropleth.
      const incomeRenderer = new ClassBreaksRenderer({
        field: 'median_income',
        defaultSymbol: new SimpleFillSymbol({
          color: [80, 80, 80, 0.35],
          outline: { color: [255, 255, 255, 0.15], width: 0.3 },
        }),
        classBreakInfos: INCOME_BREAKS.map((b) => ({
          minValue: b.min === -Infinity ? -9_999_999 : b.min,
          maxValue: b.max === Infinity ? 9_999_999 : b.max,
          symbol: new SimpleFillSymbol({
            color: b.color,
            outline: { color: [255, 255, 255, 0.12], width: 0.3 },
          }),
        })),
      });

      const tractsLayer = new GeoJSONLayer({
        url: '/geojson/equity_tracts.geojson',
        title: 'Median household income',
        opacity: 0.6,
        renderer: incomeRenderer as never,
        popupTemplate: {
          title: 'Census tract',
          content: 'Median household income: ${median_income}',
        },
      });

      // Route reliability overlay (lines).
      const reliabilityRenderer = new ClassBreaksRenderer({
        field: 'on_time_pct',
        defaultSymbol: new SimpleLineSymbol({ color: [150, 150, 150, 0.9], width: 2 }),
        classBreakInfos: RELIABILITY_BREAKS.map((b) => ({
          minValue: b.min === -Infinity ? -9_999_999 : b.min,
          maxValue: b.max === Infinity ? 9_999_999 : b.max,
          symbol: new SimpleLineSymbol({ color: b.color, width: 2.5 }),
        })),
      });

      const routesLayer = new GeoJSONLayer({
        url: '/geojson/equity_routes.geojson',
        title: 'Route on-time %',
        renderer: reliabilityRenderer as never,
        popupTemplate: {
          title: 'Route {route_id}',
          content:
            'On-time: ${on_time_pct}%<br/>Served-area median income: ${served_income}',
        },
      });

      const map = new Map({ basemap: 'dark-gray-vector', layers: [tractsLayer, routesLayer] });
      const mapView = new MapView({
        container: containerRef.current,
        map,
        center: LA_DOWNTOWN,
        zoom: 10,
      });
      view = mapView;
    })();

    return () => {
      cancelled = true;
      if (view) view.destroy();
    };
  }, []);

  const hasFinding = finding && !finding.placeholder && finding.gap_pct_points != null;

  return (
    <div className="relative h-screen w-screen overflow-hidden">
      <div ref={containerRef} className="h-full w-full" />

      <div className="pointer-events-none absolute inset-0 p-4 sm:p-5">
        {/* Finding panel */}
        <div
          className="pointer-events-auto w-[300px] overflow-hidden rounded-lg
                     border border-white/10 bg-black/70 text-zinc-100 shadow-xl
                     shadow-black/40 backdrop-blur-xl"
        >
          <div className="flex items-center justify-between gap-2 px-4 pt-4 pb-3">
            <div className="text-[15px] font-semibold leading-none tracking-tight">
              Transit Equity
            </div>
            <Link href="/" className="text-xs text-blue-400 hover:underline">
              ← Map
            </Link>
          </div>

          <div className="border-t border-white/[0.06] px-4 py-3.5">
            {hasFinding ? (
              <>
                <p className="text-sm leading-relaxed text-zinc-200">
                  Routes serving LA&apos;s{' '}
                  <span className="font-semibold text-zinc-50">lowest-income</span>{' '}
                  neighborhoods ran on time{' '}
                  <span className="font-mono text-zinc-50">
                    {finding!.bottom_quartile_on_time_pct}%
                  </span>{' '}
                  of the time, vs{' '}
                  <span className="font-mono text-zinc-50">
                    {finding!.top_quartile_on_time_pct}%
                  </span>{' '}
                  for the{' '}
                  <span className="font-semibold text-zinc-50">highest-income</span> —
                  a{' '}
                  <span className="font-mono text-amber-300">
                    {Math.abs(finding!.gap_pct_points as number)} pt
                  </span>{' '}
                  gap.
                </p>
                <div className="mt-2 text-[11px] leading-snug text-zinc-500">
                  n = {finding!.n_routes} routes · Pearson r ={' '}
                  <span className="font-mono">{finding!.pearson_r}</span> · Spearman ρ ={' '}
                  <span className="font-mono">{finding!.spearman_rho}</span>
                </div>
              </>
            ) : (
              <p className="text-sm text-zinc-400">
                Analysis pending — run{' '}
                <span className="font-mono text-zinc-300">ml/equity_analysis.py</span> to
                populate the finding.
              </p>
            )}
          </div>

          {/* Income legend */}
          <div className="border-t border-white/[0.06] px-4 py-3.5">
            <div className="mb-2 text-[12px] text-zinc-400">Median household income</div>
            <div className="grid grid-cols-2 gap-x-3 gap-y-1.5 text-[12px] text-zinc-300">
              {INCOME_BREAKS.map((b) => (
                <LegendSwatch key={b.label} color={b.color} label={b.label} />
              ))}
            </div>
          </div>

          {/* Reliability legend */}
          <div className="border-t border-white/[0.06] px-4 py-3.5">
            <div className="mb-2 text-[12px] text-zinc-400">Route on-time %</div>
            <div className="grid grid-cols-2 gap-x-3 gap-y-1.5 text-[12px] text-zinc-300">
              {RELIABILITY_BREAKS.map((b) => (
                <LegendSwatch key={b.label} color={b.color} label={b.label} line />
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function LegendSwatch({
  color,
  label,
  line = false,
}: {
  color: string;
  label: string;
  line?: boolean;
}) {
  return (
    <div className="flex items-center gap-2">
      <span
        className={line ? 'h-[3px] w-3 rounded-full' : 'h-2.5 w-2.5 rounded-sm'}
        style={{ background: color }}
        aria-hidden
      />
      <span>{label}</span>
    </div>
  );
}
