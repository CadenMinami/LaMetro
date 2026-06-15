'use client';

import { Suspense, useEffect, useState } from 'react';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import {
  fetchRouteAggregates,
  fetchRoutePrediction,
  type RouteAggregateWindow,
  type RoutePrediction,
} from '@/lib/api';
import { delayLabel } from '@/lib/colors';

function RoutePageInner() {
  const params = useSearchParams();
  const routeId = params.get('id') ?? '';
  const [windows, setWindows] = useState<RouteAggregateWindow[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [prediction, setPrediction] = useState<RoutePrediction | null>(null);

  useEffect(() => {
    if (!routeId) {
      setLoading(false);
      return;
    }
    const ctrl = new AbortController();
    setLoading(true);
    fetchRouteAggregates(routeId, ctrl.signal)
      .then((ws) => {
        // API returns newest first; flip for chronological chart.
        setWindows([...ws].reverse());
        setError(null);
      })
      .catch((e: unknown) => {
        if ((e as Error)?.name === 'AbortError') return;
        setError((e as Error).message);
      })
      .finally(() => setLoading(false));
    return () => ctrl.abort();
  }, [routeId]);

  useEffect(() => {
    if (!routeId) return;
    const ctrl = new AbortController();
    fetchRoutePrediction(routeId, ctrl.signal)
      .then(setPrediction)
      .catch(() => setPrediction(null));
    const id = setInterval(() => {
      fetchRoutePrediction(routeId).then(setPrediction).catch(() => {});
    }, 60_000);
    return () => { ctrl.abort(); clearInterval(id); };
  }, [routeId]);

  if (!routeId) return <RouteMissing />;

  // Latest window (most recent data point) for the headline summary.
  const latest = windows.length ? windows[windows.length - 1] : null;
  const withDelay = windows.filter((w) => w.avg_delay_seconds !== null);
  const totalVehicles = windows.reduce((acc, w) => acc + (w.vehicle_count ?? 0), 0);

  return (
    <main className="min-h-screen bg-[#0b0d10] text-zinc-100 p-6">
      <div className="mx-auto max-w-4xl">
        <Link href="/" className="text-sm text-blue-400 hover:underline">
          ← back to map
        </Link>
        <h1 className="mt-2 text-3xl font-semibold">
          Route <span className="font-mono">{routeId}</span>
        </h1>
        <p className="mt-1 text-sm text-zinc-400">
          5-minute on-time-performance buckets, newest first.
        </p>

        {loading && <p className="mt-8 text-zinc-400">loading…</p>}
        {error && <p className="mt-8 text-red-400">err: {error}</p>}

        {!loading && !error && windows.length === 0 && (
          <p className="mt-8 text-zinc-400">
            No aggregates for this route yet. Wait a few minutes and refresh.
          </p>
        )}

        {!loading && !error && windows.length > 0 && (
          <>
            <section className="mt-6 grid grid-cols-1 gap-4 sm:grid-cols-3">
              <Stat
                label="latest avg delay"
                value={delayLabel(latest?.avg_delay_seconds ?? null)}
              />
              <Stat
                label="latest on-time %"
                value={
                  latest?.on_time_pct === null || latest?.on_time_pct === undefined
                    ? '—'
                    : `${latest.on_time_pct.toFixed(1)}%`
                }
              />
              <Stat
                label="vehicles seen (window total)"
                value={totalVehicles.toLocaleString()}
              />
            </section>

            {prediction && (
              <section className="mt-6 rounded bg-zinc-900/50 p-4">
                <div className="text-xs uppercase tracking-wide text-zinc-500">trendline</div>
                <div className="mt-1 text-lg">
                  Currently <span className="font-mono">
                    {formatDelay(prediction.current_avg_delay_seconds)}
                  </span>, predicted <span className="font-mono">
                    {formatDelay(prediction.predicted_next_window_avg_delay_seconds)}
                  </span>{' '}
                  <TrendArrow
                    current={prediction.current_avg_delay_seconds}
                    predicted={prediction.predicted_next_window_avg_delay_seconds}
                  />
                </div>
                <div className="mt-1 text-xs text-zinc-500">
                  model {prediction.model_version}
                </div>
              </section>
            )}

            <section className="mt-8">
              <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-zinc-400">
                On-time % over time
              </h2>
              <div className="h-64 w-full rounded bg-zinc-900/50 p-2">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={withDelay} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
                    <CartesianGrid stroke="#27272a" />
                    <XAxis
                      dataKey="window_start_iso"
                      tickFormatter={(v: string) => v.slice(11, 16)}
                      stroke="#a1a1aa"
                      fontSize={11}
                    />
                    <YAxis
                      domain={[0, 100]}
                      stroke="#a1a1aa"
                      fontSize={11}
                      tickFormatter={(v: number) => `${v}%`}
                    />
                    <Tooltip
                      contentStyle={{ background: '#18181b', border: '1px solid #3f3f46' }}
                      formatter={(value) => [
                        typeof value === 'number' ? `${value.toFixed(1)}%` : '—',
                        'on time',
                      ]}
                      labelFormatter={(v) =>
                        typeof v === 'string' ? v.replace('T', ' ').replace('Z', ' UTC') : ''
                      }
                    />
                    <Line
                      type="monotone"
                      dataKey="on_time_pct"
                      stroke="#22c55e"
                      strokeWidth={2}
                      dot={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </section>

            <section className="mt-8">
              <h2 className="mb-2 text-sm font-medium uppercase tracking-wide text-zinc-400">
                Avg delay over time (seconds)
              </h2>
              <div className="h-64 w-full rounded bg-zinc-900/50 p-2">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={withDelay} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
                    <CartesianGrid stroke="#27272a" />
                    <XAxis
                      dataKey="window_start_iso"
                      tickFormatter={(v: string) => v.slice(11, 16)}
                      stroke="#a1a1aa"
                      fontSize={11}
                    />
                    <YAxis stroke="#a1a1aa" fontSize={11} />
                    <Tooltip
                      contentStyle={{ background: '#18181b', border: '1px solid #3f3f46' }}
                      formatter={(value) => [
                        typeof value === 'number' ? `${value}s` : '—',
                        'avg delay',
                      ]}
                    />
                    <Line
                      type="monotone"
                      dataKey="avg_delay_seconds"
                      stroke="#f97316"
                      strokeWidth={2}
                      dot={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </section>
          </>
        )}
      </div>
    </main>
  );
}

function RouteMissing() {
  return (
    <main className="min-h-screen bg-[#0b0d10] text-zinc-100 p-6">
      <div className="mx-auto max-w-4xl">
        <Link href="/" className="text-sm text-blue-400 hover:underline">
          ← back to map
        </Link>
        <h1 className="mt-4 text-2xl font-semibold">No route selected</h1>
        <p className="mt-2 text-zinc-400">
          Open this page from a vehicle pin on the map to see its on-time-performance
          history.
        </p>
      </div>
    </main>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded bg-zinc-900/50 p-4">
      <div className="text-xs uppercase tracking-wide text-zinc-500">{label}</div>
      <div className="mt-1 text-2xl font-semibold">{value}</div>
    </div>
  );
}

function formatDelay(seconds: number): string {
  const sign = seconds > 0 ? '+' : seconds < 0 ? '−' : '';
  const mins = Math.round(Math.abs(seconds) / 60);
  return `${sign}${mins} min`;
}

function TrendArrow({ current, predicted }: { current: number; predicted: number }) {
  const delta = predicted - current;
  if (Math.abs(delta) < 30) return <span aria-label="steady">→</span>;  // <30s = flat
  const up = delta > 0;
  return (
    <span
      className={up ? 'text-red-400' : 'text-emerald-400'}
      aria-label={up ? 'worsening' : 'improving'}
    >
      {up ? '↑' : '↓'}
    </span>
  );
}

// useSearchParams() requires a Suspense boundary in Next.js 14 App Router.
export default function RoutePage() {
  return (
    <Suspense fallback={<div className="p-6 text-zinc-400">loading…</div>}>
      <RoutePageInner />
    </Suspense>
  );
}
