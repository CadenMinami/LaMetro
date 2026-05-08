'use client';

import { useEffect, useRef, useState } from 'react';
import { fetchStopArrivals } from '@/lib/api';
import { delayColor, delayLabel, routeColor } from '@/lib/colors';
import type { Arrival, Stop, StopArrivalsResponse } from '@/types/stop';

interface Props {
  stopId: string;
  // The stop metadata from the cached stops list. Optional because the
  // panel can also render before the cache hydrates (we'll show whatever
  // the API echoes back).
  stop: Stop | null;
  onClose: () => void;
}

const POLL_MS = 15_000;
const DEFAULT_LIMIT = 5;
const EXPANDED_LIMIT = 10;

export function StopArrivalsPanel({ stopId, stop, onClose }: Props) {
  const [data, setData] = useState<StopArrivalsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [limit, setLimit] = useState(DEFAULT_LIMIT);
  // Tracks which limit we last requested so the show-more button can
  // re-fetch with the bigger window without waiting for the next poll.
  const requestedLimitRef = useRef(DEFAULT_LIMIT);

  // Re-fetch on stop change OR limit change, then poll every 15 s.
  useEffect(() => {
    let cancelled = false;
    const ctrl = new AbortController();
    requestedLimitRef.current = limit;

    const tick = async () => {
      try {
        const resp = await fetchStopArrivals(stopId, {
          limit,
          signal: ctrl.signal,
        });
        if (cancelled) return;
        setData(resp);
        setError(null);
      } catch (err) {
        if ((err as Error).name === 'AbortError') return;
        setError((err as Error).message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    setLoading(true);
    tick();
    const id = setInterval(tick, POLL_MS);
    return () => {
      cancelled = true;
      ctrl.abort();
      clearInterval(id);
    };
  }, [stopId, limit]);

  // Reset the expanded limit when the user opens a different stop.
  useEffect(() => {
    setLimit(DEFAULT_LIMIT);
    setData(null);
  }, [stopId]);

  const headerName = data?.stop_name || stop?.name || 'Stop';
  const arrivals = data?.arrivals ?? [];

  return (
    <aside
      className="absolute right-0 top-0 z-10 flex h-full w-[340px] flex-col bg-zinc-950/95 text-zinc-100 shadow-2xl backdrop-blur-sm"
      role="dialog"
      aria-label={`Arrivals at ${headerName}`}
    >
      <header className="flex items-start justify-between border-b border-white/10 px-4 py-3">
        <div>
          <div className="text-xs uppercase tracking-wide text-zinc-400">Stop {stopId}</div>
          <div className="text-base font-semibold leading-tight">{headerName}</div>
          {stop?.routes?.length ? (
            <div className="mt-1 flex flex-wrap gap-1">
              {stop.routes.slice(0, 8).map((r) => (
                <span
                  key={r}
                  className="rounded-full px-2 py-0.5 text-[10px] font-semibold text-black"
                  style={{ background: routeColor(r) }}
                >
                  {r}
                </span>
              ))}
            </div>
          ) : null}
        </div>
        <button
          type="button"
          onClick={onClose}
          aria-label="Close arrivals panel"
          className="rounded p-1 text-zinc-400 hover:bg-white/10 hover:text-white"
        >
          ✕
        </button>
      </header>

      <div className="flex-1 overflow-y-auto">
        {loading && !data ? (
          <ArrivalsSkeleton />
        ) : error ? (
          <div className="px-4 py-6 text-sm text-red-400">
            Couldn’t load arrivals — {error}
          </div>
        ) : arrivals.length === 0 ? (
          <div className="px-4 py-8 text-sm text-zinc-400">
            No upcoming arrivals in the next {data?.horizon_minutes ?? 60} minutes.
          </div>
        ) : (
          <ul className="divide-y divide-white/5">
            {arrivals.map((a) => (
              <ArrivalRow key={`${a.trip_id}:${a.scheduled_arrival}`} a={a} />
            ))}
          </ul>
        )}
      </div>

      {arrivals.length > 0 && limit < EXPANDED_LIMIT && (
        <button
          type="button"
          onClick={() => setLimit(EXPANDED_LIMIT)}
          className="border-t border-white/10 px-4 py-2 text-center text-xs text-zinc-400 hover:bg-white/5 hover:text-white"
        >
          Show more
        </button>
      )}
      {data?.as_of && (
        <footer className="border-t border-white/10 px-4 py-2 text-[10px] text-zinc-500">
          Updated {new Date(data.as_of).toLocaleTimeString()}
        </footer>
      )}
    </aside>
  );
}

function ArrivalRow({ a }: { a: Arrival }) {
  const dColor = delayColor(a.delay_seconds);
  const statusLabel =
    a.status === 'live' ? delayLabel(a.delay_seconds) :
    a.status === 'due' ? 'Due' :
    a.status === 'departed' ? 'Departed' :
    'Scheduled';
  const minutesText = a.predicted_minutes <= 0 ? 'now' : `${a.predicted_minutes} min`;

  return (
    <li className="flex items-center gap-3 px-4 py-3">
      <span
        className="flex min-w-[44px] items-center justify-center rounded px-2 py-1 text-xs font-bold text-black"
        style={{ background: routeColor(a.route_id) }}
      >
        {a.route_id || '—'}
      </span>
      <div className="flex-1">
        <div className="flex items-baseline justify-between gap-2">
          <span className="text-base font-semibold tabular-nums">{minutesText}</span>
          <span className="text-[10px] uppercase tracking-wide text-zinc-500">
            {new Date(a.predicted_arrival).toLocaleTimeString([], {
              hour: 'numeric', minute: '2-digit',
            })}
          </span>
        </div>
        <div className="flex items-center gap-2 text-xs">
          <span
            className="inline-block h-2 w-2 rounded-full"
            style={{ background: dColor ?? (a.status === 'scheduled' ? '#6b7280' : '#9ca3af') }}
            aria-hidden
          />
          <span className="text-zinc-300">{statusLabel}</span>
        </div>
      </div>
    </li>
  );
}

function ArrivalsSkeleton() {
  return (
    <ul className="divide-y divide-white/5">
      {[0, 1, 2, 3, 4].map((i) => (
        <li key={i} className="flex items-center gap-3 px-4 py-3">
          <div className="h-6 w-11 animate-pulse rounded bg-white/10" />
          <div className="flex-1 space-y-2">
            <div className="h-4 w-1/2 animate-pulse rounded bg-white/10" />
            <div className="h-3 w-1/3 animate-pulse rounded bg-white/10" />
          </div>
        </li>
      ))}
    </ul>
  );
}
