'use client';

import { useCallback, useEffect, useState } from 'react';
import Link from 'next/link';
import { AuthGate } from '@/components/AuthGate';
import {
  listGeofences,
  createGeofence,
  deleteGeofence,
  getMe,
  updateEmailAlerts,
  type Geofence,
} from '@/lib/user-api';

const THRESHOLDS = [
  { label: '3 min', seconds: 180 },
  { label: '5 min', seconds: 300 },
  { label: '10 min', seconds: 600 },
];

function AccountInner() {
  const [geofences, setGeofences] = useState<Geofence[]>([]);
  const [routeId, setRouteId] = useState('');
  const [threshold, setThreshold] = useState(300);
  const [emailAlerts, setEmailAlerts] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const refresh = useCallback(async () => {
    try {
      const [gfs, me] = await Promise.all([listGeofences(), getMe()]);
      setGeofences(gfs);
      setEmailAlerts(me.email_alerts_enabled);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  async function onAdd(e: React.FormEvent) {
    e.preventDefault();
    if (!routeId.trim()) return;
    setBusy(true);
    try {
      await createGeofence({ route_id: routeId.trim(), threshold_seconds: threshold });
      setRouteId('');
      await refresh();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  async function onDelete(id: string) {
    await deleteGeofence(id);
    await refresh();
  }

  async function onToggleEmail() {
    const next = !emailAlerts;
    setEmailAlerts(next);
    try {
      await updateEmailAlerts(next);
    } catch (e) {
      setEmailAlerts(!next); // revert on failure
      setError((e as Error).message);
    }
  }

  return (
    <div className="text-zinc-100">
      <Link href="/" className="text-sm text-blue-400 hover:underline">← back to map</Link>
      <h1 className="mt-2 text-3xl font-semibold">My Routes</h1>
      <p className="mt-1 text-sm text-zinc-400">
        Get an in-app alert when a route&apos;s average delay crosses your threshold.
      </p>

      {error && <p className="mt-4 text-red-400">err: {error}</p>}

      <form onSubmit={onAdd} className="mt-6 flex flex-wrap items-end gap-3 rounded bg-zinc-900/50 p-4">
        <label className="flex flex-col text-xs uppercase tracking-wide text-zinc-500">
          Route
          <input
            value={routeId}
            onChange={(e) => setRouteId(e.target.value)}
            placeholder="e.g. 720"
            className="mt-1 w-28 rounded bg-zinc-800 px-2 py-1 font-mono text-base text-zinc-100"
          />
        </label>
        <label className="flex flex-col text-xs uppercase tracking-wide text-zinc-500">
          Alert when late by
          <select
            value={threshold}
            onChange={(e) => setThreshold(Number(e.target.value))}
            className="mt-1 rounded bg-zinc-800 px-2 py-1 text-base text-zinc-100"
          >
            {THRESHOLDS.map((t) => (
              <option key={t.seconds} value={t.seconds}>{t.label}</option>
            ))}
          </select>
        </label>
        <button
          type="submit"
          disabled={busy || !routeId.trim()}
          className="rounded bg-blue-600 px-4 py-1.5 text-sm font-medium hover:bg-blue-500 disabled:opacity-50"
        >
          Add geofence
        </button>
      </form>

      <ul className="mt-6 space-y-2">
        {geofences.length === 0 && (
          <li className="text-zinc-400">No geofences yet. Add one above.</li>
        )}
        {geofences.map((gf) => (
          <li key={gf.geofence_id} className="flex items-center justify-between rounded bg-zinc-900/50 px-4 py-3">
            <span>
              Route <span className="font-mono">{gf.route_id}</span>
              <span className="ml-2 text-zinc-400">› alert at {Math.round(gf.threshold_seconds / 60)} min late</span>
            </span>
            <button onClick={() => onDelete(gf.geofence_id)} className="text-sm text-red-400 hover:underline">
              remove
            </button>
          </li>
        ))}
      </ul>

      <label className="mt-8 flex items-center gap-3 text-sm">
        <input type="checkbox" checked={emailAlerts} onChange={onToggleEmail} className="h-4 w-4" />
        Also email me when a geofence fires
        <span className="text-xs text-zinc-500">(coming soon — preference saved)</span>
      </label>
    </div>
  );
}

export default function AccountPage() {
  return (
    <AuthGate>
      <AccountInner />
    </AuthGate>
  );
}
