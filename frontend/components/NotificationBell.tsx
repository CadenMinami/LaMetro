'use client';

import { useEffect, useState } from 'react';
import { listNotifications, markNotificationRead, type AppNotification } from '@/lib/user-api';

const POLL_MS = 60_000;

export function NotificationBell() {
  const [open, setOpen] = useState(false);
  const [items, setItems] = useState<AppNotification[]>([]);
  // Derived, not stored: a single source of truth so the badge can't drift out
  // of sync with the list it's counting.
  const unread = items.filter((n) => !n.read).length;

  useEffect(() => {
    let active = true;
    async function poll() {
      try {
        const { notifications } = await listNotifications();
        if (!active) return;
        setItems(notifications);
      } catch {
        // Silent: bell is best-effort. Auth/network errors just leave it empty.
      }
    }
    poll();
    const id = setInterval(poll, POLL_MS);
    return () => {
      active = false;
      clearInterval(id);
    };
  }, []);

  async function onToggle() {
    const opening = !open;
    setOpen(opening);
    // Only mark-read when opening the dropdown, not when closing it.
    if (!opening) return;
    const unreadIds = items.filter((n) => !n.read).map((n) => n.id);
    if (unreadIds.length) {
      setItems((prev) => prev.map((n) => ({ ...n, read: true })));
      await Promise.allSettled(unreadIds.map((id) => markNotificationRead(id)));
    }
  }

  return (
    <div className="relative">
      <button
        onClick={onToggle}
        className="relative rounded-full bg-zinc-900/80 p-2 text-zinc-200 ring-1 ring-zinc-700 hover:bg-zinc-800"
        aria-label="Notifications"
      >
        🔔
        {unread > 0 && (
          <span className="absolute -right-1 -top-1 flex h-4 min-w-4 items-center justify-center rounded-full bg-red-500 px-1 text-[10px] font-bold text-white">
            {unread}
          </span>
        )}
      </button>
      {open && (
        <div className="absolute right-0 mt-2 w-80 rounded-lg bg-zinc-900 p-2 text-sm text-zinc-100 shadow-xl ring-1 ring-zinc-700">
          {items.length === 0 ? (
            <p className="p-3 text-zinc-400">No alerts yet.</p>
          ) : (
            <ul className="max-h-80 space-y-1 overflow-y-auto">
              {items.map((n) => (
                <li key={n.id} className="rounded px-3 py-2 hover:bg-zinc-800">
                  <div>{n.message}</div>
                  <div className="mt-0.5 text-xs text-zinc-500">
                    {n.created_at.slice(11, 16)} UTC
                  </div>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
