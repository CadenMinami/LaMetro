'use client';

import type { Vehicle } from '@/types/vehicle';
import type { BBox } from './api';

const WS_URL = process.env.NEXT_PUBLIC_WS_URL;

export interface PositionsMessage {
  type: 'positions';
  vehicles: Vehicle[];
}

export type SocketMessage = PositionsMessage;

export interface VehicleSocketHandle {
  /** Replace the active subscription. Cheap to call repeatedly. */
  setBBox: (bbox: BBox, routeId?: string) => void;
  /** Tear down the connection — also clears any pending reconnect timer. */
  close: () => void;
}

interface VehicleSocketOptions {
  onMessage: (msg: SocketMessage) => void;
  onStateChange?: (state: 'connecting' | 'open' | 'closed') => void;
}

/**
 * Open a single long-lived WebSocket. Re-sends `subscribe` whenever the
 * bbox changes, and reconnects with exponential backoff on transient
 * failures. Designed to be created once per component lifecycle (i.e. in
 * a useEffect mount) and torn down with `close()` on unmount.
 *
 * The server side echoes nothing back on `subscribe`'s success, so there's
 * no ack to wait on — callers can call `setBBox` immediately after open.
 *
 * If `NEXT_PUBLIC_WS_URL` isn't set (e.g. local dev with no backend),
 * `openVehicleSocket` returns a no-op handle so the calling component can
 * fall back to polling without conditionals.
 */
export function openVehicleSocket(opts: VehicleSocketOptions): VehicleSocketHandle {
  if (!WS_URL) {
    return { setBBox: () => {}, close: () => {} };
  }

  let ws: WebSocket | null = null;
  let pendingBBox: BBox | null = null;
  let pendingRoute: string | undefined;
  let closed = false;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let backoffMs = 500; // doubles up to MAX_BACKOFF on each failure

  const MAX_BACKOFF = 30_000;

  function connect() {
    if (closed) return;
    opts.onStateChange?.('connecting');
    ws = new WebSocket(WS_URL!);

    ws.addEventListener('open', () => {
      backoffMs = 500;
      opts.onStateChange?.('open');
      // Re-arm subscription on every (re)connect so we never end up with
      // an open socket that's not receiving anything.
      if (pendingBBox) sendSubscribe(pendingBBox, pendingRoute);
    });

    ws.addEventListener('message', (ev) => {
      try {
        const data = JSON.parse(ev.data);
        if (data && data.type === 'positions' && Array.isArray(data.vehicles)) {
          opts.onMessage(data as PositionsMessage);
        }
      } catch {
        // Malformed frame — ignore. The server only sends valid JSON.
      }
    });

    ws.addEventListener('close', () => {
      opts.onStateChange?.('closed');
      ws = null;
      if (closed) return;
      // Reconnect with capped exponential backoff.
      reconnectTimer = setTimeout(connect, backoffMs);
      backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF);
    });

    ws.addEventListener('error', () => {
      // The browser fires `error` then `close`; we handle reconnect there.
      // Swallow here so it doesn't bubble to console as unhandled.
    });
  }

  function sendSubscribe(bbox: BBox, routeId?: string) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    ws.send(
      JSON.stringify({
        action: 'subscribe',
        bbox,
        route_id: routeId,
      }),
    );
  }

  connect();

  return {
    setBBox(bbox: BBox, routeId?: string) {
      pendingBBox = bbox;
      pendingRoute = routeId;
      sendSubscribe(bbox, routeId);
    },
    close() {
      closed = true;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      ws?.close();
    },
  };
}
