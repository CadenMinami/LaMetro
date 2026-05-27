import { fetchAuthSession } from 'aws-amplify/auth';

export interface Geofence {
  user_id: string;
  geofence_id: string;
  route_id: string;
  stop_id: string | null;
  threshold_seconds: number;
  label: string;
  enabled: boolean;
  created_at: string;
}

export interface AppNotification {
  id: string;
  route_id: string;
  delay_seconds: number;
  threshold_seconds: number;
  message: string;
  read: boolean;
  created_at: string;
}

export interface Me {
  user_id: string;
  email: string;
  email_alerts_enabled: boolean;
  home_routes: string[];
}

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL;

function base(): string {
  if (!API_BASE_URL) throw new Error('NEXT_PUBLIC_API_BASE_URL not set');
  return API_BASE_URL.replace(/\/$/, '');
}

async function authHeaders(): Promise<HeadersInit> {
  const session = await fetchAuthSession();
  const token = session.tokens?.idToken?.toString();
  if (!token) throw new Error('not authenticated');
  return { Authorization: token, 'Content-Type': 'application/json' };
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const res = await fetch(`${base()}${path}`, {
    ...init,
    headers: { ...(await authHeaders()), ...(init.headers ?? {}) },
    cache: 'no-store',
  });
  if (res.status === 204) return undefined as T;
  if (!res.ok) throw new Error(`API ${res.status}`);
  return (await res.json()) as T;
}

export async function listGeofences(): Promise<Geofence[]> {
  const body = await request<{ geofences: Geofence[] }>('/geofences');
  return body.geofences ?? [];
}

export async function createGeofence(input: {
  route_id: string;
  threshold_seconds: number;
  label?: string;
}): Promise<Geofence> {
  const body = await request<{ geofence: Geofence }>('/geofences', {
    method: 'POST',
    body: JSON.stringify(input),
  });
  return body.geofence;
}

export async function deleteGeofence(geofenceId: string): Promise<void> {
  await request<void>(`/geofences/${encodeURIComponent(geofenceId)}`, { method: 'DELETE' });
}

export async function listNotifications(): Promise<{ unread_count: number; notifications: AppNotification[] }> {
  return request('/notifications');
}

export async function markNotificationRead(id: string): Promise<void> {
  await request(`/notifications/${encodeURIComponent(id)}`, { method: 'PATCH' });
}

export async function getMe(): Promise<Me> {
  return request<Me>('/me');
}

export async function updateEmailAlerts(enabled: boolean): Promise<void> {
  await request('/me', { method: 'PUT', body: JSON.stringify({ email_alerts_enabled: enabled }) });
}
