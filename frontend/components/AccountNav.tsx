'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { getCurrentUser } from 'aws-amplify/auth';
import { configureAmplify, isAuthConfigured } from '@/lib/amplify';
import { NotificationBell } from './NotificationBell';

/**
 * Floating top-right control on the map. Shows the notification bell + a "My
 * Routes" link when signed in, or a "Sign in" link otherwise. Kept out of the
 * map's layout flow so it doesn't disturb MetroMap.
 */
export function AccountNav() {
  const [signedIn, setSignedIn] = useState(false);

  useEffect(() => {
    if (!isAuthConfigured()) return;
    configureAmplify();
    getCurrentUser()
      .then(() => setSignedIn(true))
      .catch(() => setSignedIn(false));
  }, []);

  if (!isAuthConfigured()) return null;

  return (
    <div className="pointer-events-auto absolute right-4 top-4 z-[1000] flex items-center gap-3">
      {signedIn ? (
        <>
          <NotificationBell />
          <Link
            href="/account"
            className="rounded-full bg-zinc-900/80 px-3 py-2 text-sm text-zinc-200 ring-1 ring-zinc-700 hover:bg-zinc-800"
          >
            My Routes
          </Link>
        </>
      ) : (
        <Link
          href="/account"
          className="rounded-full bg-zinc-900/80 px-3 py-2 text-sm text-zinc-200 ring-1 ring-zinc-700 hover:bg-zinc-800"
        >
          Sign in
        </Link>
      )}
    </div>
  );
}
