'use client';

import { useEffect } from 'react';
import { Authenticator } from '@aws-amplify/ui-react';
import '@aws-amplify/ui-react/styles.css';
import { configureAmplify, isAuthConfigured } from '@/lib/amplify';

/**
 * Wraps children in the Amplify Authenticator. Used only on authenticated
 * pages (e.g. /account). The public map never mounts this. Themed lightly to
 * sit on the app's dark background; full token theming can come later.
 */
export function AuthGate({ children }: { children: React.ReactNode }) {
  useEffect(() => {
    configureAmplify();
  }, []);

  if (!isAuthConfigured()) {
    return (
      <main className="min-h-screen bg-[#0b0d10] text-zinc-100 p-6">
        <p className="text-zinc-400">
          Auth is not configured. Set NEXT_PUBLIC_COGNITO_USER_POOL_ID and
          NEXT_PUBLIC_COGNITO_CLIENT_ID.
        </p>
      </main>
    );
  }

  return (
    <main className="min-h-screen bg-[#0b0d10] text-zinc-100">
      <div data-amplify-theme="la-metro" className="mx-auto max-w-4xl p-6">
        <Authenticator signUpAttributes={['email']}>
          {() => <>{children}</>}
        </Authenticator>
      </div>
    </main>
  );
}
