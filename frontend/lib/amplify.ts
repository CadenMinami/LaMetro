import { Amplify } from 'aws-amplify';

// Configured from build-time env (CloudFront serves a static export, so these
// are inlined at `npm run build`). Set them in frontend/.env.local for local
// dev and as repo/CI env for the deployed build.
const userPoolId = process.env.NEXT_PUBLIC_COGNITO_USER_POOL_ID;
const userPoolClientId = process.env.NEXT_PUBLIC_COGNITO_CLIENT_ID;

let configured = false;

export function configureAmplify(): void {
  if (configured || !userPoolId || !userPoolClientId) return;
  Amplify.configure({
    Auth: {
      Cognito: {
        userPoolId,
        userPoolClientId,
      },
    },
  });
  configured = true;
}

export function isAuthConfigured(): boolean {
  return Boolean(userPoolId && userPoolClientId);
}
