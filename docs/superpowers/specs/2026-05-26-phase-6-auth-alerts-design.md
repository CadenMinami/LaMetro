# Phase 6 — Auth + In-App Alerts — Design

**Date:** 2026-05-26
**Status:** Approved (design), pending implementation plan
**Phase:** 6 of the LA Metro Reliability Platform build sequence

## Goal

Let users sign up, define geofences on the routes they care about ("alert me when
route 720 is running late"), and receive **in-app notifications** when those routes
breach a delay threshold. Auth exists in this platform *to enable* per-user
geofences — it is not login-for-its-own-sake.

## Key decisions (and the alternatives rejected)

1. **Auth UX → Amplify Authenticator component** (`@aws-amplify/ui-react`).
   In-app, no redirect, themeable to existing Tailwind tokens. ~1 day of work.
   - *Rejected:* Cognito Hosted UI (redirects away from the polished app, generic
     styling); fully custom forms (most polish but most edge cases — confirmation
     codes, resend, error states — for little marginal value).

2. **Geofence evaluation → route-level threshold**, reusing `route-aggregates`.
   A geofence fires when the route's current `avg_delay_seconds` exceeds the
   geofence's `threshold_seconds`. The Aggregation Lambda already computes this
   every minute, so the check is a cheap join over existing data.
   - `stop_id` is kept in the schema (nullable) as a clean upgrade path to per-stop
     directional geofences in v2.
   - *Rejected:* per-vehicle-on-route (more logic, still no direction); per-stop
     directional (needs upstream/downstream position along the shape + `direction_id`
     matching — significant new algorithm work, over-engineering for this phase).

3. **In-app delivery → `notifications` DynamoDB table + REST poll.**
   Aggregation Lambda writes a breach row; the frontend bell polls `GET /notifications`
   every ~60s. Breaches are minute-granular anyway, so ~60s latency loses nothing.
   - *Rejected:* authenticated WebSocket push (better live demo, but requires adding a
     Cognito authorizer to `$connect` and a persistence path — not worth it when the
     poll path is this cheap and the data updates per-minute).

4. **Channel model → in-app primary, email opt-in (stubbed this phase).**
   The "email me too" toggle is built and persisted on the user record, but no email
   is sent yet. In-app notifications are the only live channel. SES email sending is
   explicit future work.
   - *Why:* in-app demos better (a bell lighting up live in the dashboard beats
     screenshotting an inbox) and sidesteps the SES sandbox / SNS subscription-
     confirmation friction entirely. The toggle is ready to flip on later.
   - *Rejected:* SNS → notifier Lambda → SES now; direct SNS email subscription;
     direct SES from the Aggregation Lambda — all deferred.

## Architecture

No SES, no SNS, and no WebSocket changes in this phase.

### New / changed components

| Component | Type | Role |
|---|---|---|
| `AuthStack` | new CDK stack | Cognito user pool + app client; **PostConfirmation Lambda trigger** that seeds a `users` row. |
| `users`, `geofences`, `notifications` | new DynamoDB tables in `StorageStack` | Per-user hot state; same on-demand + TTL pattern as existing tables. |
| `user-api` Lambda | new, **authenticated** | CRUD for `/geofences`, `/notifications`, `/me`. Kept separate from the public `query-api` so the public/private boundary stays clean. |
| Aggregation Lambda | extend | After writing route-aggregates, evaluate geofences and write notifications. No new schedule or compute path. |
| Frontend | extend | Amplify Authenticator, "My Routes" account page, notification bell. |

### PostConfirmation trigger

Standard AWS pattern: Cognito owns identity, the app's `users` table owns app data.
When a user confirms signup, the trigger writes their `users` row
(`user_id` = Cognito sub, `email`, `created_at`, `email_alerts_enabled=false`).

## Data model

**`users`** — PK `user_id` (Cognito sub)
- `email`, `created_at`, `email_alerts_enabled` (bool, default `false`), `home_routes` (list)

**`geofences`** — PK `user_id`, SK `geofence_id`
- `route_id`, `stop_id` (nullable — reserved for v2 per-stop upgrade),
  `threshold_seconds`, `label`, `enabled` (bool), `created_at`, `last_alerted_epoch`
- **GSI `route_id → user_id`** — answers "who cares about route X?" in one query
  during geofence evaluation.

**`notifications`** — PK `user_id`, SK `created_at_iso`
- `route_id`, `delay_seconds`, `threshold_seconds`, `message`, `read` (bool),
  `ttl_epoch` (~7-day TTL)

## Geofence evaluation + de-duplication

Each minute, after route-aggregates are written, for every route that received an
aggregate this cycle:

1. Query the `geofences` GSI by `route_id` → all `enabled` geofences on that route.
2. For each, compare the route's current `avg_delay_seconds` against that geofence's
   `threshold_seconds`.
3. If exceeded **AND** `now - last_alerted_epoch > COOLDOWN` → write a `notifications`
   row and update `last_alerted_epoch`.

- **Metric:** `avg_delay_seconds` (the natural "is the route late right now" signal).
- **Cooldown:** 15 minutes (level-triggered with cooldown). A route late for an hour
  yields ~4 alerts, not ~60. Simpler than edge-triggered (tracking on-time→late
  transitions) and good enough.

## API surface

Existing `/vehicles` and `/routes/*` stay **public**. New routes sit behind a
**Cognito User Pool authorizer** on API Gateway:

- `GET / POST / DELETE /geofences` — manage the caller's geofences
- `GET /notifications` + `PATCH /notifications/{id}` (mark read)
- `GET / PUT /me` — read/update the `email_alerts_enabled` toggle

The frontend attaches the Cognito JWT (`Authorization` header) to these calls;
Amplify supplies the token.

## Frontend

- **Auth:** wrap the app, configure Amplify with the user pool, drop in
  `<Authenticator>` themed to existing Tailwind tokens.
- **"My Routes" page:** list geofences; add one via the existing searchable route
  picker + a threshold dropdown (3 / 5 / 10 min); enable / disable / delete; the
  email-alerts toggle (persists to `/me`, no sending yet).
- **Notification bell:** polls `GET /notifications` every ~60s, shows unread count,
  dropdown list, mark-as-read.

## Testing

- **pytest + moto** for new Lambda logic: geofence evaluation, cooldown suppression,
  `user-api` CRUD, PostConfirmation seeding.
- Frontend auth flows are awkward to fully automate; keep Playwright smoke coverage
  light here (the auth gate complicates the existing public smoke test).

## Cost

- Cognito: free to 50k MAU.
- 3 small on-demand DynamoDB tables.
- Negligible extra Lambda invocation (geofence eval rides the existing 1-min cycle).
- **No new always-on cost.** Stays well under the $30/month budget.

## Out of scope (→ README "future work")

- Per-stop directional geofences (the `stop_id` field is reserved and ready).
- SES email sending (the toggle is built and persisted; just not wired to delivery).
- Real-time WebSocket push of alerts (poll path is sufficient at minute granularity).
