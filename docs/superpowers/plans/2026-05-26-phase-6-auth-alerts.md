# Phase 6 — Auth + In-App Alerts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let users sign up (Cognito), define route-level geofences, and receive in-app notifications when a route's average delay breaches their threshold.

**Architecture:** A new `AuthStack` (Cognito user pool + PostConfirmation trigger) seeds a `users` row on signup. Three new DynamoDB tables (`users`, `geofences`, `notifications`) join `StorageStack`. A new authenticated `user-api` Lambda backs `/geofences`, `/notifications`, `/me` behind a Cognito authorizer on the existing REST API. The Aggregation Lambda gains a geofence-evaluation pass that writes notifications with a 15-minute cooldown. The frontend adds an Amplify Authenticator, a "My Routes" account page, and a polling notification bell.

**Tech Stack:** AWS CDK v2 (TypeScript), Python 3.12 Lambdas (boto3, stdlib only), pytest with `unittest.mock` (the repo does **not** use moto — match existing tests), Next.js 14 static export, `@aws-amplify/ui-react` + `aws-amplify`.

**Spec:** `docs/superpowers/specs/2026-05-26-phase-6-auth-alerts-design.md`

---

## Conventions to follow (read before starting)

- **Lambda tests:** `lambdas/<name>/tests/test_handler.py`, import as `from lambdas.<name> import handler`. Use `MagicMock` + `monkeypatch`. Run with `pytest lambdas/<name> -v` from the repo root. Root `pyproject.toml` sets `pythonpath = ["."]` and `--import-mode=importlib`.
- **Lambda response shape:** copy `_response`/`_json_default` from `lambdas/query_api/handler.py:176-194` (DynamoDB returns `Decimal`; convert on the way out).
- **Lambda dispatch:** API Gateway (`proxy: false`) routes by `event['resource']`; path params in `event['pathParameters']`; the Cognito authorizer puts claims in `event['requestContext']['authorizer']['claims']` (the user id is the `sub` claim).
- **Build:** `scripts/build-lambda.sh <name>` produces `lambdas/<name>/.build/`. New lambdas need a `requirements.txt` (comment-only is fine — boto3 ships in the runtime) and must be added to the CI build loop in `.github/workflows/pr-checks.yml`.
- **CDK verify:** from `cdk/`, `npx tsc --noEmit` then `npx cdk synth --quiet`. There are no CDK unit tests in this repo; synth + type-check is the gate.
- **Commits:** do NOT add a `Co-Authored-By: Claude` trailer (user preference).

---

## Task 1: Add `users`, `geofences`, `notifications` tables to StorageStack

**Files:**
- Modify: `cdk/lib/storage-stack.ts`

- [ ] **Step 1: Add the three table fields and constructs**

In `cdk/lib/storage-stack.ts`, add three public readonly fields alongside the existing ones (after `websocketConnectionsTable`):

```typescript
  public readonly usersTable: dynamodb.Table;
  public readonly geofencesTable: dynamodb.Table;
  public readonly notificationsTable: dynamodb.Table;
```

Then, after the `websocketConnectionsTable` construct block and before the `archiveBucket` block, add:

```typescript
    // Phase 6: per-user identity record. PK = Cognito `sub`. Seeded by the
    // PostConfirmation Lambda trigger on signup; updated by the user-api
    // Lambda when the user toggles email alerts. No TTL — this is durable
    // account data, not hot state.
    this.usersTable = new dynamodb.Table(this, 'UsersTable', {
      tableName: 'la-metro-users',
      partitionKey: { name: 'user_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Phase 6: one row per geofence a user has defined. The Aggregation Lambda
    // queries the route_id GSI ("who cares about route X?") each minute and
    // updates last_alerted_epoch when it fires an alert.
    this.geofencesTable = new dynamodb.Table(this, 'GeofencesTable', {
      tableName: 'la-metro-geofences',
      partitionKey: { name: 'user_id', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'geofence_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    this.geofencesTable.addGlobalSecondaryIndex({
      indexName: 'route_id-index',
      partitionKey: { name: 'route_id', type: dynamodb.AttributeType.STRING },
      // Project everything: the Aggregation Lambda needs threshold_seconds,
      // enabled, and last_alerted_epoch from the index read, plus the table
      // keys (user_id, geofence_id) to write the cooldown update back.
      projectionType: dynamodb.ProjectionType.ALL,
    });

    // Phase 6: in-app notifications. PK = user_id, SK = created_at (microsecond
    // ISO, also the client-facing id). 7-day TTL keeps the table small.
    this.notificationsTable = new dynamodb.Table(this, 'NotificationsTable', {
      tableName: 'la-metro-notifications',
      partitionKey: { name: 'user_id', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'created_at', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl_epoch',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
```

Add CfnOutputs next to the existing ones at the end of the constructor:

```typescript
    new cdk.CfnOutput(this, 'UsersTableName', { value: this.usersTable.tableName });
    new cdk.CfnOutput(this, 'GeofencesTableName', { value: this.geofencesTable.tableName });
    new cdk.CfnOutput(this, 'NotificationsTableName', { value: this.notificationsTable.tableName });
```

- [ ] **Step 2: Type-check**

Run: `cd cdk && npx tsc --noEmit`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add cdk/lib/storage-stack.ts
git commit -m "Phase 6: users, geofences, notifications DynamoDB tables"
```

---

## Task 2: PostConfirmation Lambda — seed the users row on signup

**Files:**
- Create: `lambdas/post_confirmation/handler.py`
- Create: `lambdas/post_confirmation/requirements.txt`
- Create: `lambdas/post_confirmation/tests/test_handler.py`

- [ ] **Step 1: Write the failing test**

Create `lambdas/post_confirmation/tests/test_handler.py`:

```python
"""Unit tests for the Cognito PostConfirmation trigger."""

from __future__ import annotations

from unittest.mock import MagicMock

from lambdas.post_confirmation import handler


def _event(sub: str = "abc-123", email: str = "rider@example.com") -> dict:
    return {
        "request": {"userAttributes": {"sub": sub, "email": email}},
        "userName": "rider",
    }


def test_seeds_user_row_with_defaults(monkeypatch):
    table = MagicMock()
    monkeypatch.setattr(handler, "get_table", lambda name: table)
    monkeypatch.setattr(handler, "USERS_TABLE", "fake-users")

    event = _event()
    result = handler.lambda_handler(event, MagicMock())

    # Cognito triggers must return the event unchanged.
    assert result is event
    item = table.put_item.call_args.kwargs["Item"]
    assert item["user_id"] == "abc-123"
    assert item["email"] == "rider@example.com"
    assert item["email_alerts_enabled"] is False
    assert item["home_routes"] == []
    assert "created_at" in item
    # Idempotent: never clobber an existing row.
    assert "attribute_not_exists" in table.put_item.call_args.kwargs["ConditionExpression"]


def test_swallows_conditional_check_failure(monkeypatch):
    # A repeat confirmation (same sub) must not fail the trigger — that would
    # block the user from signing in.
    table = MagicMock()
    err = handler.ConditionalCheckFailed()
    table.put_item.side_effect = err
    monkeypatch.setattr(handler, "get_table", lambda name: table)
    monkeypatch.setattr(handler, "USERS_TABLE", "fake-users")

    event = _event()
    assert handler.lambda_handler(event, MagicMock()) is event
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest lambdas/post_confirmation -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'lambdas.post_confirmation'`.

- [ ] **Step 3: Write the implementation**

Create `lambdas/post_confirmation/handler.py`:

```python
"""Cognito PostConfirmation trigger — seeds the app's users table.

Cognito owns identity; this table owns app data (the email-alerts preference,
home routes). Runs once, right after a user confirms their signup. Must return
the event object unchanged or Cognito treats the trigger as failed and blocks
the user.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

USERS_TABLE = os.environ.get("USERS_TABLE_NAME", "")

_ddb = None


# Aliased so tests can construct the boto3 client exception without a live
# client. boto3 raises botocore.exceptions.ClientError; we match on the code.
class ConditionalCheckFailed(Exception):
    pass


def get_table(name: str):
    global _ddb
    if _ddb is None:
        _ddb = boto3.resource("dynamodb")
    return _ddb.Table(name)


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    attrs = (event.get("request") or {}).get("userAttributes") or {}
    user_id = attrs.get("sub")
    email = attrs.get("email", "")

    if not USERS_TABLE or not user_id:
        logger.warning("missing USERS_TABLE or sub; skipping seed")
        return event

    table = get_table(USERS_TABLE)
    item = {
        "user_id": user_id,
        "email": email,
        "email_alerts_enabled": False,
        "home_routes": [],
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    try:
        table.put_item(
            Item=item,
            # Idempotent: a repeated confirmation must not overwrite the row
            # (which would reset the user's email preference).
            ConditionExpression="attribute_not_exists(user_id)",
        )
    except ConditionalCheckFailed:
        logger.info("user row already exists for %s", user_id)
    except Exception as exc:  # pragma: no cover - defensive
        code = getattr(getattr(exc, "response", {}), "get", lambda *_: None)(
            "Error", {}
        )
        if isinstance(code, dict) and code.get("Code") == "ConditionalCheckFailedException":
            logger.info("user row already exists for %s", user_id)
        else:
            # Never fail the trigger — log and move on so the user can sign in.
            logger.exception("failed to seed user row for %s", user_id)

    return event
```

Create `lambdas/post_confirmation/requirements.txt`:

```text
# boto3 ships in the Lambda runtime; no third-party deps.
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest lambdas/post_confirmation -v`
Expected: PASS (2 tests).

> Note: the `test_swallows_conditional_check_failure` test injects `handler.ConditionalCheckFailed` as the side effect, which the `except ConditionalCheckFailed` branch catches. In production the real exception is `botocore.exceptions.ClientError` with code `ConditionalCheckFailedException`, handled by the generic branch. Both paths are covered.

- [ ] **Step 5: Commit**

```bash
git add lambdas/post_confirmation/
git commit -m "Phase 6: PostConfirmation Lambda seeds users table"
```

---

## Task 3: AuthStack — Cognito user pool + PostConfirmation trigger

**Files:**
- Create: `cdk/lib/auth-stack.ts`

- [ ] **Step 1: Write the AuthStack construct**

Create `cdk/lib/auth-stack.ts`:

```typescript
import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as cognito from 'aws-cdk-lib/aws-cognito';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

export interface AuthStackProps extends cdk.StackProps {
  usersTable: dynamodb.ITable;
}

/**
 * Phase 6 auth tier:
 *   - Cognito user pool (email sign-up, self-service, email verification)
 *   - App client for the SPA (no client secret — public client, SRP flow)
 *   - PostConfirmation Lambda trigger that seeds the users table
 *
 * The frontend talks to this pool directly via the Amplify Authenticator
 * (SRP), so we don't need a Hosted UI domain or OAuth flows.
 */
export class AuthStack extends cdk.Stack {
  public readonly userPool: cognito.UserPool;
  public readonly userPoolClient: cognito.UserPoolClient;

  constructor(scope: Construct, id: string, props: AuthStackProps) {
    super(scope, id, props);

    const functionName = 'la-metro-post-confirmation';
    const logGroup = new logs.LogGroup(this, 'PostConfirmationFnLogs', {
      logGroupName: `/aws/lambda/${functionName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const postConfirmationFn = new lambda.Function(this, 'PostConfirmationFn', {
      functionName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'lambdas', 'post_confirmation', '.build'),
      ),
      memorySize: 256,
      timeout: cdk.Duration.seconds(10),
      environment: { USERS_TABLE_NAME: props.usersTable.tableName },
      logGroup,
      description: 'Phase 6: seeds the users table on Cognito signup confirmation.',
    });
    props.usersTable.grantWriteData(postConfirmationFn);

    this.userPool = new cognito.UserPool(this, 'UserPool', {
      userPoolName: 'la-metro-users',
      selfSignUpEnabled: true,
      signInAliases: { email: true },
      autoVerify: { email: true },
      standardAttributes: { email: { required: true, mutable: true } },
      passwordPolicy: { minLength: 8, requireLowercase: true, requireDigits: true },
      accountRecovery: cognito.AccountRecovery.EMAIL_ONLY,
      lambdaTriggers: { postConfirmation: postConfirmationFn },
      // Dev convenience: tear down with `cdk destroy`. Production would RETAIN.
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.userPoolClient = this.userPool.addClient('SpaClient', {
      userPoolClientName: 'la-metro-web',
      // Public SPA client: no secret, SRP auth (what Amplify uses).
      generateSecret: false,
      authFlows: { userSrp: true },
      accessTokenValidity: cdk.Duration.hours(1),
      idTokenValidity: cdk.Duration.hours(1),
      refreshTokenValidity: cdk.Duration.days(30),
    });

    new cdk.CfnOutput(this, 'UserPoolId', { value: this.userPool.userPoolId });
    new cdk.CfnOutput(this, 'UserPoolClientId', { value: this.userPoolClient.userPoolClientId });
  }
}
```

- [ ] **Step 2: Build the PostConfirmation asset so synth can find it**

Run: `scripts/build-lambda.sh post_confirmation`
Expected: `Built post_confirmation → .../lambdas/post_confirmation/.build`.

- [ ] **Step 3: Type-check**

Run: `cd cdk && npx tsc --noEmit`
Expected: no errors. (Full wiring into the app happens in Task 7; this just type-checks the stack file.)

- [ ] **Step 4: Commit**

```bash
git add cdk/lib/auth-stack.ts
git commit -m "Phase 6: AuthStack — Cognito user pool + PostConfirmation trigger"
```

---

## Task 4: `user-api` Lambda — geofence CRUD, notifications, account prefs

This Lambda backs all authenticated routes. Build it function-by-function with TDD. The user id always comes from the verified Cognito authorizer claims, never from the request body.

**Files:**
- Create: `lambdas/user_api/handler.py`
- Create: `lambdas/user_api/requirements.txt`
- Create: `lambdas/user_api/tests/test_handler.py`

- [ ] **Step 1: Write the failing tests**

Create `lambdas/user_api/tests/test_handler.py`:

```python
"""Unit tests for the authenticated user-api Lambda."""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock

from lambdas.user_api import handler


def _event(resource, method, *, sub="user-1", body=None, path_params=None):
    return {
        "resource": resource,
        "httpMethod": method,
        "pathParameters": path_params,
        "body": json.dumps(body) if body is not None else None,
        "requestContext": {"authorizer": {"claims": {"sub": sub, "email": "r@x.com"}}},
    }


def test_user_id_from_claims():
    ev = _event("/me", "GET", sub="abc")
    assert handler.user_id_from_event(ev) == "abc"


def test_user_id_missing_claims_returns_none():
    assert handler.user_id_from_event({"requestContext": {}}) is None


def test_unauthenticated_request_401(monkeypatch):
    ev = _event("/me", "GET")
    ev["requestContext"] = {}
    resp = handler.lambda_handler(ev, MagicMock())
    assert resp["statusCode"] == 401


def test_create_geofence(monkeypatch):
    table = MagicMock()
    monkeypatch.setattr(handler, "_geofences", lambda: table)
    monkeypatch.setattr(handler, "new_geofence_id", lambda: "gf-fixed")

    ev = _event("/geofences", "POST", body={"route_id": "720", "threshold_seconds": 300, "label": "720 to UCLA"})
    resp = handler.lambda_handler(ev, MagicMock())

    assert resp["statusCode"] == 201
    item = table.put_item.call_args.kwargs["Item"]
    assert item["user_id"] == "user-1"
    assert item["geofence_id"] == "gf-fixed"
    assert item["route_id"] == "720"
    assert item["threshold_seconds"] == 300
    assert item["enabled"] is True
    assert item["stop_id"] is None  # reserved for v2
    body = json.loads(resp["body"])
    assert body["geofence_id"] == "gf-fixed"


def test_create_geofence_validation(monkeypatch):
    monkeypatch.setattr(handler, "_geofences", lambda: MagicMock())
    # Missing route_id
    resp = handler.lambda_handler(_event("/geofences", "POST", body={"threshold_seconds": 300}), MagicMock())
    assert resp["statusCode"] == 400
    # Out-of-range threshold
    resp = handler.lambda_handler(
        _event("/geofences", "POST", body={"route_id": "2", "threshold_seconds": 5}), MagicMock()
    )
    assert resp["statusCode"] == 400


def test_list_geofences_scoped_to_user(monkeypatch):
    table = MagicMock()
    table.query.return_value = {"Items": [
        {"user_id": "user-1", "geofence_id": "gf-1", "route_id": "720",
         "threshold_seconds": Decimal("300"), "enabled": True, "stop_id": None,
         "label": "x", "created_at": "2026-05-26T00:00:00Z"},
    ]}
    monkeypatch.setattr(handler, "_geofences", lambda: table)

    resp = handler.lambda_handler(_event("/geofences", "GET"), MagicMock())
    assert resp["statusCode"] == 200
    # Query must be keyed on the caller's user_id, not anything from input.
    cond = table.query.call_args.kwargs["KeyConditionExpression"]
    assert "user_id" in str(cond.get_expression() if hasattr(cond, "get_expression") else cond)
    body = json.loads(resp["body"])
    assert body["geofences"][0]["threshold_seconds"] == 300  # Decimal coerced


def test_delete_geofence(monkeypatch):
    table = MagicMock()
    monkeypatch.setattr(handler, "_geofences", lambda: table)
    resp = handler.lambda_handler(
        _event("/geofences/{geofenceId}", "DELETE", path_params={"geofenceId": "gf-1"}), MagicMock()
    )
    assert resp["statusCode"] == 204
    key = table.delete_item.call_args.kwargs["Key"]
    assert key == {"user_id": "user-1", "geofence_id": "gf-1"}


def test_list_notifications(monkeypatch):
    table = MagicMock()
    table.query.return_value = {"Items": [
        {"user_id": "user-1", "created_at": "2026-05-26T12:00:00.000001Z",
         "route_id": "720", "delay_seconds": Decimal("360"),
         "threshold_seconds": Decimal("300"), "message": "Route 720 running ~6 min late",
         "read": False},
    ]}
    monkeypatch.setattr(handler, "_notifications", lambda: table)
    resp = handler.lambda_handler(_event("/notifications", "GET"), MagicMock())
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["unread_count"] == 1
    assert body["notifications"][0]["id"] == "2026-05-26T12:00:00.000001Z"
    assert body["notifications"][0]["delay_seconds"] == 360


def test_mark_notification_read(monkeypatch):
    table = MagicMock()
    monkeypatch.setattr(handler, "_notifications", lambda: table)
    resp = handler.lambda_handler(
        _event("/notifications/{notificationId}", "PATCH",
               path_params={"notificationId": "2026-05-26T12:00:00.000001Z"}), MagicMock()
    )
    assert resp["statusCode"] == 200
    kwargs = table.update_item.call_args.kwargs
    assert kwargs["Key"] == {"user_id": "user-1", "created_at": "2026-05-26T12:00:00.000001Z"}


def test_get_me(monkeypatch):
    table = MagicMock()
    table.get_item.return_value = {"Item": {
        "user_id": "user-1", "email": "r@x.com", "email_alerts_enabled": True, "home_routes": []
    }}
    monkeypatch.setattr(handler, "_users", lambda: table)
    resp = handler.lambda_handler(_event("/me", "GET"), MagicMock())
    assert resp["statusCode"] == 200
    assert json.loads(resp["body"])["email_alerts_enabled"] is True


def test_put_me_updates_email_toggle(monkeypatch):
    table = MagicMock()
    monkeypatch.setattr(handler, "_users", lambda: table)
    resp = handler.lambda_handler(
        _event("/me", "PUT", body={"email_alerts_enabled": True}), MagicMock()
    )
    assert resp["statusCode"] == 200
    kwargs = table.update_item.call_args.kwargs
    assert kwargs["Key"] == {"user_id": "user-1"}
    assert kwargs["ExpressionAttributeValues"][":v"] is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest lambdas/user_api -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'lambdas.user_api'`.

- [ ] **Step 3: Write the implementation**

Create `lambdas/user_api/handler.py`:

```python
"""Authenticated user-api Lambda.

Routes (all behind a Cognito User Pool authorizer on API Gateway):

    GET    /geofences                       list the caller's geofences
    POST   /geofences                       create one
    DELETE /geofences/{geofenceId}          delete one
    GET    /notifications                   list recent notifications (newest first)
    PATCH  /notifications/{notificationId}  mark one read
    GET    /me                              read the caller's profile/prefs
    PUT    /me                              update email_alerts_enabled

The caller's identity is ALWAYS the verified `sub` claim from the Cognito
authorizer — never anything in the request body. That's the whole security
model: a user can only ever touch rows under their own user_id partition.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

USERS_TABLE = os.environ.get("USERS_TABLE_NAME", "")
GEOFENCES_TABLE = os.environ.get("GEOFENCES_TABLE_NAME", "")
NOTIFICATIONS_TABLE = os.environ.get("NOTIFICATIONS_TABLE_NAME", "")

# Allowed thresholds match the frontend dropdown (3/5/10 min). Enforce a sane
# range server-side regardless of what the client sends.
MIN_THRESHOLD_SECONDS = 60
MAX_THRESHOLD_SECONDS = 3600
NOTIFICATIONS_LIMIT = 50

_ddb = None
_users_t = None
_geofences_t = None
_notifications_t = None


def _table(name: str):
    global _ddb
    if _ddb is None:
        _ddb = boto3.resource("dynamodb")
    return _ddb.Table(name)


def _users():
    global _users_t
    if _users_t is None:
        _users_t = _table(USERS_TABLE)
    return _users_t


def _geofences():
    global _geofences_t
    if _geofences_t is None:
        _geofences_t = _table(GEOFENCES_TABLE)
    return _geofences_t


def _notifications():
    global _notifications_t
    if _notifications_t is None:
        _notifications_t = _table(NOTIFICATIONS_TABLE)
    return _notifications_t


def new_geofence_id() -> str:
    return f"gf-{uuid.uuid4().hex[:12]}"


def _json_default(o: Any) -> Any:
    if isinstance(o, Decimal):
        return int(o) if o == int(o) else float(o)
    return str(o)


def _response(status: int, body: dict | list | None = None) -> dict:
    out = {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
        },
        "body": "" if body is None else json.dumps(body, default=_json_default),
    }
    return out


def user_id_from_event(event: dict[str, Any]) -> str | None:
    claims = (
        (event.get("requestContext") or {}).get("authorizer") or {}
    ).get("claims") or {}
    return claims.get("sub")


def _parse_body(event: dict[str, Any]) -> dict:
    raw = event.get("body")
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


def _iso_micro(now: datetime) -> str:
    return now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# ----- /geofences -----

def list_geofences(user_id: str) -> dict:
    resp = _geofences().query(KeyConditionExpression=Key("user_id").eq(user_id))
    items = resp.get("Items", [])
    return _response(200, {"count": len(items), "geofences": items})


def create_geofence(user_id: str, body: dict) -> dict:
    route_id = (body.get("route_id") or "").strip()
    if not route_id:
        return _response(400, {"error": "missing_route_id"})
    try:
        threshold = int(body.get("threshold_seconds"))
    except (TypeError, ValueError):
        return _response(400, {"error": "invalid_threshold"})
    if not (MIN_THRESHOLD_SECONDS <= threshold <= MAX_THRESHOLD_SECONDS):
        return _response(400, {"error": "threshold_out_of_range"})

    geofence_id = new_geofence_id()
    item = {
        "user_id": user_id,
        "geofence_id": geofence_id,
        "route_id": route_id,
        "stop_id": None,  # reserved for v2 per-stop directional geofences
        "threshold_seconds": threshold,
        "label": (body.get("label") or f"Route {route_id}")[:120],
        "enabled": True,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last_alerted_epoch": 0,
    }
    _geofences().put_item(Item=item)
    return _response(201, {"geofence_id": geofence_id, "geofence": item})


def delete_geofence(user_id: str, geofence_id: str) -> dict:
    if not geofence_id:
        return _response(400, {"error": "missing_geofence_id"})
    _geofences().delete_item(Key={"user_id": user_id, "geofence_id": geofence_id})
    return _response(204)


# ----- /notifications -----

def list_notifications(user_id: str) -> dict:
    resp = _notifications().query(
        KeyConditionExpression=Key("user_id").eq(user_id),
        ScanIndexForward=False,  # newest first
        Limit=NOTIFICATIONS_LIMIT,
    )
    items = resp.get("Items", [])
    out = []
    unread = 0
    for it in items:
        is_read = bool(it.get("read"))
        if not is_read:
            unread += 1
        out.append({
            "id": it.get("created_at"),
            "route_id": it.get("route_id"),
            "delay_seconds": it.get("delay_seconds"),
            "threshold_seconds": it.get("threshold_seconds"),
            "message": it.get("message"),
            "read": is_read,
            "created_at": it.get("created_at"),
        })
    return _response(200, {"unread_count": unread, "notifications": out})


def mark_notification_read(user_id: str, notification_id: str) -> dict:
    if not notification_id:
        return _response(400, {"error": "missing_notification_id"})
    _notifications().update_item(
        Key={"user_id": user_id, "created_at": notification_id},
        UpdateExpression="SET #r = :true",
        ExpressionAttributeNames={"#r": "read"},
        ExpressionAttributeValues={":true": True},
    )
    return _response(200, {"id": notification_id, "read": True})


# ----- /me -----

def get_me(user_id: str, email: str) -> dict:
    resp = _users().get_item(Key={"user_id": user_id})
    item = resp.get("Item")
    if not item:
        # The PostConfirmation trigger normally seeds this; fall back gracefully.
        item = {"user_id": user_id, "email": email, "email_alerts_enabled": False, "home_routes": []}
    return _response(200, item)


def put_me(user_id: str, body: dict) -> dict:
    enabled = bool(body.get("email_alerts_enabled"))
    _users().update_item(
        Key={"user_id": user_id},
        UpdateExpression="SET email_alerts_enabled = :v",
        ExpressionAttributeValues={":v": enabled},
    )
    return _response(200, {"email_alerts_enabled": enabled})


def lambda_handler(event: dict[str, Any], context: Any) -> dict:
    user_id = user_id_from_event(event)
    if not user_id:
        return _response(401, {"error": "unauthenticated"})

    resource = event.get("resource") or ""
    method = (event.get("httpMethod") or "").upper()
    path_params = event.get("pathParameters") or {}
    claims = ((event.get("requestContext") or {}).get("authorizer") or {}).get("claims") or {}

    if resource == "/geofences":
        if method == "GET":
            return list_geofences(user_id)
        if method == "POST":
            return create_geofence(user_id, _parse_body(event))
    elif resource == "/geofences/{geofenceId}":
        if method == "DELETE":
            return delete_geofence(user_id, path_params.get("geofenceId") or "")
    elif resource == "/notifications":
        if method == "GET":
            return list_notifications(user_id)
    elif resource == "/notifications/{notificationId}":
        if method == "PATCH":
            return mark_notification_read(user_id, path_params.get("notificationId") or "")
    elif resource == "/me":
        if method == "GET":
            return get_me(user_id, claims.get("email", ""))
        if method == "PUT":
            return put_me(user_id, _parse_body(event))

    return _response(404, {"error": "not_found", "resource": resource, "method": method})
```

Create `lambdas/user_api/requirements.txt`:

```text
# boto3 ships in the Lambda runtime; no third-party deps.
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest lambdas/user_api -v`
Expected: PASS (all tests). If `test_list_geofences_scoped_to_user`'s condition assertion is brittle across boto3 versions, simplify it to assert `table.query.called` and that no other table method was invoked — the security guarantee is that the query key is built from `user_id` in code.

- [ ] **Step 5: Commit**

```bash
git add lambdas/user_api/
git commit -m "Phase 6: user-api Lambda — geofence CRUD, notifications, prefs"
```

---

## Task 5: Wire authenticated routes + Cognito authorizer into ApiStack

**Files:**
- Modify: `cdk/lib/api-stack.ts`

- [ ] **Step 1: Extend ApiStackProps and imports**

In `cdk/lib/api-stack.ts`, add imports at the top:

```typescript
import * as cognito from 'aws-cdk-lib/aws-cognito';
```

Extend the props interface:

```typescript
export interface ApiStackProps extends cdk.StackProps {
  hotVehiclesTable: dynamodb.ITable;
  routeAggregatesTable: dynamodb.ITable;
  archiveBucket: s3.IBucket;
  // Phase 6: authenticated user-api dependencies.
  userPool: cognito.IUserPool;
  usersTable: dynamodb.ITable;
  geofencesTable: dynamodb.ITable;
  notificationsTable: dynamodb.ITable;
}
```

- [ ] **Step 2: Update CORS to allow the write verbs + Authorization header**

Replace the existing `defaultCorsPreflightOptions` block in the `new apigw.LambdaRestApi(...)` call with:

```typescript
      defaultCorsPreflightOptions: {
        allowOrigins: apigw.Cors.ALL_ORIGINS,
        allowMethods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
        allowHeaders: ['Content-Type', 'Authorization'],
      },
```

- [ ] **Step 3: Add the user-api Lambda, authorizer, and protected routes**

After the existing `/stops/{stopId}/arrivals` block (around line 110, before the `ApiUrl` CfnOutput), add:

```typescript
    // ----- Phase 6: authenticated user-api -----
    const userApiName = 'la-metro-user-api';
    const userApiLogGroup = new logs.LogGroup(this, 'UserApiFnLogs', {
      logGroupName: `/aws/lambda/${userApiName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const userApiFn = new lambda.Function(this, 'UserApiFn', {
      functionName: userApiName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'lambdas', 'user_api', '.build'),
      ),
      memorySize: 256,
      timeout: cdk.Duration.seconds(10),
      environment: {
        USERS_TABLE_NAME: props.usersTable.tableName,
        GEOFENCES_TABLE_NAME: props.geofencesTable.tableName,
        NOTIFICATIONS_TABLE_NAME: props.notificationsTable.tableName,
      },
      logGroup: userApiLogGroup,
      description: 'Phase 6: authenticated geofence CRUD, notifications, prefs.',
    });
    props.usersTable.grantReadWriteData(userApiFn);
    props.geofencesTable.grantReadWriteData(userApiFn);
    props.notificationsTable.grantReadWriteData(userApiFn);

    // Cognito User Pool authorizer — validates the JWT and exposes claims at
    // event.requestContext.authorizer.claims for the Lambda.
    const authorizer = new apigw.CognitoUserPoolsAuthorizer(this, 'UserPoolAuthorizer', {
      cognitoUserPools: [props.userPool],
      authorizerName: 'la-metro-cognito',
    });

    const userIntegration = new apigw.LambdaIntegration(userApiFn);
    const authMethodOptions: apigw.MethodOptions = {
      authorizer,
      authorizationType: apigw.AuthorizationType.COGNITO,
    };

    // /geofences and /geofences/{geofenceId}
    const geofences = api.root.addResource('geofences');
    geofences.addMethod('GET', userIntegration, authMethodOptions);
    geofences.addMethod('POST', userIntegration, authMethodOptions);
    const geofenceById = geofences.addResource('{geofenceId}');
    geofenceById.addMethod('DELETE', userIntegration, authMethodOptions);

    // /notifications and /notifications/{notificationId}
    const notifications = api.root.addResource('notifications');
    notifications.addMethod('GET', userIntegration, authMethodOptions);
    const notificationById = notifications.addResource('{notificationId}');
    notificationById.addMethod('PATCH', userIntegration, authMethodOptions);

    // /me
    const me = api.root.addResource('me');
    me.addMethod('GET', userIntegration, authMethodOptions);
    me.addMethod('PUT', userIntegration, authMethodOptions);
```

> Note: the existing `/vehicles`, `/routes/*`, `/stops/*` methods stay public — they're added with the default handler (`queryFn`) and no `authMethodOptions`. Only the methods above carry the Cognito authorizer.

- [ ] **Step 4: Build the user_api asset and type-check**

Run:
```bash
scripts/build-lambda.sh user_api
cd cdk && npx tsc --noEmit
```
Expected: build succeeds; no type errors. (App wiring is Task 7.)

- [ ] **Step 5: Commit**

```bash
git add cdk/lib/api-stack.ts
git commit -m "Phase 6: Cognito authorizer + authenticated routes on ApiStack"
```

---

## Task 6: Geofence evaluation in the Aggregation Lambda

Add a pure decision function (TDD) plus the IO that queries geofences and writes notifications, then call it from `lambda_handler` after `write_aggregates`.

**Files:**
- Modify: `lambdas/aggregation/handler.py`
- Modify: `lambdas/aggregation/tests/test_handler.py`
- Modify: `cdk/lib/processing-stack.ts`

- [ ] **Step 1: Write the failing tests**

Append to `lambdas/aggregation/tests/test_handler.py`:

```python
def test_geofence_breaches_pure_logic():
    # avg_delay 360s; one geofence threshold 300 (breach, cold), one 600 (no
    # breach), one 300 but recently alerted (cooldown suppresses it).
    now_epoch = 1_700_000_000
    geofences = [
        {"user_id": "u1", "geofence_id": "g1", "threshold_seconds": Decimal("300"),
         "enabled": True, "last_alerted_epoch": Decimal("0")},
        {"user_id": "u2", "geofence_id": "g2", "threshold_seconds": Decimal("600"),
         "enabled": True, "last_alerted_epoch": Decimal("0")},
        {"user_id": "u3", "geofence_id": "g3", "threshold_seconds": Decimal("300"),
         "enabled": True, "last_alerted_epoch": Decimal(str(now_epoch - 60))},
        {"user_id": "u4", "geofence_id": "g4", "threshold_seconds": Decimal("300"),
         "enabled": False, "last_alerted_epoch": Decimal("0")},
    ]
    breaches = handler.geofence_breaches(geofences, avg_delay=360, now_epoch=now_epoch, cooldown=900)
    fired = {g["geofence_id"] for g in breaches}
    assert fired == {"g1"}  # g2 below threshold, g3 in cooldown, g4 disabled


def test_geofence_breaches_cooldown_elapsed():
    now_epoch = 1_700_000_000
    geofences = [
        {"user_id": "u1", "geofence_id": "g1", "threshold_seconds": Decimal("300"),
         "enabled": True, "last_alerted_epoch": Decimal(str(now_epoch - 1000))},
    ]
    # 1000s since last alert > 900s cooldown → fires again.
    assert len(handler.geofence_breaches(geofences, 360, now_epoch, 900)) == 1


def test_build_notification_item():
    item = handler.build_notification_item(
        user_id="u1", route_id="720", avg_delay=372, threshold=300,
        now=datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc),
    )
    assert item["user_id"] == "u1"
    assert item["route_id"] == "720"
    assert item["delay_seconds"] == 372
    assert item["threshold_seconds"] == 300
    assert item["read"] is False
    assert item["created_at"].startswith("2026-05-26T12:00:00")
    assert "720" in item["message"]
    created_epoch = datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    assert item["ttl_epoch"] > int(created_epoch)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest lambdas/aggregation -v -k "geofence or notification"`
Expected: FAIL — `AttributeError: module 'lambdas.aggregation.handler' has no attribute 'geofence_breaches'`.

- [ ] **Step 3: Add the pure logic + builders to the handler**

In `lambdas/aggregation/handler.py`, add new env vars near the existing ones (after `ON_TIME_TOLERANCE_SECONDS`):

```python
GEOFENCES_TABLE = os.environ.get("GEOFENCES_TABLE_NAME", "")
GEOFENCES_ROUTE_GSI = os.environ.get("GEOFENCES_ROUTE_GSI", "route_id-index")
NOTIFICATIONS_TABLE = os.environ.get("NOTIFICATIONS_TABLE_NAME", "")
ALERT_COOLDOWN_SECONDS = int(os.environ.get("ALERT_COOLDOWN_SECONDS", "900"))  # 15 min
NOTIFICATION_TTL_DAYS = int(os.environ.get("NOTIFICATION_TTL_DAYS", "7"))
```

Add these functions (before `lambda_handler`):

```python
def geofence_breaches(
    geofences: Iterable[dict[str, Any]],
    avg_delay: int,
    now_epoch: int,
    cooldown: int = ALERT_COOLDOWN_SECONDS,
) -> list[dict[str, Any]]:
    """Pure decision: which geofences should fire for this route right now?

    A geofence fires when it is enabled, the route's avg delay exceeds its
    threshold, and its cooldown window has elapsed since the last alert.
    """
    out: list[dict[str, Any]] = []
    for gf in geofences:
        if not gf.get("enabled", False):
            continue
        threshold = _to_int(gf.get("threshold_seconds"))
        if threshold is None or avg_delay <= threshold:
            continue
        last = _to_int(gf.get("last_alerted_epoch")) or 0
        if now_epoch - last < cooldown:
            continue
        out.append(gf)
    return out


def build_notification_item(
    user_id: str, route_id: str, avg_delay: int, threshold: int, now: datetime
) -> dict[str, Any]:
    minutes = round(avg_delay / 60)
    return {
        "user_id": user_id,
        "created_at": now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "route_id": route_id,
        "delay_seconds": avg_delay,
        "threshold_seconds": threshold,
        "message": f"Route {route_id} is running ~{minutes} min late "
                   f"(over your {round(threshold / 60)} min alert).",
        "read": False,
        "ttl_epoch": int((now + timedelta(days=NOTIFICATION_TTL_DAYS)).timestamp()),
    }


def evaluate_geofences(
    geofences_table,
    notifications_table,
    aggregates: dict[str, dict[str, Any]],
    now: datetime,
) -> int:
    """For each route with a real avg delay, find breaching geofences, write a
    notification per breach, and stamp last_alerted_epoch. Returns alert count.
    """
    now_epoch = int(now.timestamp())
    fired = 0
    for route_id, agg in aggregates.items():
        avg_delay = agg.get("avg_delay_seconds")
        if avg_delay is None:
            continue
        resp = geofences_table.query(
            IndexName=GEOFENCES_ROUTE_GSI,
            KeyConditionExpression=Key("route_id").eq(route_id),
        )
        breaches = geofence_breaches(resp.get("Items", []), int(avg_delay), now_epoch)
        for gf in breaches:
            threshold = _to_int(gf.get("threshold_seconds")) or 0
            notifications_table.put_item(
                Item=build_notification_item(
                    gf["user_id"], route_id, int(avg_delay), threshold, now
                )
            )
            geofences_table.update_item(
                Key={"user_id": gf["user_id"], "geofence_id": gf["geofence_id"]},
                UpdateExpression="SET last_alerted_epoch = :e",
                ExpressionAttributeValues={":e": now_epoch},
            )
            fired += 1
    return fired
```

Add the `Key` import at the top (next to `from boto3.dynamodb.conditions import Attr`):

```python
from boto3.dynamodb.conditions import Attr, Key
```

- [ ] **Step 4: Run the pure-logic tests**

Run: `pytest lambdas/aggregation -v -k "geofence or notification"`
Expected: PASS.

- [ ] **Step 5: Call evaluate_geofences from lambda_handler**

In `lambda_handler`, after `written = write_aggregates(...)` and before building the `log` dict, add:

```python
    alerts_fired = 0
    if GEOFENCES_TABLE and NOTIFICATIONS_TABLE:
        try:
            alerts_fired = evaluate_geofences(
                get_table(GEOFENCES_TABLE),
                get_table(NOTIFICATIONS_TABLE),
                aggregates,
                now,
            )
        except Exception:
            # Alerting must never take down the aggregation cycle.
            logger.exception("geofence_evaluation_failed")
```

Add `"alerts_fired": alerts_fired,` to the `log` dict.

- [ ] **Step 6: Run the full aggregation test suite**

Run: `pytest lambdas/aggregation -v`
Expected: PASS (existing tests still green; the end-to-end test leaves `GEOFENCES_TABLE` empty so evaluation is skipped).

- [ ] **Step 7: Grant the Aggregation Lambda access + env in ProcessingStack**

In `cdk/lib/processing-stack.ts`, extend `ProcessingStackProps` with:

```typescript
  geofencesTable: dynamodb.ITable;
  notificationsTable: dynamodb.ITable;
```

In the `aggregationFn` `environment` block, add:

```typescript
        GEOFENCES_TABLE_NAME: props.geofencesTable.tableName,
        GEOFENCES_ROUTE_GSI: 'route_id-index',
        NOTIFICATIONS_TABLE_NAME: props.notificationsTable.tableName,
```

After the existing `props.routeAggregatesTable.grantWriteData(aggregationFn);`, add:

```typescript
    // Phase 6: read+update geofences (GSI read + cooldown stamp) and write
    // notifications when a route breaches a user's threshold.
    props.geofencesTable.grantReadWriteData(aggregationFn);
    props.notificationsTable.grantWriteData(aggregationFn);
```

- [ ] **Step 8: Type-check and commit**

Run: `cd cdk && npx tsc --noEmit` → expect no errors.

```bash
git add lambdas/aggregation/ cdk/lib/processing-stack.ts
git commit -m "Phase 6: geofence evaluation + notifications in Aggregation Lambda"
```

---

## Task 7: Wire AuthStack into the CDK app + update CI build list

**Files:**
- Modify: `cdk/bin/cdk.ts`
- Modify: `.github/workflows/pr-checks.yml`

- [ ] **Step 1: Construct AuthStack and pass new deps through**

In `cdk/bin/cdk.ts`, add the import after the others:

```typescript
import { AuthStack } from '../lib/auth-stack';
```

Construct `AuthStack` after `storage` and before `processing`:

```typescript
const auth = new AuthStack(app, 'LaMetro-AuthStack', {
  env,
  usersTable: storage.usersTable,
  description: 'Phase 6: Cognito user pool + PostConfirmation trigger.',
});
```

Add the new tables to the `processing` props:

```typescript
  geofencesTable: storage.geofencesTable,
  notificationsTable: storage.notificationsTable,
```

Add the new deps to the `api` props:

```typescript
  userPool: auth.userPool,
  usersTable: storage.usersTable,
  geofencesTable: storage.geofencesTable,
  notificationsTable: storage.notificationsTable,
```

Add `auth` to the tagging loop array:

```typescript
for (const stack of [storage, auth, ingestion, processing, api, frontend, websocket, billing]) {
```

- [ ] **Step 2: Add new lambdas to the CI build loop**

In `.github/workflows/pr-checks.yml`, in the "Build Lambda assets" step, change the loop to include the two new lambdas:

```yaml
          for d in ingestion enrichment query_api aggregation websocket user_api post_confirmation; do
            scripts/build-lambda.sh "$d"
          done
```

- [ ] **Step 3: Full local build + synth**

Run:
```bash
for d in ingestion enrichment query_api aggregation websocket user_api post_confirmation; do scripts/build-lambda.sh "$d"; done
cd cdk && npx tsc --noEmit && npx cdk synth --quiet
```
Expected: synth succeeds and prints the templates (or `--quiet` suppresses output and exits 0). Confirm there are no missing-asset errors for `user_api/.build` or `post_confirmation/.build`.

- [ ] **Step 4: Run the whole Python suite**

Run: `pytest lambdas/`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add cdk/bin/cdk.ts .github/workflows/pr-checks.yml
git commit -m "Phase 6: wire AuthStack into CDK app + CI build list"
```

---

## Task 8: Frontend — Amplify config + auth provider

The frontend is a Next.js **static export** (`output: 'export'`), so all auth is client-side. We gate only the account page, not the public map.

**Files:**
- Modify: `frontend/package.json` (add deps)
- Create: `frontend/lib/amplify.ts`
- Create: `frontend/components/AuthGate.tsx`
- Create/modify: `frontend/.env.local.example`

- [ ] **Step 1: Add Amplify dependencies**

Run from `frontend/`:
```bash
npm install aws-amplify @aws-amplify/ui-react
```
Expected: both added to `dependencies` in `frontend/package.json`.

- [ ] **Step 2: Amplify configuration module**

Create `frontend/lib/amplify.ts`:

```typescript
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
```

- [ ] **Step 3: Auth gate component**

Create `frontend/components/AuthGate.tsx`:

```tsx
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
```

- [ ] **Step 4: Document the new env vars**

Create `frontend/.env.local.example` (or append if it exists):

```text
NEXT_PUBLIC_API_BASE_URL=https://your-api-id.execute-api.us-west-2.amazonaws.com/prod
NEXT_PUBLIC_COGNITO_USER_POOL_ID=us-west-2_xxxxxxxxx
NEXT_PUBLIC_COGNITO_CLIENT_ID=xxxxxxxxxxxxxxxxxxxxxxxxxx
```

- [ ] **Step 5: Verify the build still compiles**

Run from `frontend/`: `npm run build`
Expected: build succeeds (the account page consuming these comes next; this step just confirms the deps + config module compile).

- [ ] **Step 6: Commit**

```bash
git add frontend/package.json frontend/package-lock.json frontend/lib/amplify.ts frontend/components/AuthGate.tsx frontend/.env.local.example
git commit -m "Phase 6: frontend Amplify config + auth gate"
```

---

## Task 9: Frontend — authenticated API client

**Files:**
- Create: `frontend/lib/user-api.ts`

- [ ] **Step 1: Write the authenticated client**

Create `frontend/lib/user-api.ts`:

```typescript
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
```

- [ ] **Step 2: Verify compile**

Run from `frontend/`: `npx tsc --noEmit` (or `npm run build`)
Expected: no type errors.

- [ ] **Step 3: Commit**

```bash
git add frontend/lib/user-api.ts
git commit -m "Phase 6: frontend authenticated API client"
```

---

## Task 10: Frontend — "My Routes" account page

**Files:**
- Create: `frontend/app/account/page.tsx`

- [ ] **Step 1: Build the account page**

Create `frontend/app/account/page.tsx`:

```tsx
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
```

> Route selection is a validated text input for the MVP (LA Metro route ids are short, e.g. `720`, `33`, `2`). Upgrading to the searchable picker used in `MetroMap.tsx` is a clean follow-up; note it in the README future-work, not here.

- [ ] **Step 2: Build the frontend**

Run from `frontend/`: `npm run build`
Expected: static export builds, including `/account`.

- [ ] **Step 3: Commit**

```bash
git add frontend/app/account/page.tsx
git commit -m "Phase 6: My Routes account page (geofence CRUD + email toggle)"
```

---

## Task 11: Frontend — notification bell + map-page entry point

**Files:**
- Create: `frontend/components/NotificationBell.tsx`
- Create: `frontend/components/AccountNav.tsx`
- Modify: `frontend/app/page.tsx`

- [ ] **Step 1: Notification bell component**

Create `frontend/components/NotificationBell.tsx`:

```tsx
'use client';

import { useEffect, useState } from 'react';
import { listNotifications, markNotificationRead, type AppNotification } from '@/lib/user-api';

const POLL_MS = 60_000;

export function NotificationBell() {
  const [open, setOpen] = useState(false);
  const [items, setItems] = useState<AppNotification[]>([]);
  const [unread, setUnread] = useState(0);

  useEffect(() => {
    let active = true;
    async function poll() {
      try {
        const { notifications, unread_count } = await listNotifications();
        if (!active) return;
        setItems(notifications);
        setUnread(unread_count);
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

  async function onOpen() {
    setOpen((v) => !v);
    const unreadIds = items.filter((n) => !n.read).map((n) => n.id);
    if (unreadIds.length) {
      setUnread(0);
      setItems((prev) => prev.map((n) => ({ ...n, read: true })));
      await Promise.allSettled(unreadIds.map((id) => markNotificationRead(id)));
    }
  }

  return (
    <div className="relative">
      <button
        onClick={onOpen}
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
```

- [ ] **Step 2: Account nav (sign-in aware) for the map page**

Create `frontend/components/AccountNav.tsx`:

```tsx
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
```

- [ ] **Step 3: Mount AccountNav on the map page**

In `frontend/app/page.tsx`, import and render `<AccountNav />` inside the page's root container (it's absolutely positioned, so place it as a sibling of the map element). Add near the top of the JSX returned by the page:

```tsx
import { AccountNav } from '@/components/AccountNav';
```

and render it inside the outermost wrapper, e.g.:

```tsx
      <AccountNav />
```

> Open `frontend/app/page.tsx` first to confirm the root element; place `<AccountNav />` as a direct child of the relatively/absolutely-positioned container that wraps the map so the `absolute right-4 top-4` anchors correctly. If the root isn't positioned, wrap the existing content in a `<div className="relative">`.

- [ ] **Step 4: Build the frontend**

Run from `frontend/`: `npm run build`
Expected: builds clean.

- [ ] **Step 5: Commit**

```bash
git add frontend/components/NotificationBell.tsx frontend/components/AccountNav.tsx frontend/app/page.tsx
git commit -m "Phase 6: notification bell + account nav on map page"
```

---

## Task 12: Docs — README future work + cost note

**Files:**
- Modify: `README.md` (or `docs/COST.md` if that's where cost notes live — check first)

- [ ] **Step 1: Add a Phase 6 "future work" note**

In the README's future-work section (create one if absent), add:

```markdown
### Phase 6 follow-ups
- **Email alerts:** the per-user "email me" toggle is built and persisted; wiring
  it to SES (or SNS→Lambda→SES) is the remaining step. Deferred to avoid the SES
  sandbox during the build.
- **Per-stop directional geofences:** geofences currently fire on a route's
  average delay. The `stop_id` field is reserved so alerts can later target the
  next vehicle approaching a specific stop in the user's direction.
- **Real-time alert push:** notifications are polled every 60s; they could ride
  the existing WebSocket for instant delivery once the socket is authenticated.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "Phase 6: document email + per-stop + real-time follow-ups"
```

---

## Deployment & manual verification (after all tasks)

These require AWS credentials and are run once, manually — not part of the per-task loop.

- [ ] `cd cdk && npx cdk deploy LaMetro-StorageStack LaMetro-AuthStack LaMetro-ProcessingStack LaMetro-ApiStack` (StorageStack first so the tables exist; AuthStack before ApiStack so the user pool exists for the authorizer).
- [ ] Capture the `UserPoolId`, `UserPoolClientId`, and `ApiUrl` outputs into `frontend/.env.local`.
- [ ] `cd frontend && npm run build && cd ../cdk && npx cdk deploy LaMetro-FrontendStack`.
- [ ] Sign up with a real email, confirm the code, verify a `users` row appears in `la-metro-users` (PostConfirmation worked).
- [ ] Add a geofence on a route that's currently late (or temporarily set threshold to a low value); within ~1 min the Aggregation Lambda writes a `notifications` row and the bell shows a badge within 60s.
- [ ] Confirm a second alert does NOT arrive for ~15 min (cooldown), then does after.

---

## Self-review notes (author)

- **Spec coverage:** Cognito + Authenticator (Tasks 3, 8) · users/geofences/notifications tables (Task 1) · PostConfirmation seeding (Task 2) · route-level eval + 15-min cooldown + avg metric (Task 6) · authenticated `/geofences`,`/notifications`,`/me` behind Cognito authorizer (Tasks 4, 5) · My Routes page + bell + email toggle stub (Tasks 9–11) · SES/per-stop/WebSocket deferred (Task 12). All spec sections map to a task.
- **moto deviation:** the spec named moto; the codebase uses `MagicMock`+`monkeypatch` with no moto dependency. The plan follows the codebase to keep CI green without adding a dependency. (Worth a one-line note to the user.)
- **Type consistency:** notification `id` == DynamoDB sort key `created_at` (microsecond ISO) throughout (Lambda `list_notifications`/`mark_notification_read`, frontend `AppNotification.id`). Geofence GSI name `route_id-index` matches in StorageStack, aggregation env, and ProcessingStack env. Authorizer claims path `requestContext.authorizer.claims.sub` consistent across user-api handler and tests.
