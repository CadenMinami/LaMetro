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
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

USERS_TABLE = os.environ.get("USERS_TABLE_NAME", "")

_ddb = None


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
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            logger.info("user row already exists for %s", user_id)
        else:
            # Never fail the trigger — log and move on so the user can sign in.
            logger.exception("failed to seed user row for %s", user_id)

    return event
