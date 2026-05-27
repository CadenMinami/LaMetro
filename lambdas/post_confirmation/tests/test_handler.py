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
    # A repeat confirmation (same sub) raises ConditionalCheckFailedException —
    # this must NOT fail the trigger, or the user can't sign in.
    from botocore.exceptions import ClientError

    table = MagicMock()
    table.put_item.side_effect = ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException", "Message": "exists"}},
        "PutItem",
    )
    monkeypatch.setattr(handler, "get_table", lambda name: table)
    monkeypatch.setattr(handler, "USERS_TABLE", "fake-users")

    event = _event()
    assert handler.lambda_handler(event, MagicMock()) is event


def test_swallows_unexpected_client_error(monkeypatch):
    # Any other DynamoDB error must also be swallowed — a failed seed should
    # never block the user from signing in.
    from botocore.exceptions import ClientError

    table = MagicMock()
    table.put_item.side_effect = ClientError(
        {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "slow down"}},
        "PutItem",
    )
    monkeypatch.setattr(handler, "get_table", lambda name: table)
    monkeypatch.setattr(handler, "USERS_TABLE", "fake-users")

    event = _event()
    assert handler.lambda_handler(event, MagicMock()) is event
