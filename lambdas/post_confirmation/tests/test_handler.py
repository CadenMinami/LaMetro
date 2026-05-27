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
