"""Message revision, room freeze, and member moderation integration coverage."""

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from samsarix_chat_engine import AccessTokenService, Settings, create_app
from samsarix_chat_engine.models import MemberModerationUpdate, RoomCreate
from samsarix_chat_engine.store import (
    ChatStore,
    MemberBannedError,
    MemberMutedError,
    MessageOwnershipError,
    RoomArchivedError,
    RoomFrozenError,
)

SIGNING_SECRET = "test-only-signing-secret-that-is-long-enough"
OPERATOR_KEY = "test-only-operator-api-key"


@pytest.fixture
def conversation_client(tmp_path: Path) -> Iterator[tuple[TestClient, AccessTokenService]]:
    settings = Settings(
        database_path=tmp_path / "conversation-controls.db",
        api_key=OPERATOR_KEY,
        token_signing_secret=SIGNING_SECRET,
        token_issuer="test-issuer",
        token_audience="test-audience",
        token_max_lifetime_seconds=3_600,
        token_clock_skew_seconds=0,
    )
    service = AccessTokenService(
        SIGNING_SECRET,
        issuer="test-issuer",
        audience="test-audience",
        max_lifetime_seconds=3_600,
        clock_skew_seconds=0,
    )
    with TestClient(create_app(settings)) as client:
        created = client.post(
            "/v1/rooms",
            headers={"X-API-Key": OPERATOR_KEY},
            json={"id": "alpha", "name": "Alpha"},
        )
        assert created.status_code == 201
        yield client, service


def _token(service: AccessTokenService, subject: str) -> str:
    return service.issue(
        subject,
        rooms=["alpha"],
        permissions=["room:read", "room:write"],
        expires_in_seconds=300,
    )


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _operator() -> dict[str, str]:
    return {"X-API-Key": OPERATOR_KEY}


def test_authors_edit_and_delete_tombstones_without_leaking_content(
    conversation_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = conversation_client
    author = _token(service, "author")
    stranger = _token(service, "stranger")
    created = client.post(
        "/v1/rooms/alpha/messages",
        headers=_bearer(author),
        json={"content": "sensitive original"},
    )
    message_id = created.json()["id"]

    denied = client.patch(
        f"/v1/rooms/alpha/messages/{message_id}",
        headers=_bearer(stranger),
        json={"content": "hijacked"},
    )
    edited = client.patch(
        f"/v1/rooms/alpha/messages/{message_id}",
        headers=_bearer(author),
        json={"content": "corrected"},
    )
    deleted = client.delete(f"/v1/rooms/alpha/messages/{message_id}", headers=_bearer(author))
    replay = client.delete(f"/v1/rooms/alpha/messages/{message_id}", headers=_bearer(author))
    rejected_edit = client.patch(
        f"/v1/rooms/alpha/messages/{message_id}",
        headers=_bearer(author),
        json={"content": "resurrected"},
    )
    history = client.get("/v1/rooms/alpha/messages", headers=_bearer(author)).json()["items"]
    audit = client.get("/v1/admin/audit-events", headers=_operator()).json()["items"]

    assert denied.status_code == 403
    assert denied.json()["error"]["code"] == "message_not_owned"
    assert edited.status_code == 200
    assert edited.json()["content"] == "corrected"
    assert edited.json()["edited_at"] is not None
    assert deleted.status_code == replay.status_code == 204
    assert rejected_edit.status_code == 409
    assert rejected_edit.json()["error"]["code"] == "message_deleted"
    assert history[0]["content"] == ""
    assert history[0]["deleted_at"] is not None
    serialized_audit = str(audit)
    assert "sensitive original" not in serialized_audit
    assert "corrected" not in serialized_audit


def test_room_freeze_blocks_members_but_not_operator_moderation(
    conversation_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = conversation_client
    member = _token(service, "member")
    existing = client.post(
        "/v1/rooms/alpha/messages",
        headers=_bearer(member),
        json={"content": "before freeze"},
    ).json()

    frozen = client.patch("/v1/rooms/alpha", headers=_operator(), json={"frozen": True})
    blocked = client.post(
        "/v1/rooms/alpha/messages",
        headers=_bearer(member),
        json={"content": "blocked"},
    )
    operator_message = client.post(
        "/v1/rooms/alpha/messages",
        headers=_operator(),
        json={"sender": "moderator", "content": "announcement"},
    )
    blocked_edit = client.patch(
        f"/v1/rooms/alpha/messages/{existing['id']}",
        headers=_bearer(member),
        json={"content": "blocked edit"},
    )
    operator_edit = client.patch(
        f"/v1/rooms/alpha/messages/{existing['id']}",
        headers=_operator(),
        json={"content": "moderator correction"},
    )
    unfrozen = client.patch("/v1/rooms/alpha", headers=_operator(), json={"frozen": False})
    resumed = client.post(
        "/v1/rooms/alpha/messages",
        headers=_bearer(member),
        json={"content": "resumed"},
    )

    assert frozen.status_code == 200
    assert frozen.json()["frozen_at"] is not None
    assert blocked.status_code == 409
    assert blocked.json()["error"]["code"] == "room_frozen"
    assert operator_message.status_code == 201
    assert blocked_edit.status_code == 409
    assert blocked_edit.json()["error"]["code"] == "room_frozen"
    assert operator_edit.status_code == 200
    assert unfrozen.json()["frozen_at"] is None
    assert resumed.status_code == 201


def test_mute_preserves_reads_and_ban_revokes_room_access(
    conversation_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = conversation_client
    token = _token(service, "member")

    muted = client.patch(
        "/v1/rooms/alpha/members/member/moderation",
        headers=_operator(),
        json={"muted_for_seconds": 300},
    )
    readable = client.get("/v1/rooms/alpha", headers=_bearer(token))
    blocked_write = client.post(
        "/v1/rooms/alpha/messages",
        headers=_bearer(token),
        json={"content": "blocked"},
    )
    cleared = client.patch(
        "/v1/rooms/alpha/members/member/moderation",
        headers=_operator(),
        json={"muted_for_seconds": 0},
    )
    resumed = client.post(
        "/v1/rooms/alpha/messages",
        headers=_bearer(token),
        json={"content": "allowed"},
    )
    banned = client.patch(
        "/v1/rooms/alpha/members/member/moderation",
        headers=_operator(),
        json={"banned_for_seconds": 300},
    )
    blocked_read = client.get("/v1/rooms/alpha", headers=_bearer(token))

    assert muted.status_code == 200
    assert muted.json()["muted_until"] is not None
    assert readable.status_code == 200
    assert blocked_write.status_code == 403
    assert blocked_write.json()["error"]["code"] == "room_muted"
    assert cleared.json()["muted_until"] is None
    assert resumed.status_code == 201
    assert banned.json()["banned_until"] is not None
    assert blocked_read.status_code == 403
    assert blocked_read.json()["error"]["code"] == "room_banned"


def test_conversation_control_errors_are_stable(
    conversation_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = conversation_client
    token = _token(service, "member")

    missing_edit = client.patch(
        "/v1/rooms/alpha/messages/missing",
        headers=_bearer(token),
        json={"content": "replacement"},
    )
    missing_delete = client.delete("/v1/rooms/alpha/messages/missing", headers=_bearer(token))
    missing_room = client.patch(
        "/v1/rooms/missing/members/member/moderation",
        headers=_operator(),
        json={"muted_for_seconds": 60},
    )
    padded_subject = client.patch(
        "/v1/rooms/alpha/members/%20member%20/moderation",
        headers=_operator(),
        json={"muted_for_seconds": 60},
    )

    assert missing_edit.status_code == 404
    assert missing_edit.json()["error"]["code"] == "message_not_found"
    assert missing_delete.status_code == 404
    assert missing_delete.json()["error"]["code"] == "message_not_found"
    assert missing_room.status_code == 404
    assert missing_room.json()["error"]["code"] == "room_not_found"
    assert padded_subject.status_code == 422
    assert padded_subject.json()["error"]["code"] == "invalid_request"


def test_live_ban_notifies_and_closes_matching_subject(
    conversation_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = conversation_client
    token = _token(service, "member")

    with pytest.raises(WebSocketDisconnect) as disconnected:
        with client.websocket_connect("/v1/rooms/alpha/ws") as websocket:
            assert websocket.receive_json()["type"] == "auth.required"
            websocket.send_json({"type": "auth", "token": token})
            assert websocket.receive_json()["type"] == "ready"
            assert websocket.receive_json()["type"] == "history"
            response = client.patch(
                "/v1/rooms/alpha/members/member/moderation",
                headers=_operator(),
                json={"banned_for_seconds": 300},
            )
            assert response.status_code == 200
            event = websocket.receive_json()
            assert event["type"] == "member.banned"
            assert event["subject"] == "member"
            websocket.receive_json()
    assert disconnected.value.code == 4403


def test_message_revisions_broadcast_and_reconnect_as_current_state(
    conversation_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = conversation_client
    author = _token(service, "author")
    observer = _token(service, "observer")

    with client.websocket_connect("/v1/rooms/alpha/ws") as websocket:
        websocket.receive_json()
        websocket.send_json({"type": "auth", "token": observer})
        assert websocket.receive_json()["type"] == "ready"
        assert websocket.receive_json()["type"] == "history"
        created = client.post(
            "/v1/rooms/alpha/messages",
            headers=_bearer(author),
            json={"content": "draft"},
        )
        assert websocket.receive_json()["type"] == "message.created"
        message_id = created.json()["id"]

        client.patch(
            f"/v1/rooms/alpha/messages/{message_id}",
            headers=_bearer(author),
            json={"content": "final"},
        )
        updated = websocket.receive_json()
        assert updated["type"] == "message.updated"
        assert updated["message"]["content"] == "final"

        client.delete(f"/v1/rooms/alpha/messages/{message_id}", headers=_bearer(author))
        deleted = websocket.receive_json()
        assert deleted["type"] == "message.deleted"
        assert deleted["message"]["content"] == ""
        assert deleted["message"]["deleted_at"] is not None

    with client.websocket_connect("/v1/rooms/alpha/ws") as websocket:
        websocket.receive_json()
        websocket.send_json({"type": "auth", "token": observer})
        websocket.receive_json()
        history = websocket.receive_json()
        assert history["items"][0]["id"] == message_id
        assert history["items"][0]["content"] == ""
        assert history["items"][0]["deleted_at"] is not None


def test_freeze_is_live_and_keeps_read_sessions_connected(
    conversation_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = conversation_client
    token = _token(service, "member")

    with client.websocket_connect("/v1/rooms/alpha/ws") as websocket:
        websocket.receive_json()
        websocket.send_json({"type": "auth", "token": token})
        websocket.receive_json()
        websocket.receive_json()
        response = client.patch("/v1/rooms/alpha", headers=_operator(), json={"frozen": True})
        assert response.status_code == 200
        assert websocket.receive_json()["type"] == "room.frozen"
        websocket.send_json({"type": "message", "content": "blocked"})
        assert websocket.receive_json()["code"] == "room_frozen"
        websocket.send_json({"type": "ping"})
        assert websocket.receive_json() == {"type": "pong"}


@pytest.mark.asyncio
async def test_store_rechecks_moderation_and_lifecycle_inside_write_transactions(tmp_path: Path) -> None:
    store = ChatStore(
        tmp_path / "transactional-controls.db",
        max_rooms=10,
        max_stored_messages=100,
        max_stored_messages_per_room=100,
    )
    await store.initialize()
    await store.create_room(RoomCreate(id="alpha", name="Alpha"))
    original, _ = await store.create_message(
        room_id="alpha",
        sender="author",
        content="original",
        client_message_id=None,
        allow_frozen=False,
        member_subject="author",
    )

    await store.set_member_moderation(
        "alpha",
        "author",
        MemberModerationUpdate(muted_for_seconds=300),
        actor="operator",
    )
    with pytest.raises(MemberMutedError):
        await store.update_message(
            room_id="alpha",
            message_id=original.id,
            actor="author",
            content="muted edit",
            is_admin=False,
            member_subject="author",
        )

    await store.set_member_moderation(
        "alpha",
        "author",
        MemberModerationUpdate(muted_for_seconds=0, banned_for_seconds=300),
        actor="operator",
    )
    with pytest.raises(MemberBannedError):
        await store.create_message(
            room_id="alpha",
            sender="author",
            content="banned write",
            client_message_id=None,
            allow_frozen=False,
            member_subject="author",
        )

    await store.set_member_moderation(
        "alpha",
        "author",
        MemberModerationUpdate(banned_for_seconds=0),
        actor="operator",
    )
    with pytest.raises(MessageOwnershipError):
        await store.delete_message(
            room_id="alpha",
            message_id=original.id,
            actor="stranger",
            is_admin=False,
            member_subject="stranger",
        )

    await store.set_room_state("alpha", archived=None, frozen=True, actor="operator")
    with pytest.raises(RoomFrozenError):
        await store.update_message(
            room_id="alpha",
            message_id=original.id,
            actor="author",
            content="frozen edit",
            is_admin=False,
            member_subject="author",
        )
    await store.set_room_state("alpha", archived=True, frozen=None, actor="operator")
    with pytest.raises(RoomArchivedError):
        await store.delete_message(
            room_id="alpha",
            message_id=original.id,
            actor="author",
            is_admin=False,
            member_subject="author",
        )
    tombstone, deleted = await store.delete_message(
        room_id="alpha",
        message_id=original.id,
        actor="operator",
        is_admin=True,
    )
    assert deleted is True
    assert tombstone.content == ""
