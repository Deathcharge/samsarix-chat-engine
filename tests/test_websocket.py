"""End-to-end WebSocket protocol tests."""

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from helix_chat_engine import Settings, create_app


def test_websocket_message_is_broadcast_and_recovered(client: TestClient, room: dict[str, str]) -> None:
    with client.websocket_connect("/v1/rooms/general/ws?username=Andrew") as websocket:
        ready = websocket.receive_json()
        history = websocket.receive_json()
        websocket.send_json({"type": "message", "content": "Hello room", "client_message_id": "ws-1"})
        message = websocket.receive_json()

    assert ready["type"] == "ready"
    assert ready["room"]["id"] == "general"
    assert history == {"type": "history", "items": [], "next_before": None}
    assert message["type"] == "message.created"
    assert message["message"]["content"] == "Hello room"
    assert message["idempotent_replay"] is False

    with client.websocket_connect("/v1/rooms/general/ws?username=Reader") as websocket:
        websocket.receive_json()
        recovered = websocket.receive_json()
    assert [item["content"] for item in recovered["items"]] == ["Hello room"]


def test_websocket_broadcasts_between_clients(client: TestClient, room: dict[str, str]) -> None:
    with client.websocket_connect("/v1/rooms/general/ws?username=Alice") as alice:
        alice.receive_json()
        alice.receive_json()
        with client.websocket_connect("/v1/rooms/general/ws?username=Bob") as bob:
            bob.receive_json()
            bob.receive_json()
            joined = alice.receive_json()
            bob.send_json({"type": "message", "content": "Hi Alice"})
            alice_message = alice.receive_json()
            bob_message = bob.receive_json()
        left = alice.receive_json()

    assert joined["type"] == "presence.joined"
    assert joined["username"] == "Bob"
    assert alice_message["message"] == bob_message["message"]
    assert alice_message["message"]["sender"] == "Bob"
    assert left["type"] == "presence.left"


def test_websocket_ping_invalid_input_and_idempotent_replay(client: TestClient, room: dict[str, str]) -> None:
    with client.websocket_connect("/v1/rooms/general/ws?username=Tester") as websocket:
        websocket.receive_json()
        websocket.receive_json()
        websocket.send_json({"type": "ping"})
        assert websocket.receive_json() == {"type": "pong"}
        websocket.send_text("not json")
        assert websocket.receive_json()["code"] == "invalid_command"
        websocket.send_json({"type": "message", "content": "x" * 65})
        assert websocket.receive_json()["code"] == "message_too_large"
        websocket.send_json({"type": "message", "content": "Once", "client_message_id": "same"})
        first = websocket.receive_json()
        websocket.send_json({"type": "message", "content": "Different", "client_message_id": "same"})
        replay = websocket.receive_json()

    assert first["message"] == replay["message"]
    assert replay["idempotent_replay"] is True


def test_websocket_rejects_repeated_binary_and_invalid_commands(client: TestClient, room: dict[str, str]) -> None:
    with pytest.raises(WebSocketDisconnect) as binary_close:
        with client.websocket_connect("/v1/rooms/general/ws?username=Binary") as websocket:
            websocket.receive_json()
            websocket.receive_json()
            for _ in range(3):
                websocket.send_bytes(b"not-text")
                websocket.receive_json()
            websocket.receive_json()
    assert binary_close.value.code == 1003

    with pytest.raises(WebSocketDisconnect) as invalid_close:
        with client.websocket_connect("/v1/rooms/general/ws?username=Invalid") as websocket:
            websocket.receive_json()
            websocket.receive_json()
            for _ in range(3):
                websocket.send_json({"type": "unknown"})
                websocket.receive_json()
            websocket.receive_json()
    assert invalid_close.value.code == 1008

    with pytest.raises(WebSocketDisconnect) as oversized_close:
        with client.websocket_connect("/v1/rooms/general/ws?username=Oversized") as websocket:
            websocket.receive_json()
            websocket.receive_json()
            websocket.send_text("x" * 17_000)
            assert websocket.receive_json()["code"] == "frame_too_large"
            websocket.receive_json()
    assert oversized_close.value.code == 1009


def test_websocket_authentication_handshake_and_origin_policy(tmp_path) -> None:
    authenticated = Settings(database_path=tmp_path / "auth.db", api_key="correct-horse-battery-staple")
    with TestClient(create_app(authenticated)) as client:
        client.post(
            "/v1/rooms",
            headers={"X-API-Key": "correct-horse-battery-staple"},
            json={"id": "secure", "name": "Secure"},
        )
        with client.websocket_connect("/v1/rooms/secure/ws?username=Browser") as websocket:
            assert websocket.receive_json()["type"] == "auth.required"
            websocket.send_json({"type": "auth", "api_key": "correct-horse-battery-staple"})
            assert websocket.receive_json()["type"] == "ready"
            assert websocket.receive_json()["type"] == "history"

        with client.websocket_connect(
            "/v1/rooms/secure/ws?username=Header",
            headers={"X-API-Key": "correct-horse-battery-staple"},
        ) as websocket:
            assert websocket.receive_json()["type"] == "ready"
            assert websocket.receive_json()["type"] == "history"

        with pytest.raises(WebSocketDisconnect) as rejected:
            with client.websocket_connect("/v1/rooms/secure/ws?username=Browser") as websocket:
                websocket.receive_json()
                websocket.send_json({"type": "auth", "api_key": "wrong"})
                websocket.receive_json()
                websocket.receive_json()
        assert rejected.value.code == 4401

    local_only = Settings(database_path=tmp_path / "local.db")
    with TestClient(create_app(local_only)) as client:
        with pytest.raises(WebSocketDisconnect) as rejected_origin:
            with client.websocket_connect(
                "/v1/rooms/unknown/ws?username=Browser",
                headers={"Origin": "https://evil.example"},
            ):
                pass
        assert rejected_origin.value.code == 4403


def test_missing_room_and_connection_capacity(client: TestClient, room: dict[str, str]) -> None:
    with pytest.raises(WebSocketDisconnect) as missing:
        with client.websocket_connect("/v1/rooms/missing/ws?username=A") as websocket:
            assert websocket.receive_json()["code"] == "room_not_found"
            websocket.receive_json()
    assert missing.value.code == 4404

    client.app.state.connections.max_per_room = 1
    with client.websocket_connect("/v1/rooms/general/ws?username=A") as first:
        first.receive_json()
        first.receive_json()
        with pytest.raises(WebSocketDisconnect) as full:
            with client.websocket_connect("/v1/rooms/general/ws?username=B") as second:
                assert second.receive_json()["code"] == "connection_capacity_reached"
                second.receive_json()
        assert full.value.code == 1013
