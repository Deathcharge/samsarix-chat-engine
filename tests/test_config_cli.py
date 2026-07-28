"""Configuration, CLI safety, and public-package tests."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import helix_chat_engine
from helix_chat_engine.cli import build_parser, main
from helix_chat_engine.config import ConfigurationError, Settings


def test_public_api_and_parser_help() -> None:
    assert helix_chat_engine.__version__ == "0.2.0"
    assert callable(helix_chat_engine.create_app)
    help_text = build_parser().format_help()
    assert "serve" in help_text
    assert "local-first" in help_text


def test_settings_from_env_and_validation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HELIX_CHAT_DATABASE", str(tmp_path / "configured.db"))
    monkeypatch.setenv("HELIX_CHAT_ALLOWED_ORIGINS", "https://one.example/, https://two.example")
    monkeypatch.setenv("HELIX_CHAT_MAX_MESSAGE_CHARS", "123")
    settings = Settings.from_env()

    assert settings.database_path == tmp_path / "configured.db"
    assert settings.allowed_origins == ("https://one.example", "https://two.example")
    assert settings.max_message_chars == 123

    monkeypatch.setenv("HELIX_CHAT_MAX_CONNECTIONS", "not-a-number")
    with pytest.raises(ConfigurationError, match="must be an integer"):
        Settings.from_env()
    with pytest.raises(ConfigurationError, match="between 16 and 4096"):
        Settings(api_key="short")
    with pytest.raises(ConfigurationError, match="cannot exceed"):
        Settings(max_connections=1, max_connections_per_room=2)
    with pytest.raises(ConfigurationError, match="exact http"):
        Settings(allowed_origins=("https://chat.example/path",))

    monkeypatch.delenv("HELIX_CHAT_MAX_CONNECTIONS")
    monkeypatch.setenv("HELIX_CHAT_WS_AUTH_TIMEOUT", "slow")
    with pytest.raises(ConfigurationError, match="must be a number"):
        Settings.from_env()


def test_cli_refuses_unauthenticated_public_bind(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HELIX_CHAT_API_KEY", raising=False)
    with pytest.raises(SystemExit) as exit_info:
        main(["serve", "--host", "0.0.0.0"])
    assert exit_info.value.code == 2


def test_cli_help_and_version_ignore_invalid_runtime_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HELIX_CHAT_MAX_CONNECTIONS", "invalid")
    with pytest.raises(SystemExit) as help_exit:
        main(["--help"])
    with pytest.raises(SystemExit) as version_exit:
        main(["--version"])
    assert help_exit.value.code == 0
    assert version_exit.value.code == 0


def test_cli_passes_safe_configuration_to_uvicorn(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[tuple[object, dict[str, object]]] = []

    def fake_run(application: object, **kwargs: object) -> None:
        calls.append((application, kwargs))

    monkeypatch.setenv("HELIX_CHAT_API_KEY", "correct-horse-battery-staple")
    monkeypatch.setitem(sys.modules, "uvicorn", SimpleNamespace(run=fake_run))
    result = main(
        [
            "serve",
            "--host",
            "0.0.0.0",
            "--port",
            "8765",
            "--database",
            str(tmp_path / "cli.db"),
        ]
    )

    assert result == 0
    assert calls[0][1]["host"] == "0.0.0.0"
    assert calls[0][1]["port"] == 8765
    assert calls[0][1]["ws_max_size"] == 16_384
