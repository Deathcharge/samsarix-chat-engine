"""Configuration, CLI safety, and public-package tests."""

import importlib
import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import samsarix_chat_engine
from samsarix_chat_engine import create_app
from samsarix_chat_engine.cli import build_parser, main
from samsarix_chat_engine.config import ConfigurationError, Settings


def test_public_api_and_parser_help() -> None:
    assert samsarix_chat_engine.__version__ == "0.6.0"
    assert callable(samsarix_chat_engine.create_app)
    help_text = build_parser().format_help()
    assert "serve" in help_text
    assert "local-first" in help_text


def test_settings_from_env_and_validation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SAMSARIX_CHAT_DATABASE", str(tmp_path / "configured.db"))
    monkeypatch.setenv("SAMSARIX_CHAT_ALLOWED_ORIGINS", "https://one.example/, https://two.example")
    monkeypatch.setenv("SAMSARIX_CHAT_MAX_MESSAGE_CHARS", "123")
    monkeypatch.setenv("SAMSARIX_CHAT_MESSAGE_RETENTION_DAYS", "30")
    monkeypatch.setenv("SAMSARIX_CHAT_MAX_AUDIT_EVENTS", "500")
    settings = Settings.from_env()

    assert settings.database_path == tmp_path / "configured.db"
    assert settings.allowed_origins == ("https://one.example", "https://two.example")
    assert settings.max_message_chars == 123
    assert settings.message_retention_days == 30
    assert settings.max_audit_events == 500

    monkeypatch.setenv("SAMSARIX_CHAT_MAX_CONNECTIONS", "not-a-number")
    with pytest.raises(ConfigurationError, match="must be an integer"):
        Settings.from_env()
    with pytest.raises(ConfigurationError, match="between 16 and 4096"):
        Settings(api_key="short")
    with pytest.raises(ConfigurationError, match="cannot exceed"):
        Settings(max_connections=1, max_connections_per_room=2)
    with pytest.raises(ConfigurationError, match="exact http"):
        Settings(allowed_origins=("https://chat.example/path",))
    with pytest.raises(ConfigurationError, match="between 1 and 3650"):
        Settings(message_retention_days=0)
    monkeypatch.setenv("SAMSARIX_CHAT_MESSAGE_RETENTION_DAYS", "not-a-number")
    with pytest.raises(ConfigurationError, match="must be an integer"):
        Settings.from_env()
    monkeypatch.setenv("SAMSARIX_CHAT_MESSAGE_RETENTION_DAYS", "30")

    monkeypatch.delenv("SAMSARIX_CHAT_MAX_CONNECTIONS")
    monkeypatch.setenv("SAMSARIX_CHAT_WS_AUTH_TIMEOUT", "slow")
    with pytest.raises(ConfigurationError, match="must be a number"):
        Settings.from_env()


def test_cli_refuses_unauthenticated_public_bind(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SAMSARIX_CHAT_API_KEY", raising=False)
    with pytest.raises(SystemExit) as exit_info:
        main(["serve", "--host", "0.0.0.0"])
    assert exit_info.value.code == 2


def test_cli_help_and_version_ignore_invalid_runtime_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SAMSARIX_CHAT_MAX_CONNECTIONS", "invalid")
    with pytest.raises(SystemExit) as help_exit:
        main(["--help"])
    with pytest.raises(SystemExit) as version_exit:
        main(["--version"])
    assert help_exit.value.code == 0
    assert version_exit.value.code == 0


def test_legacy_cli_name_warns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["helix-chat", "--version"])
    with pytest.warns(FutureWarning, match="use samsarix-chat"):
        with pytest.raises(SystemExit) as exit_info:
            main()
    assert exit_info.value.code == 0


def test_cli_passes_safe_configuration_to_uvicorn(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[tuple[object, dict[str, object]]] = []

    def fake_run(application: object, **kwargs: object) -> None:
        calls.append((application, kwargs))

    monkeypatch.setenv("SAMSARIX_CHAT_API_KEY", "correct-horse-battery-staple")
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


def test_cli_backup_and_restore_use_consistent_snapshots(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source = tmp_path / "source.db"
    backup = tmp_path / "backups" / "snapshot.db"
    restored = tmp_path / "restored.db"
    with closing(sqlite3.connect(source)) as connection, connection:
        connection.execute("CREATE TABLE values_for_test (value TEXT NOT NULL)")
        connection.execute("INSERT INTO values_for_test VALUES ('preserved')")

    assert main(["database", "--database", str(source), "backup", str(backup)]) == 0
    assert Path(capsys.readouterr().out.strip()) == backup.resolve()
    assert main(["database", "--database", str(restored), "restore", str(backup)]) == 0
    with closing(sqlite3.connect(restored)) as connection:
        assert connection.execute("SELECT value FROM values_for_test").fetchone()[0] == "preserved"

    with pytest.raises(SystemExit) as protected:
        main(["database", "--database", str(restored), "restore", str(backup)])
    assert protected.value.code == 2

    with TestClient(create_app(Settings(database_path=restored))):
        with pytest.raises(SystemExit) as active:
            main(["database", "--database", str(restored), "restore", str(backup), "--replace"])
    assert active.value.code == 2

    Path(f"{restored}-wal").write_bytes(b"stale-wal")
    Path(f"{restored}-shm").write_bytes(b"stale-shm")
    assert main(["database", "--database", str(restored), "restore", str(backup), "--replace"]) == 0
    assert not Path(f"{restored}-wal").exists()
    assert not Path(f"{restored}-shm").exists()


def test_legacy_import_and_environment_aliases(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    sys.modules.pop("helix_chat_engine", None)
    with pytest.warns(DeprecationWarning, match="import samsarix_chat_engine"):
        legacy_package = importlib.import_module("helix_chat_engine")
    assert legacy_package.Settings is Settings
    assert legacy_package.__version__ == "0.6.0"
    assert importlib.import_module("helix_chat_engine.app").create_app is samsarix_chat_engine.create_app
    assert importlib.import_module("helix_chat_engine.cli").main is main
    assert importlib.import_module("helix_chat_engine.config").Settings is Settings
    assert importlib.import_module("helix_chat_engine.models").Room is samsarix_chat_engine.Room
    assert importlib.import_module("helix_chat_engine.store").ChatStore is samsarix_chat_engine.ChatStore
    assert (
        importlib.import_module("helix_chat_engine.websocket_manager").ConnectionManager
        is samsarix_chat_engine.ConnectionManager
    )

    monkeypatch.setenv("HELIX_CHAT_DATABASE", str(tmp_path / "legacy.db"))
    with pytest.warns(FutureWarning, match="SAMSARIX_CHAT_DATABASE"):
        settings = Settings.from_env()
    assert settings.database_path == tmp_path / "legacy.db"


def test_canonical_environment_takes_precedence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SAMSARIX_CHAT_DATABASE", str(tmp_path / "canonical.db"))
    monkeypatch.setenv("HELIX_CHAT_DATABASE", str(tmp_path / "legacy.db"))
    with pytest.warns(FutureWarning, match="ignored"):
        settings = Settings.from_env()
    assert settings.database_path == tmp_path / "canonical.db"


def test_legacy_default_database_is_reused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SAMSARIX_CHAT_DATABASE", raising=False)
    monkeypatch.delenv("HELIX_CHAT_DATABASE", raising=False)
    monkeypatch.chdir(tmp_path)
    legacy_database = tmp_path / "data" / "helix-chat.db"
    legacy_database.parent.mkdir()
    legacy_database.touch()

    with pytest.warns(FutureWarning, match="Using legacy database"):
        settings = Settings.from_env()

    assert settings.database_path == Path("data/helix-chat.db")
