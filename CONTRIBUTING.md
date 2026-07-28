# Contributing

Helix Chat Engine is an alpha, single-instance chat service. Keep changes focused on its documented HTTP/WebSocket and SQLite product rather than reintroducing dependencies on private Helix repositories.

## Setup

```bash
python -m venv .venv
```

Activate `.venv\Scripts\Activate.ps1` on PowerShell or `source .venv/bin/activate` on POSIX, then install:

```bash
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[dev]"
```

## Required checks

```bash
python -m ruff check .
python -m ruff format --check .
python -m mypy helix_chat_engine
python -m pip_audit
python -m pytest --cov=helix_chat_engine --cov-report=term-missing
python -m build
python -m twine check dist/*
```

Add tests that exercise production code. Do not use mocks as a substitute for the primary SQLite, HTTP, or WebSocket behavior. Update `docs/API_REFERENCE.md` for protocol changes and `docs/PRODUCTIZATION.md` when a P0/P1 decision or release gate changes.

## Pull requests

- Explain the user problem and compatibility impact.
- Keep public behavior backward-compatible within the 0.2 protocol where practical.
- Never commit API keys, chat databases, message content, or generated build artifacts.
- Call out migration, retention, security, privacy, and operating-cost changes.
- Confirm that documentation describes behavior you ran, not planned behavior.

The existing repository license is owner/legal-review blocked as described in the README. Contributions do not resolve or reinterpret that gate.
