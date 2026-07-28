"""Allow ``python -m helix_chat_engine`` to behave like ``helix-chat``."""

from .cli import main

raise SystemExit(main())
