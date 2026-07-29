# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Allow ``python -m samsarix_chat_engine`` to behave like ``samsarix-chat``."""

from .cli import main

raise SystemExit(main())
