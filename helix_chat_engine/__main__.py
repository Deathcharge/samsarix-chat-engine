# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Run the legacy module entry point through the Samsarix CLI."""

import warnings

from samsarix_chat_engine.cli import main

warnings.warn(
    "python -m helix_chat_engine is deprecated; use python -m samsarix_chat_engine instead",
    FutureWarning,
    stacklevel=1,
)

raise SystemExit(main())
