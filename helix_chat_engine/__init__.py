# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Backward-compatible import alias for :mod:`samsarix_chat_engine`."""

import warnings

warnings.warn(
    "helix_chat_engine is deprecated; import samsarix_chat_engine instead",
    DeprecationWarning,
    stacklevel=2,
)

from samsarix_chat_engine import *  # noqa: E402,F401,F403
from samsarix_chat_engine import __all__, __version__  # noqa: E402,F401
