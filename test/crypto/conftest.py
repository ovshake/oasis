"""Conftest for crypto tests.

The OASIS root ``oasis/__init__.py`` imports torch and other heavy deps
that aren't needed for the crypto subpackage. We register ``oasis`` and
``oasis.crypto`` as lightweight namespace stubs before any test import
so that ``from oasis.crypto.persona import ...`` works without pulling
in the full OASIS dependency tree.
"""

import importlib
import sys
import types
from pathlib import Path

# Project root — two levels up from test/crypto/
_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _ensure_light_oasis() -> None:
    """Register oasis + oasis.crypto as lightweight namespace packages."""
    oasis_dir = _PROJECT_ROOT / "oasis"
    crypto_dir = oasis_dir / "crypto"

    # Only patch if oasis hasn't been fully imported yet (i.e., torch etc.
    # are not available). If the full OASIS env is present, let it be.
    if "oasis" in sys.modules:
        return

    # Create a minimal oasis namespace module
    oasis_mod = types.ModuleType("oasis")
    oasis_mod.__path__ = [str(oasis_dir)]
    oasis_mod.__package__ = "oasis"
    sys.modules["oasis"] = oasis_mod

    # Create oasis.crypto subpackage from its real __init__
    crypto_mod = types.ModuleType("oasis.crypto")
    crypto_mod.__path__ = [str(crypto_dir)]
    crypto_mod.__package__ = "oasis.crypto"
    sys.modules["oasis.crypto"] = crypto_mod


_ensure_light_oasis()
