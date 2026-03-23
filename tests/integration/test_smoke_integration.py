"""Tests d'intégration — services réels, réseau, base, etc."""

import pytest

pytestmark = pytest.mark.integration


def test_smoke() -> None:
    """Ensure the integration test suite runs without error."""
    assert True
