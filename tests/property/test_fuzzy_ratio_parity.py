"""The pure-Python fuzzy ratio (Pyodide fallback) scores exactly like rapidfuzz."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tif1 import fuzzy

rapidfuzz_fuzz = pytest.importorskip("rapidfuzz.fuzz")


@settings(max_examples=300, deadline=None)
@given(st.text(max_size=40), st.text(max_size=40))
def test_indel_ratio_matches_rapidfuzz(a: str, b: str) -> None:
    assert fuzzy._indel_ratio(a, b) == pytest.approx(rapidfuzz_fuzz.ratio(a, b), abs=1e-9)


def test_indel_ratio_edge_cases() -> None:
    assert fuzzy._indel_ratio("", "") == 100.0
    assert fuzzy._indel_ratio("abc", "") == 0.0
    assert fuzzy._indel_ratio("bahrain", "bahrain") == 100.0
