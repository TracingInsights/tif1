"""The Windows deadline override in ``tests/conftest.py`` reaches Hypothesis.

``drop_hypothesis_deadline`` (through the ``hypothesis_deadline_dropper``
fixture) replaces a private Hypothesis attribute; this
test fails when a Hypothesis release stops reading it, so the override never
breaks silently.
"""

from __future__ import annotations

import time

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis.errors import DeadlineExceeded


def _slow_property():
    @given(st.just(0))
    @settings(max_examples=1, deadline=1, derandomize=True, database=None)
    def slow(_value: int) -> None:
        time.sleep(0.05)

    return slow


def test_the_deadline_applies_without_the_override() -> None:
    with pytest.raises(DeadlineExceeded):
        _slow_property()()


def test_the_override_removes_the_deadline(hypothesis_deadline_dropper) -> None:
    slow = _slow_property()
    assert hypothesis_deadline_dropper(slow)
    slow()  # 50 ms against a 1 ms deadline: passes only without one


def test_a_plain_function_is_left_alone(hypothesis_deadline_dropper) -> None:
    def plain() -> None:
        pass

    assert not hypothesis_deadline_dropper(plain)
