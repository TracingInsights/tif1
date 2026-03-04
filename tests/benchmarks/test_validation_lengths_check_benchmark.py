"""Benchmarks for telemetry length-consistency check logic."""

from __future__ import annotations

from typing import Any

import pytest


def _build_length_payload(
    size: int = 20_000, *, inconsistent: bool = False, sparse: bool = False
) -> dict[str, Any]:
    base = [float(i) for i in range(size)]
    reduced = base[:-1] if inconsistent else base
    maybe_empty: list[float] = [] if sparse else base

    return {
        "time": base,
        "speed": base,
        "rpm": [int(i) for i in base],
        "gear": [int(i % 8) for i in base],
        "throttle": maybe_empty,
        "brake": maybe_empty,
        "drs": maybe_empty,
        "distance": base,
        "rel_distance": maybe_empty,
        "x": base,
        "y": base,
        "z": base,
        "acc_x": reduced,
        "acc_y": reduced,
        "acc_z": reduced,
    }


def _legacy_lengths_check(payload: dict[str, Any]) -> bool:
    lengths = set()
    if payload["time"]:
        lengths.add(len(payload["time"]))
    if payload["speed"]:
        lengths.add(len(payload["speed"]))
    if payload["rpm"]:
        lengths.add(len(payload["rpm"]))
    if payload["gear"]:
        lengths.add(len(payload["gear"]))
    if payload["throttle"]:
        lengths.add(len(payload["throttle"]))
    if payload["brake"]:
        lengths.add(len(payload["brake"]))
    if payload["drs"]:
        lengths.add(len(payload["drs"]))
    if payload["distance"]:
        lengths.add(len(payload["distance"]))
    if payload["rel_distance"]:
        lengths.add(len(payload["rel_distance"]))
    if payload["x"]:
        lengths.add(len(payload["x"]))
    if payload["y"]:
        lengths.add(len(payload["y"]))
    if payload["z"]:
        lengths.add(len(payload["z"]))
    if payload["acc_x"]:
        lengths.add(len(payload["acc_x"]))
    if payload["acc_y"]:
        lengths.add(len(payload["acc_y"]))
    if payload["acc_z"]:
        lengths.add(len(payload["acc_z"]))

    return len(lengths) <= 1


def _candidate_lengths_check(payload: dict[str, Any]) -> bool:
    lengths = {
        len(values)
        for values in (
            payload["time"],
            payload["speed"],
            payload["rpm"],
            payload["gear"],
            payload["throttle"],
            payload["brake"],
            payload["drs"],
            payload["distance"],
            payload["rel_distance"],
            payload["x"],
            payload["y"],
            payload["z"],
            payload["acc_x"],
            payload["acc_y"],
            payload["acc_z"],
        )
        if values
    }
    return len(lengths) <= 1


def _candidate_first_len_check(payload: dict[str, Any]) -> bool:
    first_len: int | None = None
    for values in (
        payload["time"],
        payload["speed"],
        payload["rpm"],
        payload["gear"],
        payload["throttle"],
        payload["brake"],
        payload["drs"],
        payload["distance"],
        payload["rel_distance"],
        payload["x"],
        payload["y"],
        payload["z"],
        payload["acc_x"],
        payload["acc_y"],
        payload["acc_z"],
    ):
        if not values:
            continue
        current_len = len(values)
        if first_len is None:
            first_len = current_len
        elif current_len != first_len:
            return False
    return True


def test_lengths_check_parity_consistent():
    payload = _build_length_payload()
    assert _candidate_lengths_check(payload) == _legacy_lengths_check(payload)
    assert _candidate_first_len_check(payload) == _legacy_lengths_check(payload)


def test_lengths_check_parity_inconsistent():
    payload = _build_length_payload(inconsistent=True)
    assert _candidate_lengths_check(payload) == _legacy_lengths_check(payload)
    assert _candidate_first_len_check(payload) == _legacy_lengths_check(payload)


def test_lengths_check_parity_sparse():
    payload = _build_length_payload(sparse=True)
    assert _candidate_lengths_check(payload) == _legacy_lengths_check(payload)
    assert _candidate_first_len_check(payload) == _legacy_lengths_check(payload)


@pytest.mark.benchmark(group="validation_lengths_check")
class TestValidationLengthsCheckBenchmark:
    def test_legacy_consistent(self, benchmark):
        payload = _build_length_payload()
        result = benchmark(_legacy_lengths_check, payload)
        assert result is True

    def test_candidate_consistent(self, benchmark):
        payload = _build_length_payload()
        result = benchmark(_candidate_lengths_check, payload)
        assert result is True

    def test_legacy_sparse(self, benchmark):
        payload = _build_length_payload(sparse=True)
        result = benchmark(_legacy_lengths_check, payload)
        assert result is True

    def test_candidate_sparse(self, benchmark):
        payload = _build_length_payload(sparse=True)
        result = benchmark(_candidate_lengths_check, payload)
        assert result is True

    def test_candidate_first_len_consistent(self, benchmark):
        payload = _build_length_payload()
        result = benchmark(_candidate_first_len_check, payload)
        assert result is True

    def test_candidate_first_len_sparse(self, benchmark):
        payload = _build_length_payload(sparse=True)
        result = benchmark(_candidate_first_len_check, payload)
        assert result is True
