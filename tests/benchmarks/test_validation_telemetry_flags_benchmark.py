"""Benchmarks for validate_telemetry flag conversion path."""

from __future__ import annotations

from typing import Any, Literal

import pytest

from tif1.validation import TelemetryData, validate_telemetry


def _build_payload(size: int = 20_000, flag_type: Literal["int", "bool"] = "int") -> dict[str, Any]:
    time_values = [i * 0.1 for i in range(size)]
    speed_values = [100.0 + (i % 250) for i in range(size)]
    if flag_type == "bool":
        brake_values = [bool(i % 2) if i % 11 else None for i in range(size)]
        drs_values = [bool((i + 1) % 2) if i % 13 else None for i in range(size)]
    else:
        brake_values = [(i % 2) if i % 11 else None for i in range(size)]
        drs_values = [((i + 1) % 2) if i % 13 else None for i in range(size)]

    return {
        "time": time_values,
        "speed": speed_values,
        "brake": brake_values,
        "drs": drs_values,
    }


def _legacy_validate_telemetry(data: dict[str, Any]) -> TelemetryData:
    tel_data = data.copy()
    if "brake" in tel_data:
        tel_data["brake"] = [bool(v) if v is not None else None for v in tel_data["brake"]]
    if "drs" in tel_data:
        tel_data["drs"] = [bool(v) if v is not None else None for v in tel_data["drs"]]
    return TelemetryData.model_validate(tel_data)


def _coerce_optional_bool_list(values: list[Any]) -> list[Any]:
    if not values:
        return values

    for value in values:
        if value is not None and not isinstance(value, bool):
            return [bool(v) if v is not None else None for v in values]
    return values


def _candidate_validate_telemetry(data: dict[str, Any]) -> TelemetryData:
    tel_data: dict[str, Any] = data

    brake_values = data.get("brake")
    if isinstance(brake_values, list):
        converted_brake = _coerce_optional_bool_list(brake_values)
        if converted_brake is not brake_values:
            tel_data = data.copy()
            tel_data["brake"] = converted_brake

    drs_values = data.get("drs")
    if isinstance(drs_values, list):
        converted_drs = _coerce_optional_bool_list(drs_values)
        if converted_drs is not drs_values:
            if tel_data is data:
                tel_data = data.copy()
            tel_data["drs"] = converted_drs

    return TelemetryData.model_validate(tel_data)


def test_validate_telemetry_candidate_parity_int_flags():
    payload = _build_payload(2000, "int")
    legacy = _legacy_validate_telemetry(payload).model_dump()
    candidate = _candidate_validate_telemetry(payload).model_dump()
    assert candidate == legacy


def test_validate_telemetry_candidate_parity_bool_flags():
    payload = _build_payload(2000, "bool")
    legacy = _legacy_validate_telemetry(payload).model_dump()
    candidate = _candidate_validate_telemetry(payload).model_dump()
    assert candidate == legacy


@pytest.mark.benchmark(group="validation_telemetry_flags")
class TestValidationTelemetryFlagsBenchmark:
    def test_legacy_int_flags(self, benchmark):
        payload = _build_payload(flag_type="int")
        result = benchmark(_legacy_validate_telemetry, payload)
        assert len(result.time) == len(payload["time"])

    def test_candidate_int_flags(self, benchmark):
        payload = _build_payload(flag_type="int")
        result = benchmark(_candidate_validate_telemetry, payload)
        assert len(result.time) == len(payload["time"])

    def test_production_int_flags(self, benchmark):
        payload = _build_payload(flag_type="int")
        result = benchmark(validate_telemetry, payload)
        assert len(result.time) == len(payload["time"])

    def test_legacy_bool_flags(self, benchmark):
        payload = _build_payload(flag_type="bool")
        result = benchmark(_legacy_validate_telemetry, payload)
        assert len(result.time) == len(payload["time"])

    def test_candidate_bool_flags(self, benchmark):
        payload = _build_payload(flag_type="bool")
        result = benchmark(_candidate_validate_telemetry, payload)
        assert len(result.time) == len(payload["time"])

    def test_production_bool_flags(self, benchmark):
        payload = _build_payload(flag_type="bool")
        result = benchmark(validate_telemetry, payload)
        assert len(result.time) == len(payload["time"])
