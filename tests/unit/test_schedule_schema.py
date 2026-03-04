"""Tests for schedule schema validation."""

import pytest

from tif1.exceptions import InvalidDataError
from tif1.schedule_schema import validate_schedule_payload


def test_validate_schedule_payload_accepts_packaged_data():
    from tif1.events import _load_vendored_f1schedule_years

    years = _load_vendored_f1schedule_years()
    payload = {"schema_version": 1, "years": years}

    validated = validate_schedule_payload(payload)
    assert validated["schema_version"] == 1
    assert "2025" in validated["years"]
    assert "2018" in validated["years"]


def test_validate_schedule_payload_rejects_bad_version():
    with pytest.raises(InvalidDataError, match="Unsupported schedule schema version"):
        validate_schedule_payload({"schema_version": 2, "years": {}})


def test_validate_schedule_payload_rejects_missing_event_sessions():
    bad_payload = {
        "schema_version": 1,
        "years": {"2025": {"events": ["Abu Dhabi Grand Prix"], "sessions": {}}},
    }
    with pytest.raises(InvalidDataError, match="Invalid session list"):
        validate_schedule_payload(bad_payload)
