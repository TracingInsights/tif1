"""Switch test patches from _ensure_polars_available to _ensure_polars_bound."""

from pathlib import Path

for path, subs in (
    (
        "tests/unit/test_backend_conversion.py",
        [
            (
                "# lazily-resolved _ensure_polars_available() (POLARS_AVAILABLE was a",
                "# lazily-resolved _ensure_polars_bound() (POLARS_AVAILABLE was a",
            ),
            (
                'patch.object(bc_module, "_ensure_polars_available", return_value=False)',
                'patch.object(bc_module, "_ensure_polars_bound", return_value=False)',
            ),
        ],
    ),
    (
        "tests/unit/test_coverage_shims_and_utils.py",
        [
            (
                'monkeypatch.setattr(conv, "_ensure_polars_available", lambda: False)',
                'monkeypatch.setattr(conv, "_ensure_polars_bound", lambda: False)',
            ),
            (
                'monkeypatch.setattr(conv, "_ensure_polars_available", lambda: True)',
                'monkeypatch.setattr(conv, "_ensure_polars_bound", lambda: True)',
            ),
        ],
    ),
):
    p = Path(path)
    text = p.read_text()
    for old, new in subs:
        text = text.replace(old, new)
    p.write_text(text)
    print(f"updated {path}")
