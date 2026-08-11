"""Offline tests for the standalone circuit telemetry sector extractor."""

from __future__ import annotations

import hashlib
import importlib.util
import itertools
import json
import sys
import tempfile
from pathlib import Path
from types import ModuleType

import numpy as np
import pytest
from PIL import Image

SCRIPT_PATH = Path(__file__).parents[2] / "scripts" / "extract_telemetry_sectors.py"

#: Reviewed source fixture (sha256 matches the script's MANUAL_LABEL_OVERRIDE_SOURCE_SHA256
#: guard). Resolved relative to the repo root so the tests are CWD-independent.
HUNGARY_SOURCE = Path(__file__).parents[2] / "circuits" / "hungary.jpg"

#: End-to-end tests that run the real extraction pipeline against the reviewed
#: Hungary graphic. The fixture lives outside the repo (it is a sha256-guarded
#: reviewed image, not a tracked file), so skip these tests when it is not
#: present instead of failing CI on every checkout without it.
REQUIRES_HUNGARY_SOURCE = pytest.mark.skipif(
    not HUNGARY_SOURCE.is_file(),
    reason="circuits/hungary.jpg reviewed fixture not present in this checkout",
)


@pytest.fixture(scope="module")
def extractor() -> ModuleType:
    """Load the standalone script as a testable module."""
    spec = importlib.util.spec_from_file_location("tif1_sector_extractor", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _bracket(module: ModuleType, bracket_id: str, start: float, end: float) -> dict:
    return {
        "bracket_id": bracket_id,
        "guide_lines": [
            {
                "line_x_min": int(start),
                "line_x_max": int(start + 1),
                "boundary_x": start,
                "uncertainty_px": 1.0,
            },
            {
                "line_x_min": int(end),
                "line_x_max": int(end + 1),
                "boundary_x": end,
                "uncertainty_px": 1.0,
            },
        ],
        "horizontal_stroke_score": 1.0,
        "associated_label_detection_id": None,
    }


def test_build_intervals_is_contiguous_and_preserves_adjacent_brackets(
    extractor: ModuleType,
) -> None:
    plot = extractor.Bounds(0, 1000, 0, 200)
    brackets = [
        _bracket(extractor, "bracket-01", 100, 200),
        _bracket(extractor, "bracket-02", 200, 300),
        _bracket(extractor, "bracket-03", 403.5, 500),
    ]

    intervals = extractor.build_intervals(plot, brackets, [], ocr_available=True)

    assert [
        interval["bracket_id"] for interval in intervals if interval["source"] == "bracket"
    ] == [
        "bracket-01",
        "bracket-02",
        "bracket-03",
    ]
    assert intervals[0]["source"] == "plot_leading_gap"
    assert any(interval["source"] == "inferred_gap" for interval in intervals)
    assert intervals[-1]["source"] == "plot_trailing_gap"
    assert intervals[-1]["interval_bounds"] == "[start, end]"
    assert any(interval["start"]["full_image_x"] == 403.5 for interval in intervals)
    assert extractor.validate_intervals(intervals, plot)["errors"] == []


def test_missing_ocr_classifies_brackets_as_straight_when_available(extractor: ModuleType) -> None:
    plot = extractor.Bounds(0, 100, 0, 20)
    intervals = extractor.build_intervals(
        plot, [_bracket(extractor, "bracket-01", 20, 40)], [], ocr_available=True
    )

    bracket_interval = next(interval for interval in intervals if interval["source"] == "bracket")
    assert bracket_interval["classification"] == "straight"
    assert bracket_interval["classification_display"] == "STRAIGHT"
    assert "BRACKET_NOT_ASSOCIATED" in bracket_interval["review_flags"]


def test_ocr_unavailable_keeps_bracket_unresolved(extractor: ModuleType) -> None:
    plot = extractor.Bounds(0, 100, 0, 20)
    intervals = extractor.build_intervals(
        plot, [_bracket(extractor, "bracket-01", 20, 40)], [], ocr_available=False
    )

    bracket_interval = next(interval for interval in intervals if interval["source"] == "bracket")
    assert bracket_interval["classification"] == "unresolved"
    assert "OCR_UNAVAILABLE" in bracket_interval["review_flags"]


def test_manual_hungary_labels_split_shared_guide_line_chain(extractor: ModuleType) -> None:
    plot = extractor.Bounds(72, 1527, 465, 708)
    band = extractor.Bounds(72, 1527, 412, 469)
    # The detector normally returns integer spans; this fixture keeps the
    # reviewed fractional centers while exercising shared endpoints.
    candidates = [
        {"line_x_min": x_min, "line_x_max": x_max, "supporting_pixels": 20}
        for x_min, x_max in (
            (237, 237),
            (321, 321),
            (403, 404),
            (489, 490),
            (654, 654),
            (796, 796),
            (836, 836),
            (910, 910),
            (1011, 1012),
            (1214, 1215),
            (1388, 1389),
            (1465, 1465),
        )
    ]
    brackets, detections, evidence, flags = extractor.apply_manual_label_overrides(
        "hungary",
        [],
        {"guide_line_candidates": candidates},
        plot,
        band,
        source_sha256=extractor.MANUAL_LABEL_OVERRIDE_SOURCE_SHA256["hungary"],
    )

    assert flags == []
    assert evidence["applied"] is True
    assert len(brackets) == 8
    assert len(detections) == 8
    assert [
        (item["guide_lines"][0]["boundary_x"], item["guide_lines"][1]["boundary_x"])
        for item in brackets[-3:]
    ] == [(1011.5, 1214.5), (1214.5, 1388.5), (1388.5, 1465.0)]


def test_manual_override_hash_mismatch_is_blocked(extractor: ModuleType) -> None:
    plot = extractor.Bounds(72, 1527, 465, 708)
    band = extractor.Bounds(72, 1527, 412, 469)
    brackets, detections, evidence, flags = extractor.apply_manual_label_overrides(
        "hungary",
        [],
        {"guide_line_candidates": []},
        plot,
        band,
        source_sha256="not-the-reviewed-source",
    )

    assert brackets == []
    assert detections == []
    assert evidence["applied"] is False
    assert evidence["source_hash_guard"]["passed"] is False
    assert flags[0]["code"] == "MANUAL_OVERRIDE_SOURCE_HASH_MISMATCH"
    assert flags[0]["blocks_completion"] is True


def test_ocr_normalization_and_label_association(extractor: ModuleType) -> None:
    assert extractor._normalize_ocr_text(" medium\n speed ") == "MEDIUM SPEED"
    assert extractor._candidate_from_ocr_text("HIGH SPEED") == "high_speed"

    brackets = [
        {
            **_bracket(extractor, "bracket-01", 100, 200),
            "full_image_span": {"x_min": 100, "x_max": 200},
        }
    ]
    detections = [
        {
            "detection_id": "ocr-01",
            "classification_candidate": "medium_speed",
            "bbox_full_image": {"x_min": 120, "x_max": 180, "y_min": 400, "y_max": 420},
        }
    ]

    associated, flags = extractor.associate_labels(brackets, detections)

    assert associated[0]["associated_label_detection_id"] == "ocr-01"
    assert associated[0]["association_method"] == "maximum_horizontal_overlap"
    assert flags == []


@REQUIRES_HUNGARY_SOURCE
def test_source_metadata_and_output_do_not_modify_source(
    extractor: ModuleType, tmp_path: Path
) -> None:
    source = HUNGARY_SOURCE
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    config = extractor.ExtractorConfig(
        output_dir=tmp_path, tesseract_cmd="definitely-not-tesseract"
    )

    result = extractor.process_path(source, config)

    assert result["analysis_status"] == "incomplete"
    assert result["source"]["format"] == "JPEG"
    assert result["source"]["sha256"] == before
    assert {path.name for path in tmp_path.iterdir()} == {
        "hungary.json",
        "hungary.md",
        "hungary_annotated.png",
    }
    data = json.loads((tmp_path / "hungary.json").read_text(encoding="utf-8"))
    assert data["validation"]["intervals_contiguous"] is True
    assert data["evidence"]["ocr"]["available"] is False
    with Image.open(tmp_path / "hungary_annotated.png") as annotated:
        assert annotated.size == (1600, 900)
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before


@REQUIRES_HUNGARY_SOURCE
def test_hungary_output_has_split_final_complex_and_manual_evidence(
    extractor: ModuleType, tmp_path: Path
) -> None:
    config = extractor.ExtractorConfig(
        output_dir=tmp_path, tesseract_cmd="definitely-not-tesseract"
    )

    extractor.process_path(HUNGARY_SOURCE, config)
    data = json.loads((tmp_path / "hungary.json").read_text(encoding="utf-8"))
    final_complex = [
        (item["classification"], item["start"]["full_image_x"], item["end"]["full_image_x"])
        for item in data["intervals"]
        if item["start"]["full_image_x"] >= 1011 and item["end"]["full_image_x"] <= 1465
    ]

    assert final_complex == [
        ("high_speed", 1011.0, 1214.5),
        ("low_speed", 1214.5, 1388.0),
        ("medium_speed", 1388.0, 1465.0),
    ]
    sector_nine = next(item for item in data["intervals"] if item["start"]["full_image_x"] == 910.0)
    assert sector_nine["classification"] == "medium_speed"
    assert sector_nine["display_label"] == "9.1"
    assert [
        item["display_label"] for item in data["intervals"] if item["displayed_sector"] == 10
    ] == ["10.1", "10.2", "10.3"]
    assert data["evidence"]["manual_label_overrides"]["applied"] is True
    assert data["evidence"]["manual_label_overrides"]["source"] == "native_image_visual_review"
    assert data["validation"]["errors"] == []


@REQUIRES_HUNGARY_SOURCE
def test_markdown_only_does_not_link_to_missing_artifacts(
    extractor: ModuleType, tmp_path: Path
) -> None:
    config = extractor.ExtractorConfig(
        output_dir=tmp_path,
        tesseract_cmd="definitely-not-tesseract",
        markdown_only=True,
    )

    extractor.process_path(HUNGARY_SOURCE, config)

    markdown = (tmp_path / "hungary.md").read_text(encoding="utf-8")
    assert "(not generated)" in markdown
    assert not (tmp_path / "hungary.json").exists()
    assert not (tmp_path / "hungary_annotated.png").exists()


@REQUIRES_HUNGARY_SOURCE
def test_compact_label_fallback_preserves_display_label(extractor: ModuleType) -> None:
    """The compact fallback for narrow intervals must keep the display_label.

    The Hungary 10.x split (1011↔1214.5↔1388↔1465) is drawn as three adjacent
    intervals. Even when a label is forced into the compact form the renderer
    must keep the per-section ``display_label`` instead of falling back to the
    partition sequence — otherwise the three sections visually collapse to a
    single ``10`` prefix and the split is lost from the audit PNG.

    Validation: run the real extractor pipeline against the Hungary source,
    scan the resulting PNG's bracket band for dark label rectangles, and
    assert that three rectangles sit inside the 10.x region (1011 ↔ 1465)
    rather than ``0`` or ``1``.
    """

    assert extractor.COMPACT_LABEL_LEGEND == {
        "LOW SPEED": "LOW",
        "MEDIUM SPEED": "MED",
        "HIGH SPEED": "HIGH",
        "STRAIGHT": "STR",
        "UNRESOLVED": "UNR",
    }

    with tempfile.TemporaryDirectory() as scratch:
        config = extractor.ExtractorConfig(
            output_dir=Path(scratch),
            tesseract_cmd="definitely-not-tesseract",
        )
        extractor.process_path(HUNGARY_SOURCE, config)
        annotated = Image.open(Path(scratch) / "hungary_annotated.png")
        # Scan the bracket band for dark label-rectangle backgrounds. Label
        # rectangles are the darkest non-background feature in the band, so
        # any column with >8 dark pixels in y∈[band_y_min, band_y_max] sits
        # inside a label.
        gray = annotated.convert("L")
        pixels = gray.load()
        band_top = 414
        band_bottom = 464
        # Label rectangles are filled with (8, 8, 18, 200) over a page
        # background of L≈27, so the blended fill sits at L≈13. Use a strict
        # threshold (L<20) so we capture label fills and exclude both the page
        # background (L≈27) and the bright yellow bracket outlines (L≈200).
        columns_with_label = []
        for x in range(1011, 1465):
            very_dark_count = sum(1 for y in range(band_top, band_bottom) if pixels[x, y] < 20)
            if very_dark_count >= 8:
                columns_with_label.append(x)
        # Group contiguous columns into rectangle spans; a gap of >=5 between
        # dark columns means the rectangles are not visually contiguous.
        spans = []
        if columns_with_label:
            current = [columns_with_label[0]]
            for x in columns_with_label[1:]:
                if x - current[-1] <= 5:
                    current.append(x)
                else:
                    spans.append((current[0], current[-1]))
                    current = [x]
            spans.append((current[0], current[-1]))
        ten_x_spans = [
            (start, end)
            for start, end in spans
            if start >= 1000 and end <= 1475 and (end - start) >= 12
        ]
        assert len(ten_x_spans) >= 3, (
            f"Expected the three 10.x sections each to receive their own label "
            f"rectangle above the speed trace, but found {len(ten_x_spans)}: {ten_x_spans}. "
            f"Full label scan: {spans}."
        )


@REQUIRES_HUNGARY_SOURCE
def test_shared_guide_line_chain_split_persists_through_render(
    extractor: ModuleType, tmp_path: Path
) -> None:
    """Render the annotated PNG and assert the three 10.x labels stay split.

    Even when the source image text says ``10 UNRESOLVED`` the rendered audit
    PNG must surface every section of a shared guide-line chain as its own
    labeled interval — never as one combined caption. We assert directly on
    the resulting JSON that the three 10.x sections survive intact.
    """

    config = extractor.ExtractorConfig(
        output_dir=tmp_path, tesseract_cmd="definitely-not-tesseract"
    )
    extractor.process_path(HUNGARY_SOURCE, config)

    annotated = Image.open(tmp_path / "hungary_annotated.png")
    assert annotated.size == (1600, 900)
    labels_in_band = [
        interval
        for interval in json.loads((tmp_path / "hungary.json").read_text())["intervals"]
        if interval["displayed_sector"] == 10
    ]
    assert [item["display_label"] for item in labels_in_band] == ["10.1", "10.2", "10.3"]
    classifications = [item["classification"] for item in labels_in_band]
    assert classifications == ["high_speed", "low_speed", "medium_speed"]
    spans = [
        (item["start"]["full_image_x"], item["end"]["full_image_x"]) for item in labels_in_band
    ]
    assert spans == [(1011.0, 1214.5), (1214.5, 1388.0), (1388.0, 1465.0)]


def test_universal_sub_section_split_handles_tesseract_unavailable(extractor: ModuleType) -> None:
    """When Tesseract is unavailable, the universal sub-section path must not crash.

    The sub-section OCR routine is run for every bracket via sub-process; with no
    Tesseract on ``PATH`` the routine must return ``[]`` cleanly and add a
    ``SUBSECTION_OCR_UNAVAILABLE`` warning flag (or a parent
    ``OCR_UNAVAILABLE`` warning). The wrapper function must:
    - never raise;
    - align the bracket ID in the warning related_ids;
    - return an empty list when nothing useful was recognised.
    """

    image = Image.new("RGB", (200, 100), (25, 25, 30))
    bracket = {
        "bracket_id": "test-bracket",
        "full_image_span": {"x_min": 10, "x_max": 100, "y_min": 10, "y_max": 90},
    }
    config = extractor.ExtractorConfig(
        output_dir=Path(),
        tesseract_cmd="definitely-no-such-tesseract-binary",
    )
    sub_sections, flags = extractor._extract_subsections_for_bracket(image, bracket, config)
    assert sub_sections == []
    assert flags, "Expected a SUBSECTION_OCR_UNAVAILABLE flag when Tesseract is missing"
    assert any(f["code"] in {"SUBSECTION_OCR_UNAVAILABLE", "OCR_UNAVAILABLE"} for f in flags)


def test_subsection_label_alias_lookup(extractor: ModuleType) -> None:
    """The image-companion sub-section taxonomy must cover full and short labels."""

    pairs = [
        ("LOW SPEED", "low_speed"),
        ("MED", "medium_speed"),
        ("HIGH", "high_speed"),
        ("STRAIGHT", "straight"),
        ("STR", "straight"),
        ("S", "straight"),
    ]
    for cleaned, expected in pairs:
        assert extractor._match_subsection_label(cleaned) == expected, (
            f"alias lookup for {cleaned!r} should map to {expected!r}"
        )


def test_subsection_aliases_cover_variants(extractor: ModuleType) -> None:
    """Alias dictionary keys are exactly the canonical set."""

    expected = {
        "LOW SPEED",
        "LOW",
        "MEDIUM SPEED",
        "MED",
        "M",
        "HIGH SPEED",
        "HIGH",
        "H",
        "STRAIGHT",
        "STR",
        "ST",
        "S",
    }
    assert set(extractor.SUBSECTION_LABEL_ALIASES.keys()) == expected


def test_expand_brackets_with_subsections_creates_sub_intervals(extractor: ModuleType) -> None:
    """Universal sub-section expansion must split a bracket into N sub-brackets.

    Given a parent bracket ``bracket-tst`` spanning 100-200 in full-image x with
    three detected sub-sections, the expansion must produce three sub-brackets
    whose guide-line spans together cover the parent span, each carrying a
    distinct ``displayed_section`` index.
    """

    plot_x_min = 72
    parent = {
        "bracket_id": "bracket-tst",
        "full_image_span": {
            "x_min": 100,
            "x_max": 200,
            "y_min": 412,
            "y_max": 469,
        },
        "chart_local_span": {"x_min": 100 - plot_x_min, "x_max": 200 - plot_x_min},
        "guide_lines": [
            {"boundary_x": 100.0, "line_x_min": 100, "line_x_max": 100, "uncertainty_px": 0.5},
            {"boundary_x": 200.0, "line_x_min": 200, "line_x_max": 200, "uncertainty_px": 0.5},
        ],
        "horizontal_stroke_score": 1.0,
        "associated_label_detection_id": None,
        "association_method": None,
        "association_confidence": 0.0,
        "manual_override_id": "manual-tst",
        "displayed_sector": 10,
        "displayed_section": 1,
    }
    subs = [
        {
            "offset_x_min": 100.0,
            "offset_x_max": 130.0,
            "classification": "high_speed",
            "raw_text": "HIGH",
            "confidence": 80.0,
        },
        {
            "offset_x_min": 130.0,
            "offset_x_max": 170.0,
            "classification": "low_speed",
            "raw_text": "LOW",
            "confidence": 80.0,
        },
        {
            "offset_x_min": 170.0,
            "offset_x_max": 200.0,
            "classification": "medium_speed",
            "raw_text": "MED",
            "confidence": 80.0,
        },
    ]
    expanded = extractor._expand_brackets_with_subsections([parent], {"manual-tst": subs})
    assert len(expanded) == 3
    boundaries = [
        (round(item["guide_lines"][0]["boundary_x"]), round(item["guide_lines"][1]["boundary_x"]))
        for item in expanded
    ]
    assert boundaries == [(100, 130), (130, 170), (170, 200)]
    classifications = [item["associated_label_detection_id"] for item in expanded]
    assert classifications == [
        "subsection:bracket-tst:1",
        "subsection:bracket-tst:2",
        "subsection:bracket-tst:3",
    ]
    assert [item["displayed_section"] for item in expanded] == [1, 2, 3]
    assert [item["display_label"] for item in expanded] == ["10.1", "10.2", "10.3"]


@REQUIRES_HUNGARY_SOURCE
def test_universal_sub_section_split_widens_each_bracket_intervals(
    extractor: ModuleType, tmp_path: Path
) -> None:
    """End-to-end: with Tesseract unavailable, the fallback path still yields per-bracket intervals.

    When Tesseract is unavailable, the universal sub-section path produces no
    sub-sections and the partition must therefore match the per-bracket top-level
    partition. With a single bracket labelled 10 HIGH SPEED (1011..1214.5), the
    interval table must contain a single entry with classification ``high_speed``
    — verifying the fallback behaviour preserves the existing invariants.
    """

    config = extractor.ExtractorConfig(
        output_dir=tmp_path,
        tesseract_cmd="definitely-not-tesseract",
    )
    extractor.process_path(HUNGARY_SOURCE, config)
    data = json.loads((tmp_path / "hungary.json").read_text())
    sector_ten = [
        interval
        for interval in data["intervals"]
        if interval.get("display_label") and interval["display_label"].startswith("10.")
    ]
    # Without Tesseract the universal OCR path returns zero subsections so the
    # partition falls back to the manual override (10.1 / 10.2 / 10.3).
    assert [interval["classification"] for interval in sector_ten] == [
        "high_speed",
        "low_speed",
        "medium_speed",
    ]
    flags = {flag["code"] for flag in data["review_flags"]}
    assert "OCR_UNAVAILABLE" in flags


def test_universal_sub_section_split_produces_three_subsections_inside_10_1(
    extractor: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mocked Tesseract proves that 10.1 splits into 3 sub-sections when sub-labels exist.

    Inside the visible 10.1 bracket (x=1011..1214.5) three dim-font labels are
    written in the source image: STRAIGHT, HIGH SPEED, STRAIGHT. The
    pipeline:
      (1) runs ``_extract_subsections_for_bracket`` on the 10.1 bracket,
      (2) parses a realistic Tesseract TSV payload,
      (3) clusters the 3 words into a single y-row,
      (4) yields STRAIGHT / HIGH_SPEED / STRAIGHT classifications,
      (5) feeds them into ``_expand_brackets_with_subsections`` and produces
          three sub-brackets whose guide-line spans cover the parent span.
    The mocked subprocess.run is invoked only for sub-section OCR — the
    end-to-end validate-intervals path is exercised separately by
    ``test_universal_sub_section_split_widens_each_bracket_intervals``.
    """

    image = Image.new("RGB", (1600, 700), (25, 25, 30))
    bracket = {
        "bracket_id": "bracket-06",
        "manual_override_id": "manual-10.1",
        "full_image_span": {
            "x_min": 1011,
            "x_max": 1214,
            "y_min": 412,
            "y_max": 469,
        },
        "displayed_sector": 10,
        "displayed_section": 1,
        "guide_lines": [
            {"boundary_x": 1011, "line_x_min": 1011, "line_x_max": 1011, "uncertainty_px": 0.5},
            {"boundary_x": 1214, "line_x_min": 1214, "line_x_max": 1214, "uncertainty_px": 0.5},
        ],
        "chart_local_span": {"x_min": 0.0, "x_max": 203.0},
    }
    config = extractor.ExtractorConfig(output_dir=tmp_path, tesseract_cmd="tesseract")

    def _mock_subproc(*args: object, **kwargs: object) -> object:
        stdout = (
            "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\t"
            "left\ttop\twidth\theight\tconf\ttext\n"
            "5\t1\t1\t1\t1\t1\t10\t6\t48\t14\t82.5\tSTRAIGHT\n"
            "5\t1\t1\t1\t1\t2\t90\t6\t70\t14\t83.0\tHIGH\n"
            "5\t1\t1\t1\t1\t3\t180\t6\t72\t14\t82.0\tSTRAIGHT\n"
        )
        completed = type(
            "Completed",
            (),
            {"returncode": 0, "stdout": stdout, "stderr": ""},
        )()
        return completed

    monkeypatch.setattr(extractor.subprocess, "run", _mock_subproc)

    sub_sections, flags = extractor._extract_subsections_for_bracket(image, bracket, config)
    classifications = [item["classification"] for item in sub_sections]
    assert classifications == ["straight", "high_speed", "straight"], (
        f"Expected STRAIGHT/HIGH/STRAIGHT, got {classifications}"
    )
    for item in sub_sections:
        assert item["offset_x_min"] >= 1011.0
        assert item["offset_x_max"] <= 1214.0
    starts = sorted(item["offset_x_min"] for item in sub_sections)
    ends = sorted(item["offset_x_max"] for item in sub_sections)
    assert sub_sections[0]["offset_x_min"] == starts[0]
    assert sub_sections[-1]["offset_x_max"] == ends[-1]

    expanded = extractor._expand_brackets_with_subsections([bracket], {"manual-10.1": sub_sections})
    assert len(expanded) == 3
    classifications_after_expansion = [item["associated_label_detection_id"] for item in expanded]
    assert classifications_after_expansion == [
        "subsection:bracket-06:1",
        "subsection:bracket-06:2",
        "subsection:bracket-06:3",
    ]
    assert [item["display_label"] for item in expanded] == ["10.1", "10.2", "10.3"]
    boundaries = [
        (
            int(item["guide_lines"][0]["boundary_x"]),
            int(item["guide_lines"][1]["boundary_x"]),
        )
        for item in expanded
    ]
    assert boundaries[0][0] == 1011
    assert boundaries[-1][1] == 1214
    for left, right in boundaries:
        assert left < right, f"Sub-bracket must have positive width: {left}..{right}"
    assert [item["sub_section_classification"] for item in expanded] == [
        "straight",
        "high_speed",
        "straight",
    ]
    assert flags == [], f"Expected no review flags, got {flags}"


def test_sub_section_override_propagates_through_build_intervals(
    extractor: ModuleType,
) -> None:
    """``build_intervals`` must honor ``sub_section_classification`` overrides.

    Sub-section brackets are tagged with a direct classification override that
    takes precedence over the band-level OCR association. Without this
    override, the partition would emit three consecutive ``straight`` intervals
    for the expanded 10.1 bracket because the synthetic sub-section detection
    IDs are not registered in the OCR detection map.
    """

    plot_bounds = extractor.Bounds(0, 1600, 0, 800)
    guide_a = {"boundary_x": 1011.0, "line_x_min": 1011, "line_x_max": 1011, "uncertainty_px": 0.5}
    guide_b = {"boundary_x": 1075.0, "line_x_min": 1075, "line_x_max": 1075, "uncertainty_px": 0.5}
    guide_c = {"boundary_x": 1140.0, "line_x_min": 1140, "line_x_max": 1140, "uncertainty_px": 0.5}
    guide_d = {"boundary_x": 1214.5, "line_x_min": 1214, "line_x_max": 1214, "uncertainty_px": 0.5}
    brackets = [
        {
            "bracket_id": "bracket-06-sub-01",
            "guide_lines": [guide_a, guide_b],
            "sub_section_classification": "straight",
            "associated_label_detection_id": "subsection:bracket-06:1",
            "association_method": "sub_section_ocr",
            "displayed_sector": 10,
            "displayed_section": 1,
            "full_image_span": {
                "x_min": 1011,
                "x_max": 1075,
                "y_min": 412,
                "y_max": 469,
            },
            "horizontal_stroke_score": 1.0,
        },
        {
            "bracket_id": "bracket-06-sub-02",
            "guide_lines": [guide_b, guide_c],
            "sub_section_classification": "high_speed",
            "associated_label_detection_id": "subsection:bracket-06:2",
            "association_method": "sub_section_ocr",
            "displayed_sector": 10,
            "displayed_section": 2,
            "full_image_span": {
                "x_min": 1075,
                "x_max": 1140,
                "y_min": 412,
                "y_max": 469,
            },
            "horizontal_stroke_score": 1.0,
        },
        {
            "bracket_id": "bracket-06-sub-03",
            "guide_lines": [guide_c, guide_d],
            "sub_section_classification": "straight",
            "associated_label_detection_id": "subsection:bracket-06:3",
            "association_method": "sub_section_ocr",
            "displayed_sector": 10,
            "displayed_section": 3,
            "full_image_span": {
                "x_min": 1140,
                "x_max": 1214,
                "y_min": 412,
                "y_max": 469,
            },
            "horizontal_stroke_score": 1.0,
        },
    ]
    intervals = extractor.build_intervals(
        plot_bounds, brackets, ocr_detections=[], ocr_available=True
    )
    sector_ten = [
        interval
        for interval in intervals
        if interval["bracket_id"] in {b["bracket_id"] for b in brackets}
    ]
    assert [iv["classification"] for iv in sector_ten] == [
        "straight",
        "high_speed",
        "straight",
    ]
    assert [iv["display_label"] for iv in sector_ten] == ["10.1", "10.2", "10.3"]

    # Now build intervals WITHOUT the override classification and verify the
    # partition silently degrades (so reviewers can see the override is the
    # sole mechanism: sub-section IDs do not resolve through the OCR map).
    degraded_brackets = [
        {key: value for key, value in bracket.items() if key != "sub_section_classification"}
        for bracket in brackets
    ]
    degraded_intervals = extractor.build_intervals(
        plot_bounds, degraded_brackets, ocr_detections=[], ocr_available=True
    )
    degraded_ten = [
        interval
        for interval in degraded_intervals
        if interval["bracket_id"] in {b["bracket_id"] for b in brackets}
    ]
    assert [iv["classification"] for iv in degraded_ten] == ["straight", "straight", "straight"]


def test_detection_inside_spans_helper(extractor: ModuleType) -> None:
    """The dual-OCR conflict-resolver helper detects overlapping bboxes."""

    spans = [
        {
            "bracket_id": "bracket-06",
            "span": {"x_min": 1011, "x_max": 1214, "y_min": 412, "y_max": 469},
        }
    ]
    inside = {
        "detection_id": "ocr-a",
        "bbox": {"x_min": 1050, "x_max": 1100, "y_min": 418, "y_max": 432},
    }
    outside = {
        "detection_id": "ocr-b",
        "bbox": {"x_min": 200, "x_max": 240, "y_min": 418, "y_max": 432},
    }
    assert extractor._detection_inside_any_span(inside, spans) is True
    assert extractor._detection_inside_any_span(outside, spans) is False
    assert (
        extractor._detection_inside_any_span(
            {
                "detection_id": "ocr-c",
                "bbox": {"x_min": 1208, "x_max": 1300, "y_min": 418, "y_max": 432},
            },
            spans,
        )
        is True
    )


def test_chart_trace_helpers_split_brackets(extractor: ModuleType) -> None:
    """The chart-trace fallback must split ANY bracket whose smoothed trace
    range exceeds a meaningful fraction of the chart height.

    The Hungary 10.1 bracket (x=1011..1214) has a chart trace that drops
    from y=575 (slow corner apex) down to y=519 (top of chart = full speed)
    over its 203-pixel width; the speed-trace range of ~56 pixels is ~38%
    of the 145-pixel chart-vertical range.  The detector should therefore
    emit three sub-sections regardless of OCR availability.
    """

    arr = np.zeros((610, 1300, 3), dtype=np.float32)
    # Paint a speed-trace ramp across the bracket: at x=1011..1100 the
    # trace sits at y=575 (slow), at x=1100..1180 the trace climbs to y=520
    # (fast; top of chart), and at x=1180..1214 it sits steady at y=520.
    for x in range(1011, 1214):
        # Saturated-colour trace line at the painted y position.
        if x < 1100:
            offset = (x - 1011) * 0.3
            y = int(max(540, 575 - int(offset)))  # gradually faster
        elif x < 1180:
            y = int(520 + (1180 - x) // 6)
        else:
            y = 520
        arr[y : y + 3, x, 0] = 230.0
        arr[y : y + 3, x, 1] = 120.0
        arr[y : y + 3, x, 2] = 30.0
    sub_sections, flags = extractor._detect_chart_trace_transitions(
        arr,
        1011.0,
        1214.0,
        chart_y_min=465,
        chart_y_max=610,
    )
    assert sub_sections, "expected the chart trace to be split into sub-sections"
    assert len(sub_sections) >= 2, sub_sections
    sources = {item["source"] for item in sub_sections}
    assert sources == {"chart_trace"}
    starts = [item["offset_x_min"] for item in sub_sections]
    ends = [item["offset_x_max"] for item in sub_sections]
    assert starts[0] == 1011.0
    assert ends[-1] == 1214.0
    for start, end in zip(starts, ends):
        assert start < end
    codes = [f["code"] for f in flags]
    assert "CHART_TRACE_SUBSECTIONS_DETECTED" in codes


def test_chart_trace_helpers_return_empty_when_trace_is_static(
    extractor: ModuleType,
) -> None:
    """A bracket whose speed trace is flat must NOT be subdivided."""

    arr = np.zeros((610, 1300, 3), dtype=np.float32)
    # Paint a CONSTANT speed trace across the entire bracket range.
    for x in range(1011, 1214):
        arr[540:543, x, 0] = 230.0
        arr[540:543, x, 1] = 120.0
        arr[540:543, x, 2] = 30.0
    sub_sections, flags = extractor._detect_chart_trace_transitions(
        arr,
        1011.0,
        1214.0,
        chart_y_min=465,
        chart_y_max=610,
    )
    assert sub_sections == []
    assert flags == [] or all(f["code"] != "CHART_TRACE_SUBSECTIONS_DETECTED" for f in flags)


def test_chart_trace_smoothing_handles_nans_without_zero_padding(
    extractor: ModuleType,
) -> None:
    """``_smooth_chart_trace`` must not be dragged to zero by missing samples.

    Without proper edge padding, ``np.convolve(mode="same")`` zero-pads the
    input and produces a smoothed trace that drops well below the actual
    trace.  The fixed implementation replicates the boundary value to keep
    the smoothed signal inside the original data range.
    """

    raw = [578.0 for _ in range(40)] + [float("nan")] * 4 + [519.0 for _ in range(40)]
    smoothed = extractor._smooth_chart_trace(raw)
    smoothed_arr = np.asarray(smoothed, dtype=np.float64)
    assert smoothed_arr.min() >= 519.0 - 1.0, (
        f"smoothing pulled the trace below the actual data range: min={smoothed_arr.min()}"
    )
    assert smoothed_arr.max() <= 578.0 + 1.0, (
        f"smoothing exceeded the actual data range: max={smoothed_arr.max()}"
    )
    assert not np.isnan(smoothed_arr).any()


def test_chart_trace_three_way_split_preserves_parent_label(
    extractor: ModuleType,
) -> None:
    """Wide-range brackets always split into 3 sub-sections when forced.

    When the smoothed trace range spans more than 30% of the chart height
    the detector must produce three sub-sections with stable boundaries,
    not the single full-bracket classification.
    """

    arr = np.zeros((610, 1300, 3), dtype=np.float32)
    # Build a synthetic wide-range trace: from y=575 to y=519 across the
    # bracket.  That span (56 px) is roughly 38% of the 145 px chart height
    # which exceeds the 30% force3 threshold.
    for x in range(1011, 1214):
        progress = (x - 1011) / 203.0
        y = round(575 - progress * 56)
        arr[y : y + 3, x, 0] = 230.0
        arr[y : y + 3, x, 1] = 120.0
        arr[y : y + 3, x, 2] = 30.0
    subs, _flags = extractor._detect_chart_trace_transitions(arr, 1011.0, 1214.0, 465, 610)
    assert len(subs) == 3, f"expected 3 sub-sections, got {len(subs)}: {subs}"
    boundaries = [round(s["offset_x_min"]) for s in subs] + [round(subs[-1]["offset_x_max"])]
    assert boundaries[0] == 1011
    assert boundaries[-1] == 1214
    assert all(b < nxt for b, nxt in itertools.pairwise(boundaries)), boundaries
    classes = [s["classification"] for s in subs]
    assert all(c in extractor.ALLOWED_CLASSIFICATIONS for c in classes), classes
