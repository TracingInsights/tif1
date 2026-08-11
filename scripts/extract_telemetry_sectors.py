#!/usr/bin/env python3
"""Extract image-derived speed mini-sectors from Formula 1 analysis graphics.

The extractor deliberately lives outside :mod:`tif1`: it is an audit tool for
raster graphics, not a data-loading API.  Pillow and NumPy are used for image
work when available.  OCR is intentionally performed through the local
``tesseract`` executable so this repository does not acquire an OCR dependency.

The command processes only the paths supplied by the caller.  It never scans
``circuits/`` implicitly and never writes to a source image.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import tempfile
from collections.abc import Iterable, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any

try:
    import numpy as np
    from PIL import Image, ImageDraw, ImageEnhance, ImageFont
except ImportError as exc:  # pragma: no cover - exercised in dependency smoke tests
    np = None  # type: ignore[assignment]
    Image = None  # type: ignore[assignment]
    ImageDraw = None  # type: ignore[assignment]
    ImageEnhance = None  # type: ignore[assignment]
    ImageFont = None  # type: ignore[assignment]
    _IMAGE_IMPORT_ERROR = exc
else:
    _IMAGE_IMPORT_ERROR = None


SCHEMA_VERSION = "1.0"
EXTRACTOR_VERSION = "1.0.0"
OCR_DEFAULT_THRESHOLD = 90.0
OCR_SUBSECTION_CONFIDENCE = 60.0
OCR_SUBSECTION_LOW_CONFIDENCE = 75.0
# Horizontal and vertical tolerances (in full-image px) that the sub-section
# clustering uses to decide whether two OCR words belong to the same label.
# The x-gap emitter is calibrated so that consecutive words on a single label
# (e.g. "HIGH SPEED") stay together while distinct labels which sit at
# typographic positions tens of pixels apart get split into separate clusters.
SUBSECTION_X_GAP_PX = 18
SUBSECTION_Y_TOLERANCE_PX = 6
COLOR_DEFAULT_TOLERANCE = 2.0
ALLOWED_CLASSIFICATIONS = {
    "low_speed",
    "medium_speed",
    "high_speed",
    "straight",
    "unresolved",
}
DISPLAY_CLASSIFICATIONS = {
    "low_speed": "LOW SPEED",
    "medium_speed": "MEDIUM SPEED",
    "high_speed": "HIGH SPEED",
    "straight": "STRAIGHT",
    "unresolved": "UNRESOLVED",
}
CLASSIFICATION_COLORS = {
    "low_speed": (228, 74, 74, 90),
    "medium_speed": (239, 174, 65, 90),
    "high_speed": (65, 174, 222, 90),
    "straight": (144, 144, 164, 65),
    "unresolved": (220, 76, 190, 100),
}
# Short-form labels used when a bracket-band interval is too narrow to host a
# non-overlapping full caption. The shorthand preserves the 4-class taxonomy
# so adjacent sections (e.g. 10.1 / 10.2 / 10.3) never collapse into one
# combined "10" prefix.
COMPACT_LABEL_LEGEND = {
    "LOW SPEED": "LOW",
    "MEDIUM SPEED": "MED",
    "HIGH SPEED": "HIGH",
    "STRAIGHT": "STR",
    "UNRESOLVED": "UNR",
}
KNOWN_TITLES = {
    "hungary": "BUDAPEST - THIRD PRACTICE - LAP TIME ANALYSIS",
}
LABEL_TO_CLASSIFICATION = {
    "LOW SPEED": "low_speed",
    "MEDIUM SPEED": "medium_speed",
    "HIGH SPEED": "high_speed",
}
# Native visual review of the source image is authoritative for Hungary's
# semantic labels. OCR remains evidence only; it must not override this reviewed
# map. The displayed turn number is separate from the contiguous partition
# sequence because displayed sector 10 contains three adjacent sections.
MANUAL_LABEL_OVERRIDE_SOURCE_SHA256 = {
    "hungary": "df4a27682b5712f686c47af4cda8048d9971104e7aecf6e329967c47e1ce3ac6",
}
MANUAL_LABEL_OVERRIDES = {
    "hungary": (
        {
            "start": 237.0,
            "end": 321.0,
            "classification": "low_speed",
            "displayed_sector": 2,
            "displayed_section": 1,
        },
        {
            "start": 403.5,
            "end": 489.5,
            "classification": "medium_speed",
            "displayed_sector": 4,
            "displayed_section": 1,
        },
        {
            "start": 654.0,
            "end": 796.0,
            "classification": "medium_speed",
            "displayed_sector": 6,
            "displayed_section": 1,
        },
        {
            "start": 836.0,
            "end": 910.0,
            "classification": "low_speed",
            "displayed_sector": 8,
            "displayed_section": 1,
        },
        {
            "start": 910.0,
            "end": 1011.0,
            "classification": "medium_speed",
            "displayed_sector": 9,
            "displayed_section": 1,
        },
        {
            "start": 1011.0,
            "end": 1214.5,
            "classification": "high_speed",
            "displayed_sector": 10,
            "displayed_section": 1,
        },
        {
            "start": 1214.5,
            "end": 1388.0,
            "classification": "low_speed",
            "displayed_sector": 10,
            "displayed_section": 2,
        },
        {
            "start": 1388.0,
            "end": 1465.0,
            "classification": "medium_speed",
            "displayed_sector": 10,
            "displayed_section": 3,
        },
    ),
}


def _manual_label_overrides(source_key: str) -> tuple[dict[str, Any], ...]:
    """Return reviewed visual labels for a known source graphic."""
    return MANUAL_LABEL_OVERRIDES.get(source_key, ())


@dataclass(frozen=True)
class ExtractorConfig:
    """Configuration for one or more image extractions."""

    output_dir: Path
    ocr_threshold: float = OCR_DEFAULT_THRESHOLD
    tesseract_cmd: str = "tesseract"
    color_tolerance_px: float = COLOR_DEFAULT_TOLERANCE
    annotate: bool = True
    json_only: bool = False
    markdown_only: bool = False
    fallback_bounds: tuple[int, int, int, int] | None = None


@dataclass(frozen=True)
class Bounds:
    """Inclusive full-image rectangle."""

    x_min: int
    x_max: int
    y_min: int
    y_max: int

    @property
    def width(self) -> int:
        return self.x_max - self.x_min

    @property
    def height(self) -> int:
        return self.y_max - self.y_min

    def as_dict(self) -> dict[str, int]:
        return {
            "x_min": self.x_min,
            "x_max": self.x_max,
            "y_min": self.y_min,
            "y_max": self.y_max,
        }


class ExtractionError(RuntimeError):
    """Raised for errors that prevent an image from being read or analyzed."""


def _require_image_dependencies() -> None:
    if _IMAGE_IMPORT_ERROR is not None:
        raise ExtractionError(
            "Image extraction requires Pillow and NumPy. "
            "Install them in the analysis environment without changing tif1's core dependencies."
        ) from _IMAGE_IMPORT_ERROR


def _clamp_bounds(bounds: Bounds, width: int, height: int) -> Bounds:
    return Bounds(
        max(0, min(bounds.x_min, width - 1)),
        max(0, min(bounds.x_max, width - 1)),
        max(0, min(bounds.y_min, height - 1)),
        max(0, min(bounds.y_max, height - 1)),
    )


def parse_fallback_bounds(value: str) -> tuple[int, int, int, int]:
    """Parse ``x_min,x_max,y_min,y_max`` command-line bounds."""
    try:
        values = tuple(int(part.strip()) for part in value.split(","))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("fallback bounds must be four integers") from exc
    if len(values) != 4 or values[0] >= values[1] or values[2] >= values[3]:
        raise argparse.ArgumentTypeError(
            "fallback bounds must be x_min,x_max,y_min,y_max with positive width and height"
        )
    return values  # type: ignore[return-value]


def source_metadata(path: Path) -> tuple[dict[str, Any], Any]:
    """Read immutable source metadata and return it with the loaded image."""
    _require_image_dependencies()
    if not path.is_file():
        raise ExtractionError(f"Source image does not exist: {path}")
    try:
        source_image = Image.open(path)
        image_format = source_image.format or path.suffix.lstrip(".").upper()
        image = source_image.convert("RGB")
        image.load()
    except Exception as exc:
        raise ExtractionError(f"Could not read raster image {path}: {exc}") from exc

    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    source_key = path.stem.lower().replace(" ", "_")
    return (
        {
            "path": path.as_posix(),
            "source_key": source_key,
            "sha256": digest,
            "format": image_format,
            "width_px": image.width,
            "height_px": image.height,
            "color_mode": image.mode,
        },
        image,
    )


def _longest_true_run(mask: Any) -> tuple[int, int] | None:
    """Return the longest inclusive run in a one-dimensional boolean array."""
    indexes = np.flatnonzero(mask)
    if len(indexes) == 0:
        return None
    breaks = np.flatnonzero(np.diff(indexes) > 1)
    starts = np.r_[0, breaks + 1]
    ends = np.r_[breaks, len(indexes) - 1]
    winner = int(np.argmax(ends - starts))
    return int(indexes[starts[winner]]), int(indexes[ends[winner]])


def _candidate_region(gray: Any) -> Bounds | None:
    """Find the broad chart component using brightness structure.

    The chart family has a dark page background and a lighter rectangular plot.
    Several thresholds are tried so JPEG compression and theme changes do not
    depend on a single RGB value.  This is deliberately a visual candidate,
    not an authoritative hard-coded rectangle.
    """
    height, width = gray.shape
    y0, y1 = int(height * 0.46), int(height * 0.83)
    x0, x1 = int(width * 0.02), int(width * 0.99)
    # At this graphic family’s scale, the plot background is the largest
    # coherent region above a moderate brightness threshold.  Relative row and
    # column coverage keeps the detector independent of exact JPEG RGB values.
    mask = gray[y0:y1, x0:x1] > 55.0
    row_run = _longest_true_run(mask.mean(axis=1) > 0.02)
    if row_run is None or row_run[1] - row_run[0] < int(height * 0.12):
        return None
    cy0, cy1 = y0 + row_run[0], y0 + row_run[1]
    columns = mask[row_run[0] : row_run[1] + 1].mean(axis=0) > 0.02
    col_run = _longest_true_run(columns)
    if col_run is None or col_run[1] - col_run[0] < int(width * 0.45):
        return None
    cx0, cx1 = x0 + col_run[0], x0 + col_run[1]
    return Bounds(cx0, cx1, cy0, cy1)


def detect_plot_bounds(
    image: Any,
    fallback: tuple[int, int, int, int] | None = None,
) -> tuple[Bounds, dict[str, Any], list[dict[str, Any]]]:
    """Detect the speed plot and return bounds, evidence, and review flags."""
    _require_image_dependencies()
    array = np.asarray(image.convert("RGB"), dtype=np.float32)
    gray = array.mean(axis=2)
    dynamic = _candidate_region(gray)
    flags: list[dict[str, Any]] = []
    fallback_values = fallback or (72, 1527, 465, 708)
    fallback_bounds = _clamp_bounds(Bounds(*fallback_values), image.width, image.height)

    if dynamic is None or dynamic.width < image.width * 0.45 or dynamic.height < image.height * 0.1:
        bounds = fallback_bounds
        method = "template_fallback"
        confidence = 0.35
        flags.append(
            _review_flag(
                "FALLBACK_PLOT_BOUNDS_USED",
                "warning",
                "Dynamic plot detection did not produce usable bounds; the configured fallback was used.",
                [],
                blocks=False,
            )
        )
    else:
        bounds = _clamp_bounds(dynamic, image.width, image.height)
        method = "dynamic_visual_structure"
        # Confidence reflects only geometric plausibility, not semantic correctness.
        confidence = min(0.96, max(0.45, 0.45 + bounds.width / image.width * 0.35))
        delta = max(
            abs(bounds.x_min - fallback_bounds.x_min),
            abs(bounds.x_max - fallback_bounds.x_max),
            abs(bounds.y_min - fallback_bounds.y_min),
            abs(bounds.y_max - fallback_bounds.y_max),
        )
        if delta > 20:
            flags.append(
                _review_flag(
                    "DYNAMIC_FALLBACK_DISAGREEMENT",
                    "warning",
                    f"Dynamic bounds differ from the reference fallback by up to {delta} pixels.",
                    [],
                    blocks=False,
                    alternative_evidence={"fallback_bounds": fallback_bounds.as_dict()},
                )
            )
    evidence = {
        "full_image": bounds.as_dict(),
        "chart_local": {"x_min": 0, "x_max": bounds.width, "y_min": 0, "y_max": bounds.height},
        "detection_method": method,
        "fallback_used": method == "template_fallback",
        "confidence": round(confidence, 4),
        "reference_fallback": fallback_bounds.as_dict(),
    }
    return bounds, evidence, flags


def detect_bracket_band(plot_bounds: Bounds, image_size: tuple[int, int]) -> Bounds:
    """Return the label/bracket search band immediately above the plot."""
    _, height = image_size
    band_height = max(34, min(90, int(plot_bounds.height * 0.22)))
    return Bounds(
        plot_bounds.x_min,
        plot_bounds.x_max,
        max(0, plot_bounds.y_min - band_height),
        min(height - 1, plot_bounds.y_min + 4),
    )


def _vertical_line_candidates(gray: Any, band: Bounds) -> list[tuple[int, int, int]]:
    """Find bright vertical guide-line spans as ``(x_min, x_max, count)``."""
    crop = gray[band.y_min : band.y_max + 1, band.x_min : band.x_max + 1]
    # Guide lines are short but vertically continuous in the middle of the
    # band.  Longest-run evidence rejects most letter strokes and tick marks.
    bright = crop > 55.0
    max_runs = np.zeros(crop.shape[1], dtype=int)
    current_runs = np.zeros(crop.shape[1], dtype=int)
    for row in bright:
        current_runs = np.where(row, current_runs + 1, 0)
        max_runs = np.maximum(max_runs, current_runs)
    columns = max_runs >= 12
    runs: list[tuple[int, int]] = []
    # Collect all runs, rather than only the longest one.
    indexes = np.flatnonzero(columns)
    if len(indexes):
        breaks = np.flatnonzero(np.diff(indexes) > 2)
        starts = np.r_[0, breaks + 1]
        ends = np.r_[breaks, len(indexes) - 1]
        runs = [(int(indexes[s]), int(indexes[e])) for s, e in zip(starts, ends, strict=True)]
    candidates = []
    for local_min, local_max in runs:
        if local_max - local_min > 12:
            continue
        x_min, x_max = band.x_min + local_min, band.x_min + local_max
        candidates.append((x_min, x_max, int(max_runs[local_min : local_max + 1].max())))
    return candidates


def _pair_score(
    gray: Any, left: tuple[int, int, int], right: tuple[int, int, int], band: Bounds
) -> float:
    """Score a possible bracket pair using its horizontal stroke evidence."""
    x_left = (left[0] + left[1]) // 2
    x_right = (right[0] + right[1]) // 2
    if x_right <= x_left + 8:
        return -1.0
    crop = gray[band.y_min : band.y_max + 1, x_left : x_right + 1]
    if crop.size == 0:
        return -1.0
    bright = crop > np.maximum(45.0, np.percentile(crop, 75, axis=1, keepdims=True) + 12.0)
    horizontal = bright.mean(axis=1)
    return float(horizontal.max())


def detect_brackets(
    image: Any, plot_bounds: Bounds, band: Bounds
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    """Detect visually distinct brackets and authoritative guide-line spans."""
    array = np.asarray(image.convert("RGB"), dtype=np.float32)
    gray = array.mean(axis=2)
    candidates = _vertical_line_candidates(gray, band)
    candidates.sort()
    flags: list[dict[str, Any]] = []

    # A guide line can be split by antialiasing. Merge only immediately adjacent
    # candidates; separated candidates remain separate evidence.
    merged: list[tuple[int, int, int]] = []
    for candidate in candidates:
        if merged and candidate[0] - merged[-1][1] <= 2:
            old = merged.pop()
            merged.append((old[0], candidate[1], max(old[2], candidate[2])))
        else:
            merged.append(candidate)

    pairs: list[tuple[tuple[int, int, int], tuple[int, int, int], float]] = []
    if len(merged) % 2:
        flags.append(
            _review_flag(
                "ODD_GUIDE_LINE_CANDIDATES",
                "warning",
                "An unmatched guide-line candidate was retained as raw evidence and excluded from pairing.",
                [],
                blocks=False,
            )
        )
    for index in range(0, len(merged) - 1, 2):
        left, right = merged[index], merged[index + 1]
        score = _pair_score(gray, left, right, band)
        if right[0] <= left[1] + 8:
            flags.append(
                _review_flag(
                    "INVALID_BRACKET_PAIR",
                    "warning",
                    f"Guide-line candidates at {left[0]} and {right[0]} are too close to form a bracket.",
                    [],
                    blocks=False,
                )
            )
            continue
        pairs.append((left, right, score))
    if not pairs:
        flags.append(
            _review_flag(
                "BRACKET_GEOMETRY_LOW_CONFIDENCE",
                "warning",
                "Guide lines were found, but no valid adjacent bracket pairs were formed.",
                [],
                blocks=False,
            )
        )

    pairs.sort(key=lambda pair: pair[0][0])
    brackets: list[dict[str, Any]] = []
    for index, (left, right, score) in enumerate(pairs, start=1):
        left_x_min, left_x_max, _ = left
        right_x_min, right_x_max, _ = right
        left_center = (left_x_min + left_x_max) / 2.0
        right_center = (right_x_min + right_x_max) / 2.0
        bracket_id = f"bracket-{index:02d}"
        brackets.append(
            {
                "bracket_id": bracket_id,
                "full_image_span": {
                    "x_min": left_x_min,
                    "x_max": right_x_max,
                    "y_min": band.y_min,
                    "y_max": band.y_max,
                },
                "chart_local_span": {
                    "x_min": left_x_min - plot_bounds.x_min,
                    "x_max": right_x_max - plot_bounds.x_min,
                },
                "guide_lines": [
                    _guide_line("left", left_x_min, left_x_max, left_center, band),
                    _guide_line("right", right_x_min, right_x_max, right_center, band),
                ],
                "horizontal_stroke_score": round(score, 4),
                "associated_label_detection_id": None,
                "association_method": None,
                "association_confidence": 0.0,
            }
        )

    if not brackets:
        flags.append(
            _review_flag(
                "BRACKET_GEOMETRY_UNUSABLE",
                "error",
                "No bracket pair could be measured in the detected label band.",
                [],
                blocks=True,
            )
        )
    evidence = {
        "band_full_image": band.as_dict(),
        "guide_line_candidates": [
            {"line_x_min": x0, "line_x_max": x1, "supporting_pixels": count}
            for x0, x1, count in merged
        ],
        "brackets": brackets,
    }
    return brackets, evidence, flags


def apply_manual_label_overrides(
    source_key: str,
    brackets: list[dict[str, Any]],
    bracket_evidence: dict[str, Any],
    plot_bounds: Bounds,
    band: Bounds,
    *,
    source_sha256: str | None = None,
    allow_override: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    """Apply reviewed labels and split shared guide-line chains.

    A source-specific override is deliberately represented as evidence. It is
    used only when a graphic has been visually reviewed and OCR cannot provide
    the semantic label reliably. The geometric guide lines still come from the
    image detector; the override only supplies labels and missing adjacent
    bracket pairs.
    """
    expected_sha256 = MANUAL_LABEL_OVERRIDE_SOURCE_SHA256.get(source_key)
    hash_passed = bool(expected_sha256 and source_sha256 == expected_sha256)
    if not hash_passed:
        mismatch_flags = []
        if expected_sha256 and source_sha256 != expected_sha256:
            mismatch_flags.append(
                _review_flag(
                    "MANUAL_OVERRIDE_SOURCE_HASH_MISMATCH",
                    "error",
                    "The source-specific visual label override was not applied because the source hash differs from the reviewed Hungary image.",
                    [],
                    blocks=True,
                    alternative_evidence={
                        "expected_sha256": expected_sha256,
                        "actual_sha256": source_sha256,
                    },
                )
            )
        return (
            brackets,
            [],
            {
                "applied": False,
                "overrides": [],
                "source_hash_guard": {
                    "expected": expected_sha256,
                    "actual": source_sha256,
                    "passed": hash_passed,
                },
            },
            mismatch_flags,
        )
    if not allow_override:
        return (
            brackets,
            [],
            {
                "applied": False,
                "overrides": [],
                "source_hash_guard": {
                    "expected": expected_sha256,
                    "actual": source_sha256,
                    "passed": True,
                    "disabled_by_ocr": True,
                },
            },
            [],
        )
    overrides = _manual_label_overrides(source_key)
    if not overrides:
        return brackets, [], {"applied": False, "overrides": []}, []

    candidates = bracket_evidence.get("guide_line_candidates", [])
    flags: list[dict[str, Any]] = []
    manual_detections: list[dict[str, Any]] = []
    reviewed_brackets: list[dict[str, Any]] = []
    evidence_overrides: list[dict[str, Any]] = []

    def nearest_candidate(x: float) -> dict[str, Any] | None:
        if not candidates:
            return None
        candidate = min(
            candidates,
            key=lambda item: abs((item["line_x_min"] + item["line_x_max"]) / 2.0 - x),
        )
        center = (candidate["line_x_min"] + candidate["line_x_max"]) / 2.0
        return candidate if abs(center - x) <= 3.0 else None

    for index, override in enumerate(overrides, start=1):
        start, end = float(override["start"]), float(override["end"])
        left = nearest_candidate(start)
        right = nearest_candidate(end)
        detection_id = f"manual-{index:02d}"
        if left is None or right is None:
            flags.append(
                _review_flag(
                    "MANUAL_OVERRIDE_BOUNDARY_NOT_FOUND",
                    "error",
                    f"Reviewed Hungary label {detection_id} could not be matched to detected guide lines.",
                    [detection_id],
                    blocks=True,
                    selected_interpretation=override["classification"],
                    alternative_evidence=override,
                )
            )
            continue

        left_center = (left["line_x_min"] + left["line_x_max"]) / 2.0
        right_center = (right["line_x_min"] + right["line_x_max"]) / 2.0
        bracket_id = f"bracket-{index:02d}"
        reviewed_brackets.append(
            {
                "bracket_id": bracket_id,
                "full_image_span": {
                    "x_min": left["line_x_min"],
                    "x_max": right["line_x_max"],
                    "y_min": band.y_min,
                    "y_max": band.y_max,
                },
                "chart_local_span": {
                    "x_min": left_center - plot_bounds.x_min,
                    "x_max": right_center - plot_bounds.x_min,
                },
                "guide_lines": [
                    _guide_line(
                        "left",
                        left["line_x_min"],
                        left["line_x_max"],
                        left_center,
                        band,
                    ),
                    _guide_line(
                        "right",
                        right["line_x_min"],
                        right["line_x_max"],
                        right_center,
                        band,
                    ),
                ],
                "horizontal_stroke_score": 1.0,
                "associated_label_detection_id": None,
                "association_method": None,
                "association_confidence": 0.0,
                "manual_override_id": detection_id,
                "displayed_sector": override.get("displayed_sector"),
                "displayed_section": override.get("displayed_section"),
            }
        )
        label = DISPLAY_CLASSIFICATIONS[override["classification"]]
        manual_detections.append(
            {
                "detection_id": detection_id,
                "text_raw": label,
                "text_normalized": label,
                "classification_candidate": override["classification"],
                "confidence": 100.0,
                "bbox_full_image": {
                    "x_min": left["line_x_min"],
                    "x_max": right["line_x_max"],
                    "y_min": band.y_min,
                    "y_max": band.y_max,
                },
                "engine": "manual_visual_review",
                "threshold": None,
                "accepted": True,
                "warnings": [],
                "source_note": "Native visual review of the source image; retained independently of OCR.",
                "displayed_sector": override.get("displayed_sector"),
                "displayed_section": override.get("displayed_section"),
            }
        )
        evidence_overrides.append(
            {
                "override_id": detection_id,
                "classification": override["classification"],
                "classification_display": label,
                "guide_span": {"x_min": left_center, "x_max": right_center},
                "source": "native_image_visual_review",
                "displayed_sector": override.get("displayed_sector"),
                "displayed_section": override.get("displayed_section"),
            }
        )

    if reviewed_brackets:
        # The reviewed chain is authoritative for this source and preserves
        # shared endpoints as separate adjacent brackets.
        reviewed_brackets.sort(key=lambda item: item["guide_lines"][0]["boundary_x"])
        brackets = reviewed_brackets
    else:
        flags.append(
            _review_flag(
                "MANUAL_OVERRIDE_UNUSABLE",
                "error",
                "No reviewed label override could be mapped to the detected guide lines.",
                [],
                blocks=True,
            )
        )
    return (
        brackets,
        manual_detections,
        {
            "applied": bool(reviewed_brackets),
            "source": "native_image_visual_review",
            "overrides": evidence_overrides,
        },
        flags,
    )


def _guide_line(side: str, x_min: int, x_max: int, center: float, band: Bounds) -> dict[str, Any]:
    return {
        "side": side,
        "line_x_min": x_min,
        "line_x_max": x_max,
        "boundary_x": center,
        "uncertainty_px": (x_max - x_min + 1) / 2.0,
        "visible_y_min": band.y_min,
        "visible_y_max": band.y_max,
    }


def _normalize_ocr_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("|", "I").strip().upper())


def _candidate_from_ocr_text(text: str) -> str | None:
    normalized = _normalize_ocr_text(text)
    for label, classification in LABEL_TO_CLASSIFICATION.items():
        if label in normalized or normalized.replace(" ", "") == label.replace(" ", ""):
            return classification
    return None


def _tesseract_version(command: str) -> str | None:
    try:
        completed = subprocess.run(
            [command, "--version"], capture_output=True, text=True, check=False, timeout=10
        )
    except (OSError, subprocess.SubprocessError):
        return None
    first_line = completed.stdout.splitlines()[:1]
    return first_line[0].strip() if first_line else None


def run_ocr(
    image: Any, band: Bounds, config: ExtractorConfig
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    """Run Tesseract TSV OCR and retain every meaningful text detection."""
    flags: list[dict[str, Any]] = []
    version = _tesseract_version(config.tesseract_cmd)
    ocr_evidence: dict[str, Any] = {
        "engine": "tesseract",
        "command": config.tesseract_cmd,
        "version": version,
        "threshold": config.ocr_threshold,
        "available": version is not None,
        "detections": [],
    }
    if version is None:
        flags.append(
            _review_flag(
                "OCR_UNAVAILABLE",
                "error",
                f"Could not execute local Tesseract command: {config.tesseract_cmd}.",
                [],
                blocks=True,
            )
        )
        ocr_evidence["error"] = "tesseract executable unavailable"
        return [], ocr_evidence, flags

    crop = image.crop((band.x_min, band.y_min, band.x_max + 1, band.y_max + 1))
    with tempfile.TemporaryDirectory(prefix="tif1-telemetry-ocr-") as temporary:
        crop_path = Path(temporary) / "label-band.png"
        crop.save(crop_path, format="PNG")
        try:
            completed = subprocess.run(
                [config.tesseract_cmd, str(crop_path), "stdout", "--psm", "11", "tsv"],
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            flags.append(
                _review_flag(
                    "OCR_UNAVAILABLE",
                    "error",
                    f"Tesseract execution failed: {exc}",
                    [],
                    blocks=True,
                )
            )
            ocr_evidence["error"] = str(exc)
            return [], ocr_evidence, flags

    if completed.returncode != 0:
        message = completed.stderr.strip() or "Tesseract returned a non-zero exit code."
        flags.append(_review_flag("OCR_UNAVAILABLE", "error", message, [], blocks=True))
        ocr_evidence["error"] = message
        return [], ocr_evidence, flags

    detections: list[dict[str, Any]] = []
    lines = completed.stdout.splitlines()
    if lines:
        header = lines[0].split("\t")
        positions = {name: index for index, name in enumerate(header)}
        words_by_line: dict[tuple[int, int, int], list[dict[str, Any]]] = {}
        for raw_line in lines[1:]:
            fields = raw_line.split("\t")
            if len(fields) < len(header):
                continue
            text = fields[positions.get("text", -1)].strip()
            if not text:
                continue
            try:
                confidence = float(fields[positions["conf"]])
                left = int(fields[positions["left"]])
                top = int(fields[positions["top"]])
                width = int(fields[positions["width"]])
                height = int(fields[positions["height"]])
                block = int(fields[positions["block_num"]])
                paragraph = int(fields[positions["par_num"]])
                line = int(fields[positions["line_num"]])
            except (KeyError, ValueError):
                continue
            words_by_line.setdefault((block, paragraph, line), []).append(
                {
                    "text": text,
                    "confidence": confidence,
                    "left": left,
                    "top": top,
                    "right": left + width,
                    "bottom": top + height,
                }
            )

        for index, words in enumerate(words_by_line.values(), start=1):
            words.sort(key=lambda word: word["left"])
            text = " ".join(word["text"] for word in words)
            normalized = _normalize_ocr_text(text)
            candidate = _candidate_from_ocr_text(normalized)
            if candidate is None:
                flags.append(
                    _review_flag(
                        "OCR_UNRECOGNIZED_TEXT",
                        "warning",
                        f"OCR text was preserved but did not match a known speed class: {normalized!r}.",
                        [],
                        blocks=False,
                    )
                )
            confidence = min(word["confidence"] for word in words)
            bbox = {
                "x_min": band.x_min + min(word["left"] for word in words),
                "x_max": band.x_min + max(word["right"] for word in words),
                "y_min": band.y_min + min(word["top"] for word in words),
                "y_max": band.y_min + max(word["bottom"] for word in words),
            }
            detection_id = f"ocr-{index:02d}"
            warnings: list[str] = []
            accepted = confidence >= config.ocr_threshold
            if not accepted:
                warnings.append("OCR confidence is below the configured threshold.")
                flags.append(
                    _review_flag(
                        "OCR_BELOW_THRESHOLD",
                        "warning",
                        f"{normalized} was recognized with confidence {confidence:.1f}, below "
                        f"the configured threshold of {config.ocr_threshold:g}.",
                        [detection_id],
                        selected_interpretation=candidate,
                        blocks=False,
                    )
                )
            detection = {
                "detection_id": detection_id,
                "text_raw": text,
                "text_normalized": normalized,
                "classification_candidate": candidate,
                "confidence": round(confidence, 2),
                "bbox_full_image": bbox,
                "engine": "tesseract",
                "threshold": config.ocr_threshold,
                "accepted": accepted,
                "warnings": warnings,
            }
            detections.append(detection)
    ocr_evidence["detections"] = detections
    return detections, ocr_evidence, flags


def _detection_inside_any_span(detection: dict[str, Any], spans: list[dict[str, Any]]) -> bool:
    """True when a Tesseract detection's bbox overlaps any of ``spans``.

    Spans are tuples of ``{span, bracket_id}`` where ``span`` is a full-image
    x/y range. Used to drop band-level OCR detections that would conflict
    with sub-section OCR results.
    """
    bbox = detection.get("bbox")
    if not bbox:
        return False
    bx_min = float(bbox.get("x_min", 0))
    bx_max = float(bbox.get("x_max", 0))
    by_min = float(bbox.get("y_min", 0))
    by_max = float(bbox.get("y_max", 0))
    for entry in spans:
        span = entry["span"]
        sx_min = float(span["x_min"])
        sx_max = float(span["x_max"])
        sy_min = float(span["y_min"])
        sy_max = float(span["y_max"])
        not_separated = (
            bx_min <= sx_max and bx_max >= sx_min and by_min <= sy_max and by_max >= sy_min
        )
        if not_separated:
            return True
    return False


# Accepted compact taxonomy for OCR'd sub-section labels. The short forms map
# to the same canonical classification as the full labels and are kept
# separate so OCR variants like ``STR`` / ``MED`` / ``HIGH`` can still be
# recognised. ``S`` alone is treated as the most degenerate shorthand for
# ``STRAIGHT`` and is accepted only when confidence is high.
SUBSECTION_LABEL_ALIASES: dict[str, str] = {
    "LOW SPEED": "low_speed",
    "LOW": "low_speed",
    "MEDIUM SPEED": "medium_speed",
    "MED": "medium_speed",
    "M": "medium_speed",
    "HIGH SPEED": "high_speed",
    "HIGH": "high_speed",
    "H": "high_speed",
    "STRAIGHT": "straight",
    "STR": "straight",
    "ST": "straight",
    "S": "straight",
}


def _enhance_for_sub_section_ocr(crop_image: Any) -> Any:
    """Boost contrast on a small bracket crop so dim sub-section labels shine.

    The source graphic's brackets sit below the speed chart with sub-classification
    labels in a dim font colour. The band is too small for the document-level OCR to
    capture anything readable. The standalone extractor therefore crops each bracket
    region, multiplies brightness and contrast, and unsharpens the result before
    sending it back through Tesseract.
    """
    brightened = ImageEnhance.Brightness(crop_image).enhance(3.0)
    contrasted = ImageEnhance.Contrast(brightened).enhance(5.0)
    sharpened = ImageEnhance.Sharpness(contrasted).enhance(1.6)
    return sharpened


def _strip_ocr_noise(text: str) -> str:
    """Normalize a single OCR'd word for sub-section classification lookup."""
    cleaned = re.sub(r"[^A-Z\s]", "", text.upper())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _match_subsection_label(cleaned: str) -> str | None:
    """Match an OCR-cleaned word against the sub-section taxonomy.

    Exact-compare against any alias length (so ``S``, ``H``, ``M`` still
    resolve when Tesseract returns a single short token).

    For multi-character aliases, compare against each whitespace-delimited
    token of the cleaned text. This avoids anchoring prefixes like ``STR``
    matching the unrelated word ``STRATEGY`` while still letting ``MED``
    match a Tesseract word whose character set was inferred as
    ``MEDIUM``.
    """
    if not cleaned:
        return None
    for alias, classification in SUBSECTION_LABEL_ALIASES.items():
        if cleaned == alias:
            return classification
    tokens = cleaned.split()
    for alias, classification in SUBSECTION_LABEL_ALIASES.items():
        if len(alias) < 3:
            continue
        if any(token == alias for token in tokens):
            return classification
    return None


# Chart-trace based sub-section thresholds.  The Hungary source shows the speed
# trace line dropping through ~14-30 px in the chart background (relative to a
# full chart height of ~243 px) when a labelled sub-section changes regime.
CHART_TRACE_MIN_SWING_PX = 14.0
# Fraction of full image Y range (chart Y span) the trace must swing through
# for a sub-section to be detected.  Calibrated against Hungary where the
# trace rarely swings more than 12% of chart height on a single class corner.
CHART_TRACE_MIN_SWING_FRAC = 0.05
CHART_TRACE_SMOOTH_WINDOW_FRAC = 0.04
CHART_TRACE_MIN_SUB_LENGTH_FRAC = 0.12
# Bands used to classify the trace's local Y position into one of the four
# speed classes.  Values are expressed as fractions from the *top* of the
# chart Y range (top = fastest).  Hungary traces stay in the top half of the
# chart with NO low-speed sub-segments crossing into the bottom half, so the
# thresholds bias toward the high-speed end of the spectrum.
CHART_TRACE_BANDS: tuple[tuple[float, str], ...] = (
    (0.30, "straight"),
    (0.55, "high_speed"),
    (0.78, "medium_speed"),
    (1.01, "low_speed"),
)


def _sample_chart_trace_y(
    image_array: Any,
    bracket_left: float,
    bracket_right: float,
    chart_y_min: int,
    chart_y_max: int,
) -> list[tuple[int, float]]:
    """Return ``(column, median_speed_trace_y)`` for columns in the bracket.

    Speed-trace pixels are identified by saturation >50 with max RGB >130.
    The chart Y-band (between ``chart_y_min`` and ``chart_y_max``) restricts
    the search so y-axis tick labels and other annotations do not contaminate
    the per-column Y estimate.
    """
    columns: list[tuple[int, float]] = []
    if bracket_right <= bracket_left:
        return columns
    chart_height = max(1, chart_y_max - chart_y_min)
    for col in range(round(bracket_left), round(bracket_right) + 1):
        if col < 0 or col >= image_array.shape[1]:
            continue
        # Pre-existing mask: speed trace pixels (saturated).
        # Inline numpy operations here to keep the per-column cost low.
        col_pixels = image_array[chart_y_min : chart_y_max + 1, col, :]
        r = col_pixels[:, 0]
        g = col_pixels[:, 1]
        b = col_pixels[:, 2]
        max_c = np.maximum(np.maximum(r, g), b)
        min_c = np.minimum(np.minimum(r, g), b)
        sat = max_c - min_c
        mask = (max_c > 130.0) & (sat > 50.0)
        rows = np.flatnonzero(mask)
        if len(rows) == 0:
            columns.append((col, float("nan")))
            continue
        median_row = float(np.median(rows)) + chart_y_min
        # Filter out trace rows that span less than ~10% of the chart height
        # — they originate from axis labels and outliers.
        if median_row - chart_y_min < 0.06 * chart_height:
            columns.append((col, float("nan")))
            continue
        if chart_y_max - median_row < 0.06 * chart_height:
            columns.append((col, float("nan")))
            continue
        columns.append((col, median_row))
    return columns


def _smooth_chart_trace(trace_y: list[float]) -> list[float]:
    """Smooth a per-column trace Y list with a centred rolling mean.

    Edges are handled by replicating the boundary value so the rolling kernel
    is never divided by zero-padded entries — ``np.convolve(mode="same")``
    uses implicit zero-padding which corrupts the trace at bracket ends.
    """
    if not trace_y:
        return []
    arr = np.asarray(trace_y, dtype=np.float64)
    clean = arr[~np.isnan(arr)]
    if len(clean) == 0:
        return list(arr)
    fill = float(np.nanmedian(arr))
    arr = np.where(np.isnan(arr), fill, arr).astype(np.float64)
    win = max(3, int(len(arr) * CHART_TRACE_SMOOTH_WINDOW_FRAC))
    if win % 2 == 0:
        win += 1
    pad = win // 2
    padded = np.concatenate([np.full(pad, arr[0]), arr, np.full(pad, arr[-1])])
    kernel = np.ones(win, dtype=np.float64) / win
    smoothed = np.convolve(padded, kernel, mode="valid")
    return smoothed.tolist()


def _classify_local_speed_band(local_y: float, chart_y_min: int, chart_y_max: int) -> str:
    """Map a single trace Y position to a sub-band classification.

    The chart Y axis runs from top (fast) to bottom (slow). Fraction-from-top
    is mapped to one of the four canonical speed classes via
    :data:`CHART_TRACE_BANDS`.
    """
    chart_height = max(1, chart_y_max - chart_y_min)
    frac_from_top = (local_y - chart_y_min) / chart_height
    frac_from_top = max(0.0, min(1.0, frac_from_top))
    for threshold, classification in CHART_TRACE_BANDS:
        if frac_from_top <= threshold:
            return classification
    return CHART_TRACE_BANDS[-1][1]


def _detect_chart_trace_transitions(
    image_array: Any,
    bracket_left: float,
    bracket_right: float,
    chart_y_min: int,
    chart_y_max: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Find chart-trace inflection points within a bracket and classify them.

    Returns a tuple of ``(sub_sections, flags)``. ``sub_sections`` is empty
    unless the chart trace shows clear internal structure.  The detector
    uses a 3-partition median comparison: if the BRACKET-INTERNAL trace
    range exceeds a quarter of the chart height, the bracket is split into
    three parts (entry / middle / exit) and each part is classified from
    its local trace Y position relative to the chart Y range.

    Each sub-section is a dict ready to participate in the same downstream
    expansion pipeline as OCR-derived sub-sections::

        {
            "offset_x_min": float,
            "offset_x_max": float,
            "classification": str,
            "raw_text": str,
            "confidence": float,
            "source": "chart_trace",
        }
    """
    raw_columns = _sample_chart_trace_y(
        image_array, bracket_left, bracket_right, chart_y_min, chart_y_max
    )
    if not raw_columns:
        return [], []
    chart_height = max(1, chart_y_max - chart_y_min)
    valid_locals = [y for _, y in raw_columns if not np.isnan(y)]
    if len(valid_locals) < 0.4 * len(raw_columns):
        return [], []
    local_median = float(np.median(valid_locals))
    trace_y = [y if not np.isnan(y) else local_median for _, y in raw_columns]
    original_first_x = round(bracket_left)
    smoothed = _smooth_chart_trace(trace_y)
    if len(smoothed) < 8:
        return [], []
    smoothed_arr = np.asarray(smoothed)
    full_range = float(smoothed_arr.max() - smoothed_arr.min())
    # Trigger thresholds.
    min_threshold = max(
        CHART_TRACE_MIN_SWING_PX,
        CHART_TRACE_MIN_SWING_FRAC * chart_height,
    )
    force3_threshold = 0.30 * chart_height
    # If the bracket's smoothed trace spans more than a third of the chart
    # height we ALWAYS split into three sub-sections: an F1 speed trace that
    # swings from very-slow (apex of one corner) to very-fast (top of another)
    # always passes through a regime change, even if the partition medians
    # gloss over the inflection.
    if full_range < min_threshold:
        return [], []
    force3 = full_range >= force3_threshold
    # Compare 3 equal partitions (entry / middle / exit).  Find the
    # partition boundaries that differ from their neighbours by enough to
    # be a real sub-section boundary.
    # Compare 3 equal partitions (entry / middle / exit).  Find the
    # partition boundaries that differ from their neighbours by enough to
    # be a real sub-section boundary.
    third = max(8, len(smoothed) // 3)
    parts: list[tuple[int, int, float]] = []
    for k in (0, 1, 2):
        if k == 0:
            s, e = 0, third
        elif k == 1:
            s, e = third, 2 * third
        else:
            s, e = 2 * third, len(smoothed)
        parts.append((s, e, float(np.median(smoothed_arr[s:e]))))
    m0, m1, m2 = parts[0][2], parts[1][2], parts[2][2]
    # If the median difference between any consecutive pair of partitions is
    # above a small per-partition threshold (so the trace clearly changes
    # regime across the boundary), split there.  Otherwise keep the bracket
    # as a single sub-section.
    per_partition_threshold = max(8.0, 0.04 * chart_height)
    boundaries: list[int] = [original_first_x]
    if force3:
        # Always insert both 1/3 and 2/3 boundaries so wide-swing brackets
        # produce three sub-sections even when partition-median differences
        # are too small to fire the partition comparison above.
        c1 = original_first_x + parts[0][1]
        c2 = original_first_x + parts[1][1]
        boundaries.append(round(c1))
        boundaries.append(round(c2))
    else:
        if abs(m0 - m1) >= per_partition_threshold:
            cand_x = original_first_x + (parts[0][1] + parts[1][0]) // 2
            boundaries.append(round(cand_x))
        if abs(m1 - m2) >= per_partition_threshold:
            cand_x = original_first_x + (parts[1][1] + parts[2][0]) // 2
            boundaries.append(round(cand_x))
    boundaries.append(round(bracket_right))
    cleaned_boundaries: list[int] = [boundaries[0]]
    min_seg = max(
        CHART_TRACE_MIN_SUB_LENGTH_FRAC * (bracket_right - bracket_left),
        8.0,
    )
    for b in boundaries[1:-1]:
        if b - cleaned_boundaries[-1] >= min_seg:
            cleaned_boundaries.append(b)
    cleaned_boundaries.append(boundaries[-1])
    if len(cleaned_boundaries) < 3:
        return [], []
    flags: list[dict[str, Any]] = []
    sub_sections: list[dict[str, Any]] = []
    for index in range(len(cleaned_boundaries) - 1):
        seg_left_x = round(cleaned_boundaries[index])
        seg_right_x = round(cleaned_boundaries[index + 1])
        if seg_right_x <= seg_left_x:
            continue
        seg_ys = trace_y[
            max(0, seg_left_x - original_first_x) : max(0, seg_right_x - original_first_x)
        ]
        seg_ys_v = [y for y in seg_ys if not np.isnan(y)]
        if not seg_ys_v:
            continue
        seg_mean_y = float(np.mean(seg_ys_v))
        seg_local_min = float(min(seg_ys_v))
        seg_local_max = float(max(seg_ys_v))
        seg_classification = _classify_local_speed_band(seg_mean_y, chart_y_min, chart_y_max)
        chart_frac = (seg_local_max - seg_local_min) / chart_height
        conf = min(100.0, 60.0 + chart_frac * 240.0)
        sub_sections.append(
            {
                "offset_x_min": float(seg_left_x),
                "offset_x_max": float(seg_right_x),
                "classification": seg_classification,
                "raw_text": "chart_trace_inferred",
                "confidence": round(conf, 2),
                "source": "chart_trace",
            }
        )
    if len(sub_sections) <= 1:
        return [], flags
    flags.append(
        _review_flag(
            "CHART_TRACE_SUBSECTIONS_DETECTED",
            "info",
            f"Chart-trace analysis subdivided bracket into {len(sub_sections)} sub-sections.",
            [],
            blocks=False,
            alternative_evidence={
                "boundary_count": len(sub_sections) + 1,
                "min_swing_px": round(force3_threshold, 2),
            },
        )
    )
    return sub_sections, flags


def _extract_subsections_for_bracket(
    image: Any,
    bracket: dict[str, Any],
    config: ExtractorConfig,
    *,
    plot_bounds: Bounds | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Run OCR on a single bracket box and return sub-section labels.

    Returns a tuple of (sub_sections, flags). Each sub_section is::

        {
            "offset_x_min": float,  # relative to bracket span, in full-image px
            "offset_x_max": float,
            "classification": str,
            "raw_text": str,
            "confidence": float,
        }
    """
    span = bracket["full_image_span"]
    left_x = int(span["x_min"])
    right_x = int(span["x_max"])
    top_y = int(span["y_min"])
    bottom_y = int(span["y_max"])
    if right_x <= left_x or bottom_y <= top_y:
        return [], []
    cropped_image = image.crop((left_x, top_y, right_x + 1, bottom_y + 1))
    enhanced = _enhance_for_sub_section_ocr(cropped_image)
    flags: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="tif1-telemetry-sub-") as temporary:
        crop_path = Path(temporary) / "subsection-band.png"
        enhanced.save(crop_path, format="PNG")
        try:
            completed = subprocess.run(
                [
                    config.tesseract_cmd,
                    str(crop_path),
                    "stdout",
                    "--psm",
                    "11",
                    "-l",
                    "eng",
                    "tsv",
                ],
                capture_output=True,
                text=True,
                check=False,
                timeout=15,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            flags.append(
                _review_flag(
                    "SUBSECTION_OCR_UNAVAILABLE",
                    "warning",
                    f"Sub-section OCR could not execute Tesseract: {exc}",
                    [bracket.get("bracket_id", "")],
                    blocks=False,
                )
            )
            return [], flags
    if completed.returncode != 0:
        flags.append(
            _review_flag(
                "SUBSECTION_OCR_UNAVAILABLE",
                "warning",
                completed.stderr.strip() or "Sub-section OCR returned non-zero exit code",
                [bracket.get("bracket_id", "")],
                blocks=False,
            )
        )
        return [], flags
    text = completed.stdout or ""
    if not text.strip():
        return [], flags
    accepted_words: list[dict[str, Any]] = []
    lines = text.splitlines()
    if not lines:
        return [], flags
    header = lines[0].split("\t")
    positions = {name: index for index, name in enumerate(header)}
    required_keys = {"level", "left", "top", "width", "height", "conf", "text"}
    if not required_keys.issubset(positions):
        flags.append(
            _review_flag(
                "SUBSECTION_OCR_UNAVAILABLE",
                "warning",
                "Sub-section OCR output did not include the expected TSV columns.",
                [bracket.get("bracket_id", "")],
                blocks=False,
            )
        )
        return [], flags
    for raw_line in lines[1:]:
        fields = raw_line.split("\t")
        if len(fields) <= max(positions.values()):
            continue
        try:
            level = int(fields[positions["level"]])
            confidence = float(fields[positions["conf"]])
        except (KeyError, ValueError):
            continue
        if level != 5:
            continue
        if confidence < OCR_SUBSECTION_CONFIDENCE:
            continue
        try:
            crop_left = int(fields[positions["left"]])
            crop_top = int(fields[positions["top"]])
            crop_width = int(fields[positions["width"]])
            crop_height = int(fields[positions["height"]])
        except (KeyError, ValueError):
            continue
        word = fields[positions["text"]].strip()
        if not word:
            continue
        full_image_x_min = left_x + crop_left
        full_image_y_min = top_y + crop_top
        full_image_x_max = full_image_x_min + crop_width
        full_image_y_max = full_image_y_min + crop_height
        cleaned = _strip_ocr_noise(word)
        if not cleaned:
            continue
        classification = _match_subsection_label(cleaned)
        if classification is None:
            continue
        accepted_words.append(
            {
                "word": word,
                "cleaned": cleaned,
                "classification": classification,
                "confidence": confidence,
                "bbox_full_image": {
                    "x_min": full_image_x_min,
                    "x_max": full_image_x_max,
                    "y_min": full_image_y_min,
                    "y_max": full_image_y_max,
                },
            }
        )
    if not accepted_words:
        if plot_bounds is not None:
            bracket_left = float(
                bracket.get("guide_lines", [{}])[0].get("boundary_x", span["x_min"])
            )
            bracket_right = float(
                bracket.get("guide_lines", [{}, {}])[1].get("boundary_x", span["x_max"])
            )
            image_array = np.asarray(image.convert("RGB"), dtype=np.float32)
            trace_subs, trace_flags = _detect_chart_trace_transitions(
                image_array,
                bracket_left,
                bracket_right,
                chart_y_min=plot_bounds.y_min,
                chart_y_max=plot_bounds.y_min + max(int(plot_bounds.height * 0.6), 60),
            )
            return trace_subs, trace_flags
        return [], []
    accepted_words.sort(
        key=lambda item: (item["bbox_full_image"]["y_min"], item["bbox_full_image"]["x_min"])
    )
    # First cluster accepted words into y-aligned rows. Each "row" is the set of
    # words with similar y_min/y_max (within SUBSECTION_Y_TOLERANCE_PX).
    rows: list[list[dict[str, Any]]] = []
    current_row: list[dict[str, Any]] = [accepted_words[0]]
    for word in accepted_words[1:]:
        current_y_min = current_row[0]["bbox_full_image"]["y_min"]
        word_y_min = word["bbox_full_image"]["y_min"]
        current_y_max = max(w["bbox_full_image"]["y_max"] for w in current_row)
        if (
            abs(word_y_min - current_y_min) <= SUBSECTION_Y_TOLERANCE_PX
            and abs(word["bbox_full_image"]["y_max"] - current_y_max) <= SUBSECTION_Y_TOLERANCE_PX
        ):
            current_row.append(word)
        else:
            rows.append(current_row)
            current_row = [word]
    rows.append(current_row)
    # Within each row, split into sub-clusters whenever the gap between
    # adjacent word bboxes exceeds SUBSECTION_X_GAP_PX. A large gap is the
    # natural boundary between two distinct labels (e.g. STRAIGHT followed
    # by HIGH SPEED). All words sharing the same horizontal cluster form
    # ONE sub-section.
    clusters: list[list[dict[str, Any]]] = []
    for row in rows:
        row.sort(key=lambda item: item["bbox_full_image"]["x_min"])
        current_cluster: list[dict[str, Any]] = [row[0]]
        for word in row[1:]:
            previous_x_max = max(w["bbox_full_image"]["x_max"] for w in current_cluster)
            gap = word["bbox_full_image"]["x_min"] - previous_x_max
            if gap > SUBSECTION_X_GAP_PX:
                clusters.append(current_cluster)
                current_cluster = [word]
            else:
                current_cluster.append(word)
        clusters.append(current_cluster)
    cluster_summaries: list[dict[str, Any]] = []
    for word_cluster in clusters:
        word_cluster.sort(key=lambda item: item["bbox_full_image"]["x_min"])
        classification_counts: dict[str, list[dict[str, Any]]] = {}
        for word in word_cluster:
            classification_counts.setdefault(word["classification"], []).append(word)
        chosen_classification, chosen_words_list = max(
            classification_counts.items(),
            key=lambda item: (
                len(item[1]),
                sum(w["confidence"] for w in item[1]) / len(item[1]),
            ),
        )
        avg_confidence = sum(w["confidence"] for w in chosen_words_list) / len(chosen_words_list)
        cluster_summaries.append(
            {
                "classification": chosen_classification,
                "raw_text": " ".join(w["word"] for w in word_cluster),
                "confidence": avg_confidence,
                "x_min": min(w["bbox_full_image"]["x_min"] for w in word_cluster),
                "x_max": max(w["bbox_full_image"]["x_max"] for w in word_cluster),
                "y_min": min(w["bbox_full_image"]["y_min"] for w in word_cluster),
                "y_max": max(w["bbox_full_image"]["y_max"] for w in word_cluster),
            }
        )
    row_summaries = cluster_summaries
    if not row_summaries:
        return [], []
    avg = sum(r["confidence"] for r in row_summaries) / len(row_summaries)
    if avg < OCR_SUBSECTION_LOW_CONFIDENCE:
        flags.append(
            _review_flag(
                "SUBSECTION_OCR_LOW_CONFIDENCE",
                "warning",
                f"Sub-section OCR average confidence {avg:.1f} is below {OCR_SUBSECTION_LOW_CONFIDENCE:g}.",
                [bracket.get("bracket_id", "")],
                blocks=False,
                alternative_evidence={"row_count": len(row_summaries)},
            )
        )
    row_summaries.sort(key=lambda r: r["x_min"])
    # Authoritative sub-section boundaries come from the bracket's guide-line
    # ``boundary_x`` (the float centre) so that sub-section offsets match the
    # parent's exact coordinate. Reading ``full_image_span`` would otherwise
    # round ``1214.5`` down to ``1214`` for a candidate window whose left edge
    # is detected at x=1214 while its visual centre is at 1214.5, producing
    # an overlap with the neighbour when the sub-section is later expanded.
    bracket_left = float(bracket.get("guide_lines", [{}])[0].get("boundary_x", span["x_min"]))
    bracket_right = float(bracket.get("guide_lines", [{}, {}])[1].get("boundary_x", span["x_max"]))
    sub_sections: list[dict[str, Any]] = []
    cursor = bracket_left
    for index, row in enumerate(row_summaries):
        boundary = float((row["x_min"] + row["x_max"]) / 2.0)
        if index == 0:
            sub_left = bracket_left
        else:
            prev_right = sub_sections[-1]["offset_x_max"]
            sub_left = (prev_right + boundary) / 2.0
        if index == len(row_summaries) - 1:
            sub_right = bracket_right
        else:
            next_left = row_summaries[index + 1]["x_min"]
            sub_right = (boundary + next_left) / 2.0
        sub_left = max(bracket_left, min(sub_left, bracket_right))
        sub_right = max(bracket_left, min(sub_right, bracket_right))
        if sub_right <= cursor:
            sub_right = min(bracket_right, cursor + (sub_right - sub_left))
        sub_sections.append(
            {
                "offset_x_min": cursor,
                "offset_x_max": sub_right,
                "classification": row["classification"],
                "raw_text": row["raw_text"],
                "confidence": row["confidence"],
            }
        )
        cursor = sub_right
    return sub_sections, flags


def associate_labels(
    brackets: list[dict[str, Any]],
    detections: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Associate labels to brackets by overlap, with midpoint tie-breaking."""
    for bracket in brackets:
        bracket["associated_label_detection_id"] = None
        bracket["association_method"] = None
        bracket["association_confidence"] = 0.0

    claims: dict[str, list[str]] = {}
    flags: list[dict[str, Any]] = []
    for detection in detections:
        if detection.get("classification_candidate") is None:
            continue
        bbox = detection["bbox_full_image"]
        label_min, label_max = bbox["x_min"], bbox["x_max"]
        label_center = (label_min + label_max) / 2.0
        scored: list[tuple[float, float, dict[str, Any]]] = []
        for bracket in brackets:
            span = bracket["full_image_span"]
            bracket_min, bracket_max = span["x_min"], span["x_max"]
            overlap = max(0, min(label_max, bracket_max) - max(label_min, bracket_min))
            midpoint_distance = abs(label_center - (bracket_min + bracket_max) / 2.0)
            scored.append((float(overlap), -midpoint_distance, bracket))
        if not scored:
            continue
        scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
        overlap, _, bracket = scored[0]
        span_width = max(
            1, bracket["full_image_span"]["x_max"] - bracket["full_image_span"]["x_min"]
        )
        bracket["associated_label_detection_id"] = detection["detection_id"]
        bracket["association_method"] = "maximum_horizontal_overlap"
        bracket["association_confidence"] = round(min(1.0, overlap / span_width), 4)
        claims.setdefault(bracket["bracket_id"], []).append(detection["detection_id"])
    for bracket in brackets:
        bracket_id = bracket["bracket_id"]
        ids = claims.get(bracket_id, [])
        if len(ids) > 1:
            bracket["association_method"] = "maximum_horizontal_overlap_tie_break"
            flags.append(
                _review_flag(
                    "MULTIPLE_BRACKET_MATCHES",
                    "warning",
                    f"Multiple OCR labels were associated with {bracket_id}; the last maximum-overlap "
                    "candidate is selected.",
                    [bracket_id, *ids],
                    blocks=False,
                )
            )
        elif not ids:
            flags.append(
                _review_flag(
                    "BRACKET_NOT_ASSOCIATED",
                    "warning",
                    f"No recognized OCR label was associated with {bracket_id}; it is classified as straight "
                    "when OCR is available.",
                    [bracket_id],
                    blocks=False,
                    selected_interpretation="straight",
                )
            )
    associated_ids = {detection_id for ids in claims.values() for detection_id in ids}
    for detection in detections:
        if (
            detection.get("classification_candidate") is not None
            and detection["detection_id"] not in associated_ids
        ):
            flags.extend(
                [
                    _review_flag(
                        "LABEL_NOT_ASSOCIATED",
                        "warning",
                        f"Recognized OCR label {detection['detection_id']} did not overlap a bracket.",
                        [detection["detection_id"]],
                        blocks=False,
                    )
                ]
            )
    return brackets, flags


def _transition_at_endpoint(gray: Any, x: float, bounds: Bounds) -> float | None:
    """Estimate a background transition near an endpoint from column medians."""
    y_min = bounds.y_min + max(8, int(bounds.height * 0.12))
    y_max = bounds.y_max - max(8, int(bounds.height * 0.12))
    if y_max <= y_min:
        return None
    profile = np.median(gray[y_min : y_max + 1], axis=0)
    left = max(bounds.x_min + 2, round(x) - 8)
    right = min(bounds.x_max - 2, round(x) + 8)
    if right - left < 4:
        return None
    differences = np.abs(np.diff(profile[left : right + 1]))
    if len(differences) == 0 or float(differences.max()) < 3.0:
        return None
    return float(left + int(np.argmax(differences)) + 0.5)


def validate_color_transitions(
    brackets: list[dict[str, Any]],
    image: Any,
    plot_bounds: Bounds,
    tolerance: float,
) -> list[dict[str, Any]]:
    """Retain independent background-transition evidence without overriding guides."""
    gray = np.asarray(image.convert("RGB"), dtype=np.float32).mean(axis=2)
    validations = []
    for bracket in brackets:
        guides = bracket["guide_lines"]
        observed_start = _transition_at_endpoint(gray, guides[0]["boundary_x"], plot_bounds)
        observed_end = _transition_at_endpoint(gray, guides[1]["boundary_x"], plot_bounds)
        expected_start = guides[0]["boundary_x"]
        expected_end = guides[1]["boundary_x"]
        if observed_start is None or observed_end is None:
            validations.append(
                {
                    "bracket_id": bracket["bracket_id"],
                    "expected_span": {"x_min": expected_start, "x_max": expected_end},
                    "observed_background_transition": None,
                    "difference_px": None,
                    "within_tolerance": None,
                    "tolerance_px": tolerance,
                    "detected": False,
                }
            )
            continue
        differences = {
            "start": round(abs(expected_start - observed_start), 3),
            "end": round(abs(expected_end - observed_end), 3),
        }
        within = max(differences.values()) <= tolerance
        validations.append(
            {
                "bracket_id": bracket["bracket_id"],
                "expected_span": {"x_min": expected_start, "x_max": expected_end},
                "observed_background_transition": {"x_min": observed_start, "x_max": observed_end},
                "difference_px": differences,
                "within_tolerance": within,
                "tolerance_px": tolerance,
                "detected": True,
            }
        )
    return validations


def _boundary(x: float, plot_bounds: Bounds, line: dict[str, Any] | None = None) -> dict[str, Any]:
    plot_width = float(plot_bounds.width)
    local_x = x - plot_bounds.x_min
    result: dict[str, Any] = {
        "full_image_x": x,
        "chart_local_x": local_x,
        "pixel_distance_from_plot_start": local_x,
        "normalized_position": local_x / plot_width if plot_width else None,
        "lap_percentage": local_x / plot_width * 100 if plot_width else None,
        "line_x_min": None,
        "line_x_max": None,
        "boundary_x": x,
        "uncertainty_px": 0.5,
        "coordinate_source": "plot_edge",
    }
    if line is not None:
        result.update(
            {
                "line_x_min": line["line_x_min"],
                "line_x_max": line["line_x_max"],
                "boundary_x": line["boundary_x"],
                "uncertainty_px": line["uncertainty_px"],
                "coordinate_source": "guide_line_center",
            }
        )
    for key in (
        "full_image_x",
        "chart_local_x",
        "pixel_distance_from_plot_start",
        "normalized_position",
        "lap_percentage",
        "boundary_x",
        "uncertainty_px",
    ):
        if isinstance(result[key], float):
            result[key] = round(result[key], 6)
    return result


def _interval_confidence(
    bracket: dict[str, Any], ocr_by_id: dict[str, dict[str, Any]], ocr_available: bool
) -> float:
    score = 0.75 if bracket.get("horizontal_stroke_score", 0) >= 0.12 else 0.5
    detection_id = bracket.get("associated_label_detection_id")
    if detection_id and detection_id in ocr_by_id:
        score = min(score + 0.2, ocr_by_id[detection_id]["confidence"] / 100.0)
    elif not ocr_available:
        score = min(score, 0.55)
    return round(score, 4)


def _expand_brackets_with_subsections(
    brackets: list[dict[str, Any]],
    subsections_data: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Replace a reviewed bracket with one sub-bracket per detected sub-section.

    For every bracket whose id appears in ``subsections_data`` we replace the
    single entry with multiple new entries whose guide-line span is restricted
    to the sub-section's x range. The new entries copy the parent's
    identification (displayed_sector / displayed_section) and append a
    sequential subsection index so the final label reads ``S.N+K``.

    Brackets without detected sub-sections are returned unchanged so the
    fallback behaviour is identical to the previous pipeline.
    """
    expanded: list[dict[str, Any]] = []
    for bracket in brackets:
        key = bracket.get("manual_override_id") or bracket.get("bracket_id")
        sub_sections = subsections_data.get(key or "")
        if not sub_sections:
            expanded.append(bracket)
            continue
        parent_sector = bracket.get("displayed_sector")
        parent_section = bracket.get("displayed_section", 1) or 1
        for sub_index, sub in enumerate(sub_sections, start=1):
            new_bracket = deepcopy(bracket)
            new_bracket["bracket_id"] = f"{bracket['bracket_id']}-sub-{sub_index:02d}"
            sub_left = float(sub["offset_x_min"])
            sub_right = float(sub["offset_x_max"])
            # ``boundary_x`` is the authoritative coordinate used by
            # ``build_intervals``; keep it as the unrounded float to avoid
            # integer-rounding drift that would otherwise overlap the
            # neighbouring bracket's boundary (e.g. 1214.5 vs 1214).
            new_bracket["guide_lines"] = [
                _guide_line(
                    "left",
                    round(sub_left),
                    round(sub_left),
                    sub_left,
                    band=Bounds(
                        round(sub_left),
                        round(sub_left),
                        0,
                        0,
                    ),
                ),
                _guide_line(
                    "right",
                    round(sub_right),
                    round(sub_right),
                    sub_right,
                    band=Bounds(
                        round(sub_right),
                        round(sub_right),
                        0,
                        0,
                    ),
                ),
            ]
            new_bracket["full_image_span"] = {
                "x_min": round(sub_left),
                "x_max": round(sub_right),
                "y_min": bracket["full_image_span"]["y_min"],
                "y_max": bracket["full_image_span"]["y_max"],
            }
            new_bracket["chart_local_span"] = {
                "x_min": sub_left,
                "x_max": sub_right,
            }
            new_bracket["horizontal_stroke_score"] = 1.0
            new_bracket["associated_label_detection_id"] = (
                f"subsection:{bracket['bracket_id']}:{sub_index}"
            )
            new_bracket["association_method"] = "sub_section_ocr"
            new_bracket["association_confidence"] = round(sub["confidence"] / 100.0, 4)
            new_bracket["sub_section_classification"] = sub["classification"]
            # When the only sub-section the OCR detected spans the whole
            # parent bracket (a single ``HIGH SPEED`` reading inside what was
            # already a single labelled bracket), keep the parent's display
            # label instead of bumping the section index by 100. The 100x
            # scaling is reserved for genuine splits of one reviewed bracket
            # into multiple sub-sections.
            if len(sub_sections) == 1:
                new_bracket["displayed_section"] = parent_section
                new_bracket["display_label"] = (
                    f"{parent_sector}.{parent_section}"
                    if parent_sector is not None
                    else f"{bracket['bracket_id']}-sub-{sub_index}"
                )
            else:
                new_bracket["displayed_section"] = (parent_section - 1) * 100 + sub_index
                new_bracket["display_label"] = (
                    f"{parent_sector}.{new_bracket['displayed_section']}"
                    if parent_sector is not None
                    else f"{bracket['bracket_id']}-sub-{sub_index}"
                )
            expanded.append(new_bracket)
    return expanded


def build_intervals(
    plot_bounds: Bounds,
    brackets: list[dict[str, Any]],
    ocr_detections: list[dict[str, Any]],
    ocr_available: bool,
) -> list[dict[str, Any]]:
    """Construct one contiguous half-open partition, preserving every bracket."""
    ocr_by_id = {item["detection_id"]: item for item in ocr_detections}
    sorted_brackets = sorted(brackets, key=lambda item: item["guide_lines"][0]["boundary_x"])
    boundaries: dict[float, dict[str, Any]] = {
        float(plot_bounds.x_min): _boundary(float(plot_bounds.x_min), plot_bounds),
        float(plot_bounds.x_max): _boundary(float(plot_bounds.x_max), plot_bounds),
    }
    for bracket in sorted_brackets:
        for guide in bracket["guide_lines"]:
            boundaries[float(guide["boundary_x"])] = _boundary(
                float(guide["boundary_x"]), plot_bounds, guide
            )

    intervals: list[dict[str, Any]] = []
    cursor = float(plot_bounds.x_min)
    sequence = 1
    for bracket in sorted_brackets:
        start = float(bracket["guide_lines"][0]["boundary_x"])
        end = float(bracket["guide_lines"][1]["boundary_x"])
        if start < cursor:
            raise ValueError(
                f"overlapping bracket {bracket['bracket_id']} cannot produce a contiguous partition"
            )
        if start > cursor:
            intervals.append(
                _make_interval(
                    sequence,
                    "straight",
                    "inferred_gap" if cursor > plot_bounds.x_min else "plot_leading_gap",
                    boundaries[cursor],
                    boundaries[start],
                    None,
                    None,
                    0.8,
                )
            )
            sequence += 1
        detection_id = bracket.get("associated_label_detection_id")
        detection = ocr_by_id.get(detection_id) if detection_id else None
        # A sub-section expansion stamps a direct classification override on the
        # bracket, taking precedence over OCR-detected associations.
        sub_section_override = bracket.get("sub_section_classification")
        if detection is not None:
            classification = detection.get("classification_candidate") or "straight"
        else:
            classification = "straight" if ocr_available else "unresolved"
        if sub_section_override is not None:
            classification = sub_section_override
        interval_flags: list[dict[str, Any]] = []
        if detection is not None and not detection.get("accepted", True):
            interval_flags.append("OCR_BELOW_THRESHOLD")
        if classification == "unresolved":
            interval_flags.append("OCR_UNAVAILABLE")
        if (
            bracket.get("association_method") is None
            and ocr_available
            and sub_section_override is None
        ):
            interval_flags.append("BRACKET_NOT_ASSOCIATED")
        intervals.append(
            _make_interval(
                sequence,
                classification,
                "bracket",
                boundaries[start],
                boundaries[end],
                bracket["bracket_id"],
                detection_id,
                _interval_confidence(bracket, ocr_by_id, ocr_available),
                interval_flags,
                bracket.get("displayed_sector"),
                bracket.get("displayed_section"),
            )
        )
        sequence += 1
        cursor = end

    if cursor < plot_bounds.x_max:
        intervals.append(
            _make_interval(
                sequence,
                "straight",
                "plot_trailing_gap" if cursor >= plot_bounds.x_min else "inferred_gap",
                boundaries[cursor],
                boundaries[float(plot_bounds.x_max)],
                None,
                None,
                0.8,
            )
        )
    if intervals:
        intervals[-1]["interval_bounds"] = "[start, end]"
    return intervals


def _make_interval(
    sequence: int,
    classification: str,
    source: str,
    start: dict[str, Any],
    end: dict[str, Any],
    bracket_id: str | None,
    label_detection_id: str | None,
    confidence: float,
    review_flags: list[str] | None = None,
    displayed_sector: int | None = None,
    displayed_section: int | None = None,
) -> dict[str, Any]:
    if classification not in ALLOWED_CLASSIFICATIONS:
        raise ValueError(f"Unsupported interval classification: {classification}")
    return {
        "sequence": sequence,
        "classification": classification,
        "classification_display": DISPLAY_CLASSIFICATIONS[classification],
        "interval_bounds": "[start, end)",
        "start": start,
        "end": end,
        "source": source,
        "bracket_id": bracket_id,
        "label_detection_id": label_detection_id,
        "displayed_sector": displayed_sector,
        "displayed_section": displayed_section,
        "display_label": (
            f"{displayed_sector}.{displayed_section}"
            if displayed_sector is not None and displayed_section is not None
            else None
        ),
        "confidence": round(confidence, 4),
        "review_required": bool(review_flags),
        "review_flags": review_flags or [],
    }


def validate_intervals(intervals: Sequence[dict[str, Any]], plot_bounds: Bounds) -> dict[str, Any]:
    """Validate partition invariants and return machine-readable results."""
    errors: list[str] = []
    if not intervals:
        errors.append("no intervals were produced")
    else:
        first = intervals[0]["start"]["full_image_x"]
        last = intervals[-1]["end"]["full_image_x"]
        if first != plot_bounds.x_min:
            errors.append("first interval does not start at plot start")
        if last != plot_bounds.x_max:
            errors.append("last interval does not end at plot end")
        for current, following in pairwise(intervals):
            if current["end"]["full_image_x"] != following["start"]["full_image_x"]:
                errors.append(
                    f"gap between interval {current['sequence']} and {following['sequence']}"
                )
        for interval in intervals:
            if interval["end"]["full_image_x"] < interval["start"]["full_image_x"]:
                errors.append(f"negative width in interval {interval['sequence']}")
            if interval["classification"] not in ALLOWED_CLASSIFICATIONS:
                errors.append(f"invalid classification in interval {interval['sequence']}")
    return {
        "intervals_contiguous": not any(error.startswith("gap between") for error in errors),
        "plot_partition_complete": not any(
            error
            in {
                "first interval does not start at plot start",
                "last interval does not end at plot end",
            }
            for error in errors
        ),
        "non_negative_widths": not any("negative width" in error for error in errors),
        "allowed_classifications": not any("invalid classification" in error for error in errors),
        "errors": errors,
    }


def _review_flag(
    code: str,
    severity: str,
    message: str,
    related_ids: Iterable[str],
    *,
    blocks: bool,
    selected_interpretation: str | None = None,
    alternative_evidence: Any = None,
) -> dict[str, Any]:
    return {
        "flag_id": "",
        "severity": severity,
        "code": code,
        "message": message,
        "related_ids": list(related_ids),
        "selected_interpretation": selected_interpretation,
        "alternative_evidence": alternative_evidence if alternative_evidence is not None else [],
        "blocks_completion": blocks,
    }


def _finalize_flags(flags: list[dict[str, Any]]) -> None:
    for index, flag in enumerate(flags, start=1):
        flag["flag_id"] = f"flag-{index:03d}"


def _title_for_source(source: dict[str, Any]) -> dict[str, Any]:
    source_key = source["source_key"]
    raw_title = KNOWN_TITLES.get(source_key, "")
    return {
        "raw_title": raw_title,
        "venue_display": raw_title.split(" - ", 1)[0].title() if raw_title else None,
        "source_identity_rule": "preserve_displayed_title",
        "detection_method": "known_graphic_title" if raw_title else "not_detected",
        "confidence": 1.0 if raw_title else 0.0,
    }


def _provenance(config: ExtractorConfig, ocr: dict[str, Any]) -> dict[str, Any]:
    return {
        "extractor": "scripts/extract_telemetry_sectors.py",
        "extractor_version": EXTRACTOR_VERSION,
        "schema_version": SCHEMA_VERSION,
        "extraction_timestamp_utc": datetime.now(UTC).isoformat(),
        "python_version": sys.version.split()[0],
        "configuration": {
            "ocr_threshold": config.ocr_threshold,
            "tesseract_cmd": config.tesseract_cmd,
            "color_tolerance_px": config.color_tolerance_px,
            "fallback_bounds": config.fallback_bounds,
            "annotation_enabled": config.annotate,
        },
        "ocr_engine": ocr.get("engine"),
        "ocr_version": ocr.get("version"),
    }


def _status(
    flags: Sequence[dict[str, Any]], validation: dict[str, Any], ocr_available: bool
) -> str:
    if not validation["plot_partition_complete"] or not validation["intervals_contiguous"]:
        return "incomplete"
    if not ocr_available:
        return "incomplete"
    if any(flag["blocks_completion"] for flag in flags):
        return "incomplete"
    return "provisional" if flags else "complete"


def extract_image(path: Path, config: ExtractorConfig) -> dict[str, Any]:
    """Analyze one image and return the authoritative result dictionary."""
    source, image = source_metadata(path)
    plot_bounds, plot_evidence, flags = detect_plot_bounds(image, config.fallback_bounds)
    band = detect_bracket_band(plot_bounds, image.size)
    brackets, bracket_evidence, bracket_flags = detect_brackets(image, plot_bounds, band)
    flags.extend(bracket_flags)
    detections, ocr_evidence, ocr_flags = run_ocr(image, band, config)
    flags.extend(ocr_flags)
    brackets, manual_detections, manual_label_evidence, manual_flags = apply_manual_label_overrides(
        source["source_key"],
        brackets,
        bracket_evidence,
        plot_bounds,
        band,
        source_sha256=source["sha256"],
        # Native visual review is authoritative for the reviewed Hungary hash;
        # OCR remains supporting evidence and never overrides it.
        allow_override=True,
    )
    flags.extend(manual_flags)
    bracket_evidence["detected_brackets"] = deepcopy(bracket_evidence.get("brackets", brackets))
    bracket_evidence["selected_brackets"] = deepcopy(brackets)
    if manual_label_evidence["applied"]:
        bracket_evidence["brackets"] = brackets
    all_detections = [*detections, *manual_detections]
    # If sub-section OCR expanded some brackets into N sub-brackets, the
    # band-level detections that fall inside any of those expanded brackets
    # must be filtered out so ``associate_labels`` does not re-associate them
    # with conflicting classifications (the sub-section OCR is authoritative
    # for sub-section brackets).
    sub_section_bracket_spans: list[dict[str, Any]] = [
        {
            "span": bracket["full_image_span"],
            "bracket_id": bracket["bracket_id"],
        }
        for bracket in brackets
        if bracket.get("sub_section_classification") is not None
    ]
    if sub_section_bracket_spans:
        filtered_detections = [
            detection
            for detection in all_detections
            if not _detection_inside_any_span(detection, sub_section_bracket_spans)
        ]
        if len(filtered_detections) != len(all_detections):
            for dropped in all_detections:
                if dropped in filtered_detections:
                    continue
                flags.append(
                    _review_flag(
                        "SUBSECTION_OCR_OVERRIDES_BAND_LABEL",
                        "info",
                        f"Sub-section OCR is authoritative: dropped band-level detection "
                        f"{dropped.get('detection_id')} whose bbox overlaps a flagged sub-section bracket.",
                        [dropped.get("detection_id", "")],
                        blocks=False,
                    )
                )
            all_detections = filtered_detections
    brackets, association_flags = associate_labels(brackets, all_detections)
    flags.extend(association_flags)
    sorted_brackets = sorted(brackets, key=lambda item: item["guide_lines"][0]["boundary_x"])
    previous_end = float(plot_bounds.x_min)
    for bracket in sorted_brackets:
        start = float(bracket["guide_lines"][0]["boundary_x"])
        end = float(bracket["guide_lines"][1]["boundary_x"])
        if start < previous_end:
            flags.append(
                _review_flag(
                    "OVERLAPPING_BRACKETS",
                    "error",
                    f"{bracket['bracket_id']} overlaps a preceding bracket; all raw bracket evidence is retained "
                    "but a contiguous finalized partition cannot be claimed.",
                    [bracket["bracket_id"]],
                    blocks=True,
                )
            )
        previous_end = max(previous_end, end)
    color_validations = validate_color_transitions(
        brackets, image, plot_bounds, config.color_tolerance_px
    )
    flags.extend(
        _review_flag(
            "GUIDE_COLOR_BOUNDARY_MISMATCH",
            "warning",
            f"{item['bracket_id']} guide-line and background transition measurements differ "
            f"beyond {item['tolerance_px']} pixels.",
            [item["bracket_id"]],
            blocks=False,
            selected_interpretation="guide_line_center",
            alternative_evidence=item,
        )
        for item in color_validations
        if item["detected"] and not item["within_tolerance"]
    )
    subsections_data: dict[str, list[dict[str, Any]]] = {}
    for bracket in sorted(brackets, key=lambda item: item["guide_lines"][0]["boundary_x"]):
        sub_sections, sub_flags = _extract_subsections_for_bracket(
            image, bracket, config, plot_bounds=plot_bounds
        )
        flags.extend(sub_flags)
        if not sub_sections:
            continue
        key = bracket.get("manual_override_id") or bracket.get("bracket_id") or ""
        subsections_data[key] = sub_sections
    brackets = _expand_brackets_with_subsections(brackets, subsections_data)

    try:
        intervals = build_intervals(
            plot_bounds, brackets, all_detections, ocr_evidence["available"]
        )
    except ValueError as exc:
        intervals = []
        flags.append(_review_flag("NONCONTIGUOUS_INTERVALS", "error", str(exc), [], blocks=True))
    validation = validate_intervals(intervals, plot_bounds)
    flags.extend(
        _review_flag("NONCONTIGUOUS_INTERVALS", "error", error, [], blocks=True)
        for error in validation["errors"]
    )
    _finalize_flags(flags)
    result = {
        "schema_version": SCHEMA_VERSION,
        "analysis_status": _status(flags, validation, ocr_evidence["available"]),
        "source": source,
        "event": _title_for_source(source),
        "coordinate_system": {
            "distance_basis": "image_pixels",
            "progression_axis": "horizontal_left_to_right",
            "authoritative_reference_frame": "full_image",
            "chart_local_reference_frame": "detected_plot_origin",
            "normalized_unit": "fraction_of_plot_width",
            "percentage_unit": "percent_of_plot_width",
            "endpoint_convention": "half_open_except_final",
        },
        "plot_bounds": plot_evidence,
        "bracket_band": band.as_dict(),
        "intervals": intervals,
        "evidence": {
            "brackets": bracket_evidence,
            "ocr": ocr_evidence,
            "manual_label_overrides": manual_label_evidence,
            "manual_label_detections": manual_detections,
            "color_transition_validation": color_validations,
        },
        "validation": validation,
        "review_flags": flags,
        "provenance": _provenance(config, ocr_evidence),
    }
    if config.annotate:
        result["_image"] = image
    return result


def _fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def render_markdown(result: dict[str, Any], json_name: str, png_name: str) -> str:
    """Render a detailed human-readable report from the JSON data structure."""
    source = result["source"]
    plot = result["plot_bounds"]["full_image"]
    lines = [
        f"# Circuit telemetry sectors: `{source['source_key']}`",
        "",
        f"- **Source:** `{source['path']}`",
        f"- **Displayed event title:** {result['event']['raw_title'] or 'Not detected'}",
        f"- **Analysis status:** `{result['analysis_status']}`",
        f"- **Source image:** {source['width_px']}×{source['height_px']} {source['format']} ({source['color_mode']})",
        f"- **Machine-readable output:** {json_name if json_name.startswith('(') else f'[`{json_name}`]({json_name})'}",
        f"- **Annotated audit image:** {png_name if png_name.startswith('(') else f'[`{png_name}`]({png_name})'}",
        "",
        "## Coordinate convention",
        "",
        "Distances are horizontal image pixels from the detected plot start. Full-image x is authoritative; chart-local x is derived from the detected plot origin. Percentages are normalized positions on the image axis, not physical circuit distances. No kilometers, turn distances, or racing-line distances are inferred. Intervals are half-open (`[start, end)`) except the final interval, which is closed on the right.",
        "",
        "## Plot bounds",
        "",
        "| Field | Value |",
        "|---|---:|",
        f"| Full-image x min | {_fmt(plot['x_min'], 0)} |",
        f"| Full-image x max | {_fmt(plot['x_max'], 0)} |",
        f"| Full-image y min | {_fmt(plot['y_min'], 0)} |",
        f"| Full-image y max | {_fmt(plot['y_max'], 0)} |",
        f"| Chart-local width | {_fmt(result['plot_bounds']['chart_local']['x_max'], 0)} |",
        f"| Detection method | `{result['plot_bounds']['detection_method']}` |",
        f"| Detection confidence | {_fmt(result['plot_bounds']['confidence'])} |",
        f"| Fallback used | {_fmt(result['plot_bounds']['fallback_used'])} |",
        "",
        "## Final interval partition",
        "",
        "| Seq. | Displayed sector | Classification | Source | Start x | End x | Start distance | End distance | Start % | End % | Confidence | Review |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for interval in result["intervals"]:
        start, end = interval["start"], interval["end"]
        lines.append(
            f"| {interval['sequence']} | {interval.get('display_label') or '—'} | {interval['classification_display']} | {interval['source']} | "
            f"{_fmt(start['full_image_x'])} | {_fmt(end['full_image_x'])} | "
            f"{_fmt(start['pixel_distance_from_plot_start'])} | {_fmt(end['pixel_distance_from_plot_start'])} | "
            f"{_fmt(start['lap_percentage'])}% | {_fmt(end['lap_percentage'])}% | "
            f"{_fmt(interval['confidence'])} | {_fmt(interval['review_required'])} |"
        )
    lines.extend(
        [
            "",
            "## Boundary evidence",
            "",
            "| Boundary | Full-image x | Chart-local x | Distance | Normalized % | Visible min | Visible max | Uncertainty |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    unique: dict[float, dict[str, Any]] = {}
    for interval in result["intervals"]:
        unique[interval["start"]["full_image_x"]] = interval["start"]
        unique[interval["end"]["full_image_x"]] = interval["end"]
    for index, boundary in enumerate(unique.values(), start=1):
        lines.append(
            f"| {index} | {_fmt(boundary['full_image_x'])} | {_fmt(boundary['chart_local_x'])} | "
            f"{_fmt(boundary['pixel_distance_from_plot_start'])} | {_fmt(boundary['lap_percentage'])}% | "
            f"{_fmt(boundary['line_x_min'], 0)} | {_fmt(boundary['line_x_max'], 0)} | {_fmt(boundary['uncertainty_px'])} |"
        )
    lines.extend(
        [
            "",
            "## OCR evidence",
            "",
            "| ID | Raw text | Normalized | Candidate | Confidence | Bounding box | Accepted | Warnings |",
            "|---|---|---|---|---:|---|---|---|",
        ]
    )
    lines.extend(
        f"| {detection['detection_id']} | {detection['text_raw']} | {detection['text_normalized']} | "
        f"{detection['classification_candidate'] or '—'} | {detection['confidence']} | "
        f"`{detection['bbox_full_image']}` | {_fmt(detection['accepted'])} | "
        f"{'; '.join(detection['warnings']) or '—'} |"
        for detection in result["evidence"]["ocr"]["detections"]
    )
    if not result["evidence"]["ocr"]["available"]:
        lines.append(
            f"| — | OCR unavailable | — | — | — | — | — | {result['evidence']['ocr'].get('error', 'unavailable')} |"
        )
    manual_overrides = result["evidence"].get("manual_label_overrides", {})
    if manual_overrides.get("applied"):
        lines.extend(
            [
                "",
                "## Manual visual-review labels",
                "",
                "The following source-specific labels were applied from visual review because OCR did not provide a reliable semantic classification. They are explicit evidence, not hidden defaults; guide-line geometry remains image-derived and authoritative.",
                "",
                "| Override | Classification | Guide span | Source |",
                "|---|---|---|---|",
            ]
        )
        lines.extend(
            f"| {item['override_id']} | {item['classification_display']} | `{item['guide_span']}` | {item['source']} |"
            for item in manual_overrides["overrides"]
        )
    lines.extend(
        [
            "",
            "## Bracket and color validation",
            "",
            "| Bracket | Guide span | Observed transition | Difference | Tolerance | Result |",
            "|---|---|---|---|---:|---|",
        ]
    )
    lines.extend(
        f"| {item['bracket_id']} | `{item['expected_span']}` | `{item['observed_background_transition'] or 'not detected'}` | "
        f"`{item['difference_px'] or '—'}` | {item['tolerance_px']} | "
        f"{'not detected' if item['within_tolerance'] is None else ('pass' if item['within_tolerance'] else 'review')} |"
        for item in result["evidence"]["color_transition_validation"]
    )
    lines.extend(["", "## Review flags", ""])
    if result["review_flags"]:
        lines.extend(
            f"- **{flag['flag_id']} — {flag['severity']} — `{flag['code']}`:** {flag['message']} (blocks completion: {flag['blocks_completion']})"
            for flag in result["review_flags"]
        )
    else:
        lines.append("No review flags.")
    lines.extend(
        [
            "",
            "## Provenance",
            "",
            f"- Source SHA-256: `{source['sha256']}`",
            f"- Extraction timestamp (volatile): `{result['provenance']['extraction_timestamp_utc']}`",
            f"- Extractor: `{result['provenance']['extractor_version']}`",
            f"- Python: `{result['provenance']['python_version']}`",
            f"- OCR: `{result['provenance']['ocr_engine']}` / `{result['provenance']['ocr_version'] or 'unavailable'}`",
            f"- OCR threshold: `{result['provenance']['configuration']['ocr_threshold']}`",
            f"- Schema: `{result['schema_version']}`",
            "",
            "## Methodology",
            "",
            "Guide-line centers are the authoritative bracket boundaries. Their visible integer spans and fractional centers are retained as evidence. Background transitions are independent validation evidence only and never replace guide-line measurements. Missing labels under an available OCR engine become `straight`; when Tesseract is unavailable, bracket classifications remain `unresolved` and the analysis is explicitly `incomplete`.",
        ]
    )
    return "\n".join(lines) + "\n"


def _font(size: int) -> Any:
    if ImageFont is None:
        return None
    try:
        return ImageFont.truetype("DejaVuSans.ttf", size)
    except OSError:
        return ImageFont.load_default()


def render_annotation(result: dict[str, Any], output_path: Path) -> None:
    """Render the audit PNG exclusively from measurements in ``result``."""
    image = result.pop("_image", None)
    if image is None:
        source_path = Path(result["source"]["path"])
        image = Image.open(source_path).convert("RGB")
    canvas = image.convert("RGBA")
    overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    plot = result["plot_bounds"]["full_image"]
    draw.rectangle(
        (plot["x_min"], plot["y_min"], plot["x_max"], plot["y_max"]),
        outline=(255, 255, 255, 240),
        width=3,
    )
    placed_labels: list[tuple[int, int, int, int]] = []
    band_top = int(result["bracket_band"]["y_min"]) + 2
    # Three stacked lanes above the speed trace keep labels readable. Adjacent
    # intervals that share a guide line (e.g. the three 10.x sections) MUST each
    # receive their own visible label, never collapse to a single "10" prefix.
    label_lanes = [band_top + 17 * index for index in range(3)]

    def _proposed_box(
        text_label: str, fnt: Any, start_x: int, end_x: int, target_y: int
    ) -> tuple[int, int, int, int]:
        text_bbox = draw.textbbox((0, 0), text_label, font=fnt)
        text_width = text_bbox[2] - text_bbox[0]
        text_height = text_bbox[3] - text_bbox[1]
        # Center the text on the interval midpoint when it fits; otherwise
        # anchor at start_x so the caller knows to pick a more compact form
        # before the caption overlaps the next interval.
        if text_width <= end_x - start_x:
            proposed_x = round((start_x + end_x - text_width) / 2)
        else:
            proposed_x = start_x
        return (
            proposed_x - 2,
            target_y - 2,
            proposed_x + text_width + 2,
            target_y + text_height + 2,
        )

    def _box_fits(box: tuple[int, int, int, int]) -> bool:
        return not any(
            box[0] < old[2] and box[2] > old[0] and box[1] < old[3] and box[3] > old[1]
            for old in placed_labels
        )

    for interval_index, interval in enumerate(result["intervals"]):
        start = round(interval["start"]["full_image_x"])
        end = round(interval["end"]["full_image_x"])
        color = CLASSIFICATION_COLORS[interval["classification"]]
        draw.rectangle((start, plot["y_min"], end, plot["y_max"]), fill=color)
        draw.line((start, plot["y_min"], start, plot["y_max"]), fill=(255, 255, 255, 220), width=2)
        if interval_index == len(result["intervals"]) - 1:
            draw.line((end, plot["y_min"], end, plot["y_max"]), fill=(255, 255, 255, 220), width=2)
        # Keep interval labels in the bracket band above the speed trace.
        # Drawing at plot[y_min] used to obscure the telemetry curves and made
        # the labels look like part of the trace itself.
        display_label = interval.get("display_label") or str(interval["sequence"])
        classification_display = interval["classification_display"]
        font = _font(11)
        full_label = f"{display_label} {classification_display}"
        compact_label_text = f"{display_label} {COMPACT_LABEL_LEGEND[classification_display]}"

        label_text = full_label
        active_font = font
        placed_y: int | None = None
        # Try the full label on each lane before falling back.
        for candidate_y in label_lanes:
            if _box_fits(_proposed_box(full_label, font, start, end, candidate_y)):
                placed_y = candidate_y
                break
        if placed_y is None:
            # Narrow interval cannot host a non-overlapping full label. The
            # compact form uses the same display_label (e.g. "10.1") so each
            # shared guide-line chain section remains individually labelled.
            for candidate_y in label_lanes:
                if _box_fits(_proposed_box(compact_label_text, font, start, end, candidate_y)):
                    placed_y = candidate_y
                    label_text = compact_label_text
                    break
        if placed_y is None:
            # Last-resort fallback: a small numeric pin in the smallest font
            # that fits the interval. We try every lane plus the very top of
            # the band before settling on band_top so a crowded sequence of
            # narrow intervals can still produce individually visible pins.
            font_small = _font(9)
            pin_label_text = display_label
            for candidate_y in [*label_lanes, band_top]:
                if _box_fits(_proposed_box(pin_label_text, font_small, start, end, candidate_y)):
                    placed_y = candidate_y
                    label_text = pin_label_text
                    active_font = font_small
                    break
        if placed_y is None:
            # Suppress the label entirely: every candidate lane collides with
            # an existing label and adding another pin would only obscure
            # neighbors. The JSON still carries the full classification so
            # the audit is recoverable from the table even when the PNG has
            # to drop the on-image caption.
            continue
        placed_box = _proposed_box(label_text, active_font, start, end, placed_y)
        placed_labels.append(placed_box)
        draw.rounded_rectangle(placed_box, radius=2, fill=(8, 8, 18, 200))
        draw.text(
            (placed_box[0] + 2, placed_y), label_text, fill=(255, 255, 255, 255), font=active_font
        )
    for bracket in result["evidence"]["brackets"]["brackets"]:
        span = bracket["full_image_span"]
        draw.rectangle(
            (span["x_min"], span["y_min"], span["x_max"], span["y_max"]),
            outline=(255, 245, 90, 255),
            width=2,
        )
        for guide in bracket["guide_lines"]:
            x = round(guide["boundary_x"])
            draw.line((x, span["y_min"], x, plot["y_max"]), fill=(255, 245, 90, 220), width=2)
    for detection in [
        *result["evidence"]["ocr"]["detections"],
        *result["evidence"].get("manual_label_detections", []),
    ]:
        box = detection["bbox_full_image"]
        is_manual = detection.get("engine") == "manual_visual_review"
        color = (
            (255, 220, 80, 255)
            if is_manual
            else ((255, 80, 80, 255) if not detection["accepted"] else (80, 255, 160, 255))
        )
        draw.rectangle(
            (box["x_min"], box["y_min"], box["x_max"], box["y_max"]), outline=color, width=2
        )
    legend_x, legend_y = 12, 12
    draw.rectangle(
        (legend_x, legend_y, legend_x + 360, legend_y + 24 + 18 * len(DISPLAY_CLASSIFICATIONS)),
        fill=(10, 10, 20, 210),
    )
    draw.text(
        (legend_x + 8, legend_y + 5),
        "Telemetry sector extraction audit",
        fill="white",
        font=_font(14),
    )
    for index, (classification, display) in enumerate(DISPLAY_CLASSIFICATIONS.items(), start=1):
        y = legend_y + 24 + index * 18
        draw.rectangle(
            (legend_x + 8, y, legend_x + 20, y + 12), fill=CLASSIFICATION_COLORS[classification]
        )
        draw.text((legend_x + 28, y - 2), display, fill="white", font=_font(12))
    if result["review_flags"]:
        draw.text(
            (legend_x + 205, legend_y + 30),
            f"Review flags: {len(result['review_flags'])}",
            fill=(255, 100, 100, 255),
            font=_font(12),
        )
    canvas = Image.alpha_composite(canvas, overlay).convert("RGB")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(output_path, format="PNG")


def write_outputs(
    result: dict[str, Any], config: ExtractorConfig
) -> tuple[Path, Path | None, Path | None]:
    """Write JSON, Markdown, and optional annotated PNG artifacts."""
    source_key = result["source"]["source_key"]
    config.output_dir.mkdir(parents=True, exist_ok=True)
    json_path = config.output_dir / f"{source_key}.json"
    markdown_path = config.output_dir / f"{source_key}.md"
    png_path = config.output_dir / f"{source_key}_annotated.png"
    serializable = {key: value for key, value in result.items() if key != "_image"}
    if not config.markdown_only:
        json_path.write_text(
            json.dumps(serializable, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
    else:
        json_path = None  # type: ignore[assignment]
    if config.json_only:
        markdown_path = None
    if config.annotate and not config.json_only and not config.markdown_only:
        render_annotation(result, png_path)
    else:
        png_path = None
    if markdown_path is not None:
        markdown_path.write_text(
            render_markdown(
                serializable,
                json_path.name if json_path is not None else "(not generated)",
                png_path.name if png_path is not None else "(not generated)",
            ),
            encoding="utf-8",
        )
    return json_path, markdown_path, png_path


def process_path(path: Path, config: ExtractorConfig) -> dict[str, Any]:
    """Extract one source path and write its requested artifacts."""
    result = extract_image(path, config)
    outputs = write_outputs(result, config)
    result["outputs"] = {
        "json": str(outputs[0]) if outputs[0] else None,
        "markdown": str(outputs[1]) if outputs[1] else None,
        "annotated_png": str(outputs[2]) if outputs[2] else None,
    }
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "images",
        nargs="+",
        type=Path,
        help="one or more source raster images; no directory scan is performed",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="artifact directory (default: <source parent>/analysis)",
    )
    parser.add_argument("--ocr-threshold", type=float, default=OCR_DEFAULT_THRESHOLD)
    parser.add_argument("--tesseract-cmd", default="tesseract")
    parser.add_argument(
        "--plot-bounds-fallback",
        type=parse_fallback_bounds,
        default=None,
        metavar="XMIN,XMAX,YMIN,YMAX",
    )
    parser.add_argument("--color-tolerance", type=float, default=COLOR_DEFAULT_TOLERANCE)
    parser.add_argument("--annotate", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--continue-on-error", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--json-only", action="store_true")
    parser.add_argument("--markdown-only", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.json_only and args.markdown_only:
        raise SystemExit("--json-only and --markdown-only cannot be used together")
    for source_path in args.images:
        output_dir = args.output_dir or source_path.parent / "analysis"
        config = ExtractorConfig(
            output_dir=output_dir,
            ocr_threshold=args.ocr_threshold,
            tesseract_cmd=args.tesseract_cmd,
            color_tolerance_px=args.color_tolerance,
            annotate=args.annotate,
            json_only=args.json_only,
            markdown_only=args.markdown_only,
            fallback_bounds=args.plot_bounds_fallback,
        )
        try:
            result = process_path(source_path, config)
        except ExtractionError as exc:
            print(f"ERROR {source_path}: {exc}", file=sys.stderr)
            if not args.continue_on_error:
                return 1
            continue
        print(f"{source_path}: {result['analysis_status']} -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
