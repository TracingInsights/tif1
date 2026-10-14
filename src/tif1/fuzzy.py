"""Fuzzy string matching utilities for event name resolution."""

from __future__ import annotations

import numpy as np
from rapidfuzz import fuzz


def fuzzy_matcher(
    query: str,
    reference: list[list[str]],
) -> tuple[int, bool]:
    """Match a query string to a reference list of lists of strings using fuzzy
    string matching.

    The reference is a list of sub-lists where each sub-list represents one
    element. The sub-lists contain one or multiple feature strings. The idea is
    that each element can be described by multiple feature strings. The
    function tries to find the best matching element in the reference list
    for the given query string.

    The function first checks for exact substring matches with the individual
    feature strings. If there is exactly one sub-list, where the query
    is a substring of a feature string, this index is returned as an
    "accurate match". Else, the function uses fuzzy string matching to find the
    best match in the reference list. The index of the best matching element is
    then returned as an "inaccurate match".

    Args:
        query: The query string to match.
        reference: A list of lists where each sub-list contains one or multiple
            feature strings describing an element.

    Returns:
        A tuple of (index, exact) where index is the index of the best matching
        element in the reference list and exact is a boolean indicating if the
        match is accurate or not.
    """
    index, exact, _score = _fuzzy_match(query, reference)
    return index, exact


def _fuzzy_match(query: str, reference: list[list[str]]) -> tuple[int, bool, float]:
    """:func:`fuzzy_matcher`, plus the best feature score (0-100) of the match.

    An accurate (substring) match scores 100. Callers use the score to reject
    a query that resembles no element at all.
    """
    query = query.casefold().replace(" ", "")
    for i in range(len(reference)):
        for j in range(len(reference[i])):
            reference[i][j] = reference[i][j].casefold().replace(" ", "")
    # Events with incomplete metadata give shorter feature lists; pad with ""
    # (scores 0, never a substring hit) so the 2-D ratio array stays regular
    # instead of numpy raising on a ragged list.
    width = max((len(features) for features in reference), default=0)
    if width == 0:
        return 0, False, 0.0
    for features in reference:
        features.extend([""] * (width - len(features)))

    # Check for exact substring matches first.
    # If exactly one reference has the query as a substring, return it as accurate.
    full_partial_match_indices = []
    for i, feature_strings in enumerate(reference):
        if any(val and query in val for val in feature_strings):
            full_partial_match_indices.append(i)

    if len(full_partial_match_indices) == 1:
        return full_partial_match_indices[0], True, 100.0

    # Zero or multiple substring matches — use fuzzy matching
    if full_partial_match_indices:
        candidate_indices = full_partial_match_indices
    else:
        candidate_indices = range(len(reference))

    ratios = np.zeros((len(reference), width), dtype=float)
    for i in candidate_indices:
        ratios[i] = [fuzz.ratio(val, query) if val else 0.0 for val in reference[i]]

    max_ratio = float(ratios.max())
    tied = np.flatnonzero(ratios.max(axis=1) == max_ratio)
    if len(tied) == 1:
        return int(tied[0]), False, max_ratio

    # Several elements share the best score (Bahrain GP and pre-season testing
    # both match "bahrain"). A feature that more than one tied element has
    # cannot tell them apart: score the tied elements on their other features
    # only, and never let an element outside the tie win.
    rows_with_feature: dict[str, int] = {}
    for i in tied:
        for val in set(reference[i]):
            if val:
                rows_with_feature[val] = rows_with_feature.get(val, 0) + 1
    tie_scores = [
        max(
            (
                ratio
                for val, ratio in zip(reference[i], ratios[i])
                if val and rows_with_feature[val] == 1
            ),
            default=0.0,
        )
        for i in tied
    ]
    return int(tied[int(np.argmax(tie_scores))]), False, max_ratio
