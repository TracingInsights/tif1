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
    query = query.casefold().replace(" ", "")
    for i in range(len(reference)):
        for j in range(len(reference[i])):
            reference[i][j] = reference[i][j].casefold().replace(" ", "")

    # Check for exact substring matches first.
    # If exactly one reference has the query as a substring, return it as accurate.
    full_partial_match_indices = []
    for i, feature_strings in enumerate(reference):
        if any(query in val for val in feature_strings):
            full_partial_match_indices.append(i)

    if len(full_partial_match_indices) == 1:
        return full_partial_match_indices[0], True

    # Zero or multiple substring matches — use fuzzy matching
    reference_arr = np.array(reference)
    ratios = np.zeros_like(reference_arr, dtype=int)

    if full_partial_match_indices:
        candidate_indices = full_partial_match_indices
    else:
        candidate_indices = range(len(reference_arr))

    for i in candidate_indices:
        feature_strings = reference_arr[i]
        ratios[i] = [fuzz.ratio(val, query) for val in feature_strings]

    max_ratio = np.max(ratios)
    max_row_ratios = np.max(ratios, axis=1)

    # If multiple rows share the max ratio, disambiguate via remaining features
    if np.sum(max_row_ratios == max_ratio) > 1:
        unique, counts = np.unique(reference_arr, return_counts=True)
        count_dict = dict(zip(unique, counts))
        mask = (np.vectorize(count_dict.get)(reference_arr) > 1) & (ratios == max_ratio)
        ratios[mask] = 0

    max_index = np.argmax(ratios) // ratios.shape[1]
    return int(max_index), False
