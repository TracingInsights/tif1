# Phase 4 — STE Conversion Report

**Date:** 2026-09-03
**Scope:** Plan Section 7, Phase 4 (DOCS_STE100_PLAN.md). STE conversion of all 28 pages in `docs/tutorials/`, ~19,000 words. One exemplar tutorial (`tire-degradation.mdx`) was converted first; the established pattern was then applied to the remaining 27.
**Basis:** The Phase 2 pilot voice decisions (DOCS_STE100_PHASE2_PILOT.md) and Phase 3 mechanical decisions (DOCS_STE100_PHASE3_REPORT.md) applied as written, under the house ruleset in `docs/style-ste100.md`.

---

## Method

Tutorials are instructional pages: procedural rules apply, so every prose sentence is capped at 20 words and instructional text uses imperatives. Code blocks, CLI output, URLs, frontmatter `keywords`, and proper nouns were exempt per the house guide. No fenced code block was modified; every page's fences were verified byte-identical to the Phase 3 tip with `tools/check_code_blocks.py`.

The exemplar was converted and gate-checked first, then the remaining 27 pages were converted in six parallel batches, each batch verified against the same gates before completion.

## Results

| Page | Violations before → after | Warnings before → after | Max sentence before → after |
| :--- | :--- | :--- | :--- |
| advanced-telemetry.mdx | 2 → 0 | 0 → 0 | 17 → 15 words |
| annotated-speed-trace.mdx | 4 → 0 | 1 → 0 | 28 → 19 words |
| data-export.mdx | 4 → 0 | 0 → 0 | 20 → 16 words |
| downforce-levels.mdx | 6 → 0 | 3 → 0 | 24 → 19 words |
| driver-laptimes.mdx | 3 → 0 | 3 → 0 | 35 → 16 words |
| gear-shifts-on-track.mdx | 2 → 0 | 0 → 0 | 20 → 19 words |
| gg-diagram.mdx | 3 → 0 | 3 → 0 | 23 → 18 words |
| lap-delta-comparison.mdx | 2 → 0 | 3 → 0 | 26 → 18 words |
| laptime-heatmap.mdx | 4 → 0 | 1 → 0 | 22 → 19 words |
| laptimes-distribution.mdx | 2 → 0 | 2 → 0 | 30 → 19 words |
| multi-driver-speed-comparison.mdx | 2 → 0 | 2 → 0 | 30 → 15 words |
| position-changes.mdx | 8 → 0 | 0 → 0 | 20 → 19 words |
| qualifying-analysis.mdx | 2 → 0 | 1 → 0 | 21 → 15 words |
| qualifying-results.mdx | 5 → 0 | 3 → 0 | 29 → 15 words |
| race-analysis.mdx | 1 → 0 | 4 → 0 | 25 → 17 words |
| race-pace-analysis.mdx | 8 → 0 | 1 → 0 | 23 → 15 words |
| speed-traces.mdx | 4 → 0 | 0 → 0 | 26 → 18 words |
| telemetry-comparison.mdx | 3 → 0 | 2 → 0 | 24 → 16 words |
| throttle-distance.mdx | 4 → 0 | 0 → 0 | 20 → 19 words |
| tire-degradation.mdx | 8 → 0 | 2 → 0 | 22 → 18 words |
| tire-strategy.mdx | 6 → 0 | 0 → 0 | 28 → 17 words |
| top-speeds.mdx | 6 → 0 | 1 → 0 | 24 → 17 words |
| track-acceleration-map.mdx | 0 → 0 | 0 → 0 | 17 → 17 words |
| track-brake-zones.mdx | 0 → 0 | 0 → 0 | 19 → 18 words |
| track-speed-map.mdx | 6 → 0 | 1 → 0 | 23 → 14 words |
| track-temperature.mdx | 4 → 0 | 1 → 0 | 26 → 15 words |
| track-throttle-map.mdx | 0 → 0 | 0 → 0 | 19 → 16 words |
| weather-impact.mdx | 2 → 0 | 0 → 0 | 17 → 13 words |
| **Total (28 pages)** | **101 → 0** | **34 → 0** | — |

Three pages (track-acceleration-map, track-brake-zones, track-throttle-map) reported zero checker violations before conversion; their prose was still reviewed against the house patterns, and conformance edits were made where patterns applied (idiom removal, `<img>` collapse, heading Title Case).

Section totals after conversion: 935 sentences, 7,799 prose words, average 8.3 words per sentence, maximum sentence 19 words.

Whole-tree checker state after Phase 4: **807 violations, 230 warnings** (from 908 / 264). All remaining violations are in `api-reference/` (Phase 5). Baseline gate: ok (807 ≤ 1,343).

## What changed

The Phase 2 and Phase 3 decisions were applied as written (second person removed, contractions removed, D1 marketing and intensity removal, idiom removal, meaning-based sentence splits, card-body periods, capitalized headings, frontmatter descriptions rewritten to imperatives under 160 characters, single-line JSX tags). Phase 4 added these tutorial-specific decisions:

1. **Frontmatter descriptions converted from gerunds to imperatives** ("Overlaying speed traces..." → "Overlay speed traces..."), matching the pilot decision for instructional pages.
2. **Multi-line `<img>` tags collapsed to single lines** in every tutorial that had them, removing the false long sentences the checker previously reported from unparsed tag markup.
3. **Sentence-case headings moved to Title Case** ("Complete example" → "Complete Example", "Loading the session" → "Loading the Session"); question headings became declarative ("What is a G-G diagram?" → "The G-G Diagram", "What is Tire Degradation?" → "Tire Degradation").
4. **Summary lead-ins standardized**: "With `tif1`, you can:" → "Use `tif1` to:" across all tutorial summaries.
5. **Slash compounds expanded in prose** ("throttle/brake" → "throttle and brake", "braking/accelerating" → "braking and accelerating"); slashes inside code, paths, and technical names were untouched.
6. **Line endings**: every prose line in all 28 files now ends CRLF. Nine files carry pre-existing bare-LF lines inside fenced code blocks at the Phase 3 tip (advanced-telemetry 11, data-export 43, laptime-heatmap 4, laptimes-distribution 32, qualifying-analysis 73, race-analysis 50, tire-strategy 14, telemetry-comparison 4, weather-impact 108). Those lines were left byte-identical per the fence rule; a mechanical check confirms zero LF lines outside fences in all 28 files.
7. **Card display text corrected where it named the wrong target**: "Telemetry Deep Dive" link text pointing at `/tutorials/telemetry-comparison` became "Telemetry Comparison" (matching the target page title); "Deep dive into qualifying data" card bodies became "A detailed examination of qualifying data."

## Verification

```bash
# per-page gate: all 28 pages report 0 violations, 0 warnings, max sentence <= 20 words
python3 tools/ste_check.py docs/tutorials/           # 0 violations, 0 warnings, max 19 words

# fence integrity: every fenced block byte-identical to the Phase 3 tip
python3 tools/check_code_blocks.py docs/tutorials/ HEAD   # 28 files, all blocks identical, PASS

# frontmatter invariants: keywords and titles unchanged (verified by diff); all
# descriptions <= 160 characters (longest run reported none over the limit)

# code examples: full 28-page baseline run before conversion
uv run python tools/run_doc_examples.py docs/tutorials/ --json --timeout 420
# 223 blocks: 213 ok, 10 pre-existing failures, all environment-dependent:
#   data-export.mdx 6 (pyarrow/openpyxl/sqlalchemy/pytables not installed)
#   race-pace-analysis.mdx 3 (pyarrow + two cascade NameErrors)
#   weather-impact.mdx 1 (sklearn not installed)
# the three failing pages were re-run after conversion: identical failure set

# line endings: zero bare-LF lines outside code fences in all 28 files

# build and links
cd docs && npx -y mintlify@latest validate       # build validation passed
cd docs && npx -y mintlify@latest broken-links   # no broken links

# whole-tree regression gate
python3 tools/ste_check.py docs/ --baseline tools/ste_baseline.json   # ok (807 <= 1343)
```

No source code, tests, or docstrings were touched. `uv.lock` churn from the sandbox environment was reverted; the branch contains only the 28 tutorial pages, this report, and the plan progress-log entry.

## Findings for a correctness follow-up (noticed during conversion, not fixed)

Phase 4 is a voice rewrite; these factual items are recorded for the owner. None were changed because each sits inside exempt content or would alter Phase 1-audited claims:

1. **Optional-dependency examples fail in a bare environment**: pyarrow, openpyxl, sqlalchemy, pytables, and sklearn are not project dependencies, but six data-export blocks, one race-pace-analysis block, and one weather-impact block require them. This extends Phase 3 finding 3 (concept-page code failures) to the tutorials tab.
2. **race-pace-analysis.mdx blocks 2 and 3 depend on block 1's parquet round-trip.** When pyarrow is absent, block 1 fails and blocks 2–3 raise `NameError` (`clean_laps`, `comparison_df` undefined). The page's core flow depends on an optional dependency.
3. **Tutorial prose still contains factual claims not re-verified this phase** (fuel-effect estimate ~0.03 s/lap in tire-degradation, cliff threshold 0.15 s, degradation-rate bands 0.00–0.05 / 0.05–0.10 / >0.10 s/lap). They were preserved exactly; a domain review can validate them.
4. **Card display text drift** (fixed here as a voice edit): several pages linked to `/tutorials/telemetry-comparison` with the display text "Telemetry Deep Dive". A link-text audit across the remaining sections is outstanding.

## Open items

1. Official ASD-STE100 Issue 9 copy still pending (carried from Phase 0); rule IDs cannot be pinned until it arrives.
2. Owner calibration review of the Phase 2 pilot pages remains open (carried from Phase 2); this phase applied those decisions as written.
3. Phase 5 (api-reference prose) and the final QA phase (including the D7 non-navigation files `docs/README.md`, `docs/VERSIONING.md`, `docs/design_language.md`) remain.
4. The whole-tree baseline (`tools/ste_baseline.json`, 1,343) can be regenerated after Phase 5 to tighten the CI regression gate.
