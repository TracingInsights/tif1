# Phase 3 — STE Conversion Report

**Date:** 2026-09-03
**Scope:** Plan Section 7, Phase 3 (DOCS_STE100_PLAN.md). STE conversion of the guides, concepts, and reference sections, plus `architecture.mdx` and the remaining root pages: 21 files, ~67,000 words.
**Basis:** The Phase 2 pilot voice decisions (DOCS_STE100_PHASE2_PILOT.md) applied as written, under the house ruleset in `docs/style-ste100.md`.

---

## Method

The 21 pages were converted in parallel batches (the plan notes Phases 3–5 are agent-parallelizable). Each page was rewritten against the genre map:

| Section | Files | Genre | Sentence cap |
| :--- | :--- | :--- | :--- |
| guides/ (4) | best-practices, common-use-cases, data-visualization, error-handling | Instructional | 20 words |
| concepts/ (6) | backends, caching-strategy, cli, data-flow, jupyter, sessions | Descriptive | 25 words |
| reference/ (7) | cli-commands, contributing, data-reference, data-schema, environment-variables, faq, troubleshooting | Descriptive | 25 words |
| architecture.mdx | 1 | Descriptive | 25 words |
| Root pages (3) | why-tif1, examples, migration-from-fastf1 | Landing (D1) | 25 words |

Code blocks, CLI output, URLs, frontmatter `keywords`, and proper nouns were exempt per the house guide. No fenced code block was modified; every page's fences were verified byte-identical to the Phase 2 tip with the new `tools/check_code_blocks.py` checker. All Phase 1 factual corrections were preserved unchanged.

## Results

| Page | Violations before → after | Warnings before → after | Max sentence before → after |
| :--- | :--- | :--- | :--- |
| guides/best-practices.mdx | 5 → 0 | 0 → 0 | 19 → 12 words |
| guides/common-use-cases.mdx | 0 → 0 | 0 → 0 | 12 → 12 words |
| guides/data-visualization.mdx | 11 → 0 | 0 → 0 | 56 → 20 words |
| guides/error-handling.mdx | 8 → 0 | 0 → 0 | 14 → 13 words |
| concepts/backends.mdx | 86 → 0 | 8 → 0 | 39 → 20 words |
| concepts/caching-strategy.mdx | 27 → 0 | 3 → 0 | 34 → 19 words |
| concepts/cli.mdx | 14 → 0 | 0 → 0 | 27 → 16 words |
| concepts/data-flow.mdx | 40 → 0 | 7 → 0 | 40 → 20 words |
| concepts/jupyter.mdx | 60 → 0 | 5 → 0 | 29 → 20 words |
| concepts/sessions.mdx | 40 → 0 | 6 → 0 | 39 → 20 words |
| reference/cli-commands.mdx | 2 → 0 | 0 → 0 | 36 → 12 words |
| reference/contributing.mdx | 15 → 0 | 0 → 0 | 30 → 17 words |
| reference/data-reference.mdx | 6 → 0 | 3 → 0 | 34 → 19 words |
| reference/data-schema.mdx | 1 → 0 | 0 → 0 | 19 → 15 words |
| reference/environment-variables.mdx | 9 → 0 | 1 → 0 | 14 → 15 words |
| reference/faq.mdx | 17 → 0 | 0 → 0 | 27 → 19 words |
| reference/troubleshooting.mdx | 12 → 0 | 0 → 0 | 19 → 20 words |
| architecture.mdx | 0 → 0 | 1 → 0 | 24 → 19 words |
| why-tif1.mdx | 34 → 0 | 7 → 0 | 39 → 21 words |
| examples.mdx | 0 → 0 | 0 → 0 | 15 → 15 words |
| migration-from-fastf1.mdx | 5 → 0 | 0 → 0 | 27 → 17 words |
| **Total** | **392 → 0** | **41 → 0** | — |

`examples.mdx` needed no changes: its prose is headings only and was already conformant.

Whole-tree checker state after Phase 3: **908 violations, 264 warnings** (from 1,300 / 305). All remaining violations are in the `api-reference/` (Phase 5) and `tutorials/` (Phase 4) sections. Baseline gate: ok (908 ≤ 1,343).

## What changed

The Phase 2 pilot decisions were applied as written (second person removed, imperatives, D1 marketing removal, idiom removal, meaning-based sentence splits, card-body periods, lead-in colon handling, capitalized headings, frontmatter descriptions under 160 characters, single-line JSX tags, BOM removal). Phase 3 added these mechanical decisions:

1. **The UTF-8 BOM was removed from four more files**: `concepts/backends.mdx`, `reference/faq.mdx`, `reference/troubleshooting.mdx`, `architecture.mdx`. Same reason as pilot decision 11: the BOM defeats the checker's frontmatter parser.
2. **New tool `tools/check_code_blocks.py`**: extracts every fenced block from a file and from a git ref and compares them byte-for-byte. It proves the docs-only constraint mechanically and is reused by Phases 4–5. All 21 pages pass with every fence identical to the Phase 2 tip.
3. **Line endings**: each file keeps its dominant ending as found (CRLF files stay CRLF; `data-flow.mdx`, `architecture.mdx`, `why-tif1.mdx` stay LF). Pre-existing mixed-ending lines inside files were left as found; no added line introduces a new minority ending.
4. **`e.g.` in table cells stays**: reference tables are exempt identifier/reference data (S9); only prose occurrences were replaced with "for example" or "such as".
5. **Possessives of product names restructured** where natural (`pandas' rich ecosystem` → `the pandas ecosystem`, `tif1's core architecture` → `the tif1 core architecture`), extending pilot decision 10.

## Verification

```bash
# per-page gate: all 21 pages report 0 violations, 0 warnings
python3 tools/ste_check.py docs/guides/<page>.mdx ...   # 0 violations, 0 warnings each

# fence integrity: every fenced block byte-identical to the Phase 2 tip
python3 tools/check_code_blocks.py docs/guides/ docs/concepts/ docs/reference/ \
  docs/architecture.mdx docs/why-tif1.mdx docs/examples.mdx docs/migration-from-fastf1.mdx  # PASS (21 files)

# whole-tree regression gate
python3 tools/ste_check.py docs/ --baseline tools/ste_baseline.json  # ok (908 <= 1343)

# code examples: re-run on the 15 non-concept pages, failure set identical to the
# pre-conversion baseline (30 pre-existing, environment-dependent failures:
# optional deps pytest/plotly/pyarrow/streamlit/fastf1 not installed, snippet
# fragments without imports, async/CLI snippets) — code blocks are byte-identical,
# so example behavior is unchanged
uv run python tools/run_doc_examples.py <15 non-concept pages> --timeout 420

# build and links
cd docs && npx -y mintlify@latest validate      # passed
cd docs && npx -y mintlify@latest broken-links # no broken links
```

Frontmatter `keywords` are unchanged in all 21 files (verified by diff). All descriptions are 160 characters or fewer (longest: 151).

## Findings for a correctness follow-up (noticed during conversion, not fixed)

Phase 3 is a voice rewrite; these factual issues are recorded for the owner. None were changed because each sits inside exempt content or would alter Phase 1-audited claims:

1. **data-flow.mdx** says the CDN fallback chain ends at GitHub in one prose summary bullet and in fenced diagrams ("GitHub fallback"), contradicting the architecture (Hugging Face buckets last; raw.githubusercontent.com is forbidden). The fenced text cannot change under the byte-identical rule.
2. **data-flow.mdx** shows `force_refresh=True` in a fenced example whose own comment says it is "not currently implemented, but planned".
3. **Concept-page code blocks have many pre-existing execution failures** (~80 across backends/data-flow alone): fragment snippets without imports, optional dependencies (pyarrow, sklearn), and `get_session(backend=...)` calls that raise `TypeError` against the current API. Phase 1's example-execution pass covered get-started/guides pages only; a concept-page pass is outstanding.
4. **backends.mdx** Related Pages card bodies do not match their card titles ("Common Use Cases" → "Performance guide").
5. **Version claims vary across pages**: polars "requires Python 3.8+" (repo requires Python 3.11+), `polars>=1.40.1` in one install line versus polars 1.43.2 / pandas 3.0.5 in benchmark notes.
6. **Sampling-rate claims disagree**: telemetry "10-60Hz" (backends) versus "100-300 Hz" (sessions); weather "1-minute samples" versus "every 1-2 minutes".
7. **caching-strategy.mdx**: the "Deploy to Production" card links to the same page; a Docker example uses `chmod 777` while the Security section recommends 750/700; example code calls APIs that need source verification (`load_async`, `get_driver_telemetry`, `smart_cleanup`, `cache_fallback_dir`, `get_next_event`).
8. **jupyter.mdx** keeps an uncited claim that polars is faster in interactive sessions (D5 evidence needed).
9. **faq.mdx** keeps "Subsequent loads are instant from the cache" — an intensity claim that survived because rewording it would alter a Phase 1-audited statement.

## Open items

1. Official ASD-STE100 Issue 9 copy still pending (carried from Phase 0); rule IDs cannot be pinned until it arrives.
2. Owner calibration review of the Phase 2 pilot pages remains open; this phase applied those decisions as written.
3. Phase 4 (tutorials) and Phase 5 (api-reference prose) remain; the D7 non-navigation files (`docs/README.md`, `docs/VERSIONING.md`, `docs/design_language.md`) are scheduled with the final QA phase.
