# Phase 5 Report — API Reference STE Conversion

**Date:** 2026-09-03
**Scope:** Plan Section 7, Phase 5 (`DOCS_STE100_PLAN.md`). STE conversion of the prose in all 29 `docs/api-reference/` pages (~190k words of reference content): module overviews, function/class descriptions, notes, warnings, tips, examples' surrounding prose, and frontmatter `description` values. Signatures and parameter tables stay exactly as audited in Phase 1 (plan Section 7, Phase 5 row; owner decision D4 keeps the change docs-only).

## Method

1. **Mechanical pass first.** A scripted pass (`contractions`, banned words, `e.g.`/`i.e.`) applied 394 replacements to prose lines only — outside code fences, excluding table rows, with inline code and URLs masked, and `keywords`/`icon` frontmatter untouched. Violations fell from 785 to 560 with zero risk to code blocks or tables.
2. **Judgment pass per file.** Every remaining violation and warning was rewritten by hand against `docs/style-ste100.md`: second person removed (imperatives or `tif1`/`the library` as actor), sentences split on meaning boundaries to 20 words or fewer, active voice, simple present, no ambiguous `as`/`since`, review words eliminated (`however` → new sentence, `attempt` → `try`). Four delegate agents converted disjoint file sets (`retry`+`cdn`, `config`+`cli`, `http`+`events`+`overview`, `utilities`+`core-utils`+`fuzzy`+`utils`); the remaining ten files (`charts`, `cache`, `async-fetch`, `types`, `validation`, `core`, `models`, `schedule`, `plotting`, `exceptions`) plus the seven already-clean small files were converted in this thread.
3. **Promotional-word sweep beyond the checker.** The checker's marketing list is narrow. A sweep removed about 100 further intensity/promotional words from prose the checker does not flag: `comprehensive`, `intelligent`, `robust`, `beautiful`, `powerful`, `intuitive`, `extensive`, `production-ready`, `dramatically`, `significantly`, `extremely`, `highly`, `battle-tested`, `mission-critical`. Each was replaced with a factual statement or deleted; no number, default, parameter name, or behavior claim changed. `minimize`/`maximize` were kept only where they state a design purpose factually (for example, "to minimize network requests").
4. **Frontmatter.** 12 `description` values rewritten to factual, imperative or descriptive forms, all 160 characters or fewer (F3). `title` and `keywords` values are byte-identical to the Phase 4 tip in all 29 files (verified by diff). The UTF-8 BOM was removed from `cli.mdx` because it defeats the checker's frontmatter parser (Phase 2 precedent).
5. **Structure fixes required by the checker's block model.** `CardGroup` bodies without terminal punctuation merge into one run-on checker sentence; card bodies in `cache.mdx`, `models.mdx`, `schedule.mdx`, and `events.mdx` now end with periods (Phase 2 pilot decision 5).

## Results

| Metric | Before (Phase 4 tip) | After |
| :--- | :--- | :--- |
| Violations (29 pages) | 785 | **0** |
| Warnings (29 pages) | 229 | **0** |
| Max sentence (body prose) | 59 words | **20 words** |
| Sentences / avg length | 8,915 / 9.0 words | 9,308 / 8.5 words |
| Whole `docs/` tree | 807 violations | 22 (all in the D7 non-navigation files) |

Per-file before → after (violations / warnings): config 92/11, retry 89/30, cdn 60/8, cli 58/4, http 55/15, events 50/25, overview 42/15, utilities 40/4, fuzzy 34/19, exceptions 33/8, utils 28/5, core-utils 26/9, models 26/7, schedule 20/9, async-fetch 19/6, plotting 19/18, jupyter 15/6, cache 13/5, fastf1-compat 13/1, charts 11/6, core 11/4, types 11/1, validation 11/1, http-session 9/2, assets 0/2, payload-loader 0/1, plotting-constants 0/5, schedule-schema 0/2, session 0/0 — all now **0/0**. Pages that started at zero were still reviewed; conformance edits were applied where patterns matched (pilot precedent).

Diff footprint: 28 files changed, 1,071 insertions / 1,072 deletions — prose-line replacements only; no file was rewritten wholesale.

## Verification

```bash
python3 tools/ste_check.py docs/api-reference/               # 0 violations, 0 warnings, max 20-word body sentence
python3 tools/check_code_blocks.py docs/api-reference/ HEAD  # PASS: every fenced block byte-identical to the Phase 4 tip
python3 tools/ste_check.py docs/ --baseline tools/ste_baseline.json  # ok (22 <= 1343)
cd docs && npx -y mintlify@latest validate                   # build validation passed
cd docs && npx -y mintlify@latest broken-links               # no broken links
```

Additional mechanical invariants verified:

- **Code blocks:** every fenced block in all 29 files is byte-identical to the Phase 4 tip. Because no executable content changed, the example-suite results cannot differ from the Phase 4 tip by construction. (`tools/run_doc_examples.py` covers tutorials, guides, and get-started pages, not `api-reference/`; api-reference signatures were audited against source in Phase 1 via `tools/sig_audit.py` and are untouched here.)
- **Tables:** zero table rows changed (diff contains no `^[+-]|` lines).
- **Frontmatter:** `title` and `keywords` byte-identical in all 29 files; only `description` values changed, all ≤160 characters.
- **Line endings:** no new bare-LF prose lines. Files that were pure CRLF stay pure CRLF except for lines that were already bare-LF at the Phase 4 tip (preserved as found); the four pure-LF files (`charts`, `overview`, `plotting`, `utils`) stay pure LF. New bare-LF lines introduced by the edit tool were normalized back to CRLF and verified against HEAD with a byte-level diff.
- **Scope:** no source code, tests, or docstrings touched (D4); no changes outside `docs/api-reference/`, this report, and the plan progress log.

## Findings recorded for a correctness follow-up (not fixed here; D4 blocks source edits and Phase 5 does not re-audit facts)

1. `cdn.mdx` documents the CDN-source failure threshold as 3 (`_max_failures`), while `retry.mdx` documents `CircuitBreaker(threshold=5)`. These are likely different components, but a reviewer should confirm both against `src/tif1/cdn.py` and `src/tif1/retry.py`.
2. `retry.mdx` states timeouts are "often 30-60 seconds"; the claim is unsourced.
3. `config.mdx` shows a `user_agent` default example `tif1/0.2.0` while the package is at 0.6.0 — stale example value.
4. `overview.mdx` claims "full compatibility with `fastf1`" while its own migration section says "Most `fastf1` code works" — inconsistent claim strength.
5. `cli.mdx` "JSON Output (Future Enhancement)" section reads "While not now supported, you can parse table output:" — awkward phrasing left from an earlier `currently` → `now` conversion; the claim itself was preserved.

## Open items

1. Official ASD-STE100 Issue 9 copy still pending (carried from Phase 0); rule IDs cannot be pinned until it arrives.
2. Phase 6 (final QA) remains: convert the three D7 non-navigation files (`docs/README.md` 12 violations, `docs/VERSIONING.md` 10, `docs/design_language.md` 1 warning) so the whole tree reaches zero; run `mint a11y`; regenerate screenshots where charts changed; merge the stack; then tighten the CI baseline from 1343.
3. Owner calibration review of the Phase 2 pilot pages remains open (carried); this phase applied those voice decisions as written.
