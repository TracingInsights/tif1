# Plan: Convert tif1 Mintlify Documentation to ASD-STE100 and Verify Correctness

**Status:** Draft for owner approval — no documentation files have been changed yet.
**Reference standard:** ASD-STE100 Simplified Technical English, Issue 9 (2025-01-15). Issue 9 fully replaces all earlier issues. It contains 53 writing rules (31 reworded in Issue 9) and an updated dictionary (555 entries revised, terminology aligned with ISO 1087-1:2019).
**Standard owner:** ASD, Brussels. Free official copy via request form at <https://www.asd-ste100.org/STE_downloads.html>.

---

## 1. Objective

1. Rewrite all documentation under `docs/` in ASD-STE100-conformant Simplified Technical English, adapted to a Python library documentation context.
2. Verify that the documentation is factually correct and current against `src/tif1` at version 0.6.0 and the CHANGELOG, and fix every drift found.

## 2. Scope inventory

81 content files, ~276,000 words total.

| Section | Files | Words (approx.) | Notes |
| :--- | :--- | :--- | :--- |
| Get Started (root pages) | 7 | ~7,200 | introduction, why-tif1, installation, getting-started, quickstart, examples, migration-from-fastf1 |
| `guides/` | 4 | ~5,200 | best-practices, error-handling, data-visualization, common-use-cases |
| `concepts/` | 6 | ~41,000 | data-flow (14.9k) and caching-strategy (13.0k) are the largest |
| `reference/` + architecture | 8 + 1 | ~10,300 | data-reference is 3.8k words |
| `tutorials/` | 28 | ~19,000 | Highly templated; each demonstrates one chart function |
| `api-reference/` | 26 | ~190,000 | Mostly structured reference: signatures, parameter tables, prose notes |
| Non-navigation files | 3 | ~1,750 | `docs/README.md`, `docs/VERSIONING.md`, `docs/design_language.md` — rewrite or consciously exclude (decision D7) |

**Out of scope:** `docs.json` (edited only for correctness in Phase 1), `style.css`, logo and image assets, image-generation scripts in `docs/assets/`, all source code and docstrings (decision D4).

**Text exempted from STE conversion inside every file:** fenced code blocks and inline code, CLI output shown verbatim, exception/message strings quoted as literals, URLs and file paths, frontmatter YAML keys (values are rewritten), proper nouns (drivers, teams, circuits, CDN names), and identifier names.

## 3. Standards and legal basis

- ASD-STE100 is a controlled natural language: a restricted vocabulary (approved words, each with one approved part of speech and one meaning) plus 53 writing rules covering procedures, sentences, and descriptive writing.
- The specification and its dictionary are copyrighted. The rules and dictionary **must not be reproduced** in this repository. This plan therefore produces a paraphrased house style guide that implements the STE approach; exact rule identifiers are pinned during Phase 0 from the officially requested copy.
- Per the STEMG White Paper on AI and STE (2025): AI-generated text can look STE-conformant without being so. Automated checks are heuristics; the authority is human review against the official Issue 9 copy.
- STE was designed for aerospace maintenance documentation. Software library documentation has different genres, so Section 4 defines a documented adaptation: STE vocabulary discipline and sentence rules, plus a project Technical Names register, with procedural rules applied to instructional pages and descriptive rules to concept/reference prose.

## 4. House style ruleset (STE adaptation for tif1)

### 4.1 Vocabulary rules

- Use each approved word only as the part of speech and only with the meaning it is approved for.
- One word, one meaning: no synonym rotation. Standardize on one word per concept across all pages.
- Maintain a Technical Names register (STE permits technical names for the subject field): tif1 API identifiers, Python language terms (module, class, DataFrame, keyword argument), F1 domain nouns (telemetry, stint, sector, DRS, downforce, pole position), and infrastructure nouns (CDN, cache tier, circuit breaker). Technical names are used as nouns only, never as verbs.
- No jargon, slang, idioms, or figurative language. Current violations to remove: "a breeze", "batteries included", "Blazing Fast", "massive", "sophisticated, enterprise-grade", "the glue of the F1 data community".
- No marketing superlatives or unverifiable intensity words. Replace with measurable statements or delete.
- No contractions (`don't` → `do not`).
- Prefer the shortest approved form that keeps the meaning.

Project substitution table (starter; extended during Phase 0):

| Non-approved / current | Replace with |
| :--- | :--- |
| utilize, employ, leverage | use |
| in order to | to |
| attempt (prose only) | try |
| prior to, subsequent to | before, after |
| facilitates | enables / allows (pick one) |
| however, thus, therefore | new sentence; or `but` where approved |
| "you can X" in procedures | imperative: "X" or "Do X" |
| "it is recommended that" | imperative, or "tif1 recommends" |
| currently, at this point in time | now (or delete) |

### 4.2 Sentence rules

- Procedural sentences: maximum 20 words. Descriptive sentences: maximum 25 words. One instruction or one topic per sentence. Split longer sentences.
- Instructions and tutorial steps begin with an imperative verb (present simple, active voice).
- Active voice. Passive voice only in descriptive writing where the actor is unknown or not relevant.
- Noun clusters: maximum three consecutive nouns.
- No ambiguous conjunctions. Do not use `as` or `since` where time and cause can be confused (use `because` for cause, `when` for time). Do not use `and/or` — restructure.
- Simple tenses only. No `will`/`shall` for what a procedure does; no future-perfect or conditional compounds.
- Keep articles; do not omit `a`/`an`/`the` to save space.

### 4.3 Structural rules

- Procedural steps: one action per step, imperative start, logical order.
- Warnings and cautions: single topic, placed before the step they qualify, consistent format (Mintlify `<Note>` / `<Warning>` components).
- Vertical lists: short, parallel structure, introduced by a lead-in sentence.
- Tables: allowed and preferred for parameter/reference data; table cells that contain identifiers or signatures are exempt from sentence-length rules.

### 4.4 Frontmatter

- Rewrite every `title`, `description`, and `sidebarTitle` to STE. Descriptions are prose and are in scope. Keep `keywords` unchanged to preserve SEO.

### 4.5 Deviations register

Every place where the house ruleset intentionally departs from strict ASD-STE100 is recorded in a `docs/STYLE_DEVIATIONS.md` entry (name, reason, scope). Known candidates to decide in Phase 0:

- Second person "you": strict STE avoids it in procedures (imperatives used instead); descriptive pages may need a documented allowance or full conversion.
- Genre mismatch: concept and "why" pages are persuasive/explanatory, not maintenance procedures — record which rule families apply.

## 5. Verification phase — correctness and currency (execute BEFORE the rewrite)

Factual drift already found during reconnaissance (to fix in Phase 1):

1. **Version drift.** `docs.json` says `version: "0.4.0"`; `pyproject.toml` says `0.6.0`. The `versions` array is missing `0.5.0`, `0.5.1`, and `0.6.0`.
2. **Chart count wrong.** `api-reference/charts.mdx` and `introduction.mdx` claim **21** chart functions; `src/tif1/charts/` defines **22** `plot_*` functions. Recount, enumerate, and fix all occurrences.
3. **CDN description drift.** `introduction.mdx` describes two sources (StaticDelivr + jsDelivr). Current architecture has three tiers including the Hugging Face bucket fallback. Align all pages.
4. **Module coverage gaps.** No API-reference page exists for `payload_loader.py` (the central loading pipeline), `session.py`, `schedule_schema.py`, `plotting_constants.py`, or `assets.py`. Decision D2.
5. **Overlapping pages.** `api-reference/utils.mdx` ("Utilities API"), `api-reference/utilities.mdx` ("Utilities"), and `api-reference/core-utils.mdx` overlap; `http.mdx` vs `http-session.mdx` needs a clear split. Decision D3.

Additional verification passes:

6. **API signature sweep.** For every documented function/class: verify name, signature, parameters, defaults, and return types against source via an introspection script, then manual review of the large pages (models.mdx 12.9k words, cli.mdx 13.7k, fuzzy.mdx 14.8k, schedule.mdx 11.1k, http.mdx 10.5k, exceptions.mdx 10.1k).
7. **Code example execution.** Extract every Python code block from tutorials, guides, and get-started pages; run headless (Agg backend) with test transports where possible. Network-dependent examples are marked and smoke-checked where feasible. Verify every tutorial's `plot_*` call resolves and runs.
8. **Claims audit.** Performance table (4.8x / 28x / 20x speedups), "4-5x faster", "20-30% faster with HTTP/2", "500 requests/hour jolpica rate limit", "2018–current" data coverage: either re-benchmark and attach evidence, cite the existing source, or reword to a verifiable neutral statement. STE conversion also requires removal of unsupported superlatives, so this pass and the rewrite reinforce each other.
9. **Links, media, build.** Run `mint broken-links`, `mint validate`, and `mint a11y`. Verify every referenced image exists and matches current chart output; regenerate stale images via `docs/assets/generate_*.py`.
10. **CHANGELOG cross-check.** Confirm every user-facing change in 0.5.0, 0.5.1, 0.6.0, and Unreleased is documented (e.g., payload_loader pipeline, always-raise validation policy, HF fallback).

## 6. Tooling

- **STE checker script** (new, `tools/ste_check.py` or equivalent): parses MDX excluding code fences; flags contractions, banned-word list (Section 4.1 table plus marketing terms), prose sentences over 20/25 words, `and/or`, ambiguous `as`/`since`, and omitted-article heuristics. Produces a per-file violation report. Wired into pre-commit (`prek.toml`) and CI so regressions are blocked.
- **Signature audit script** (new or ad hoc): introspects `src/tif1` public API and compares against documented signatures; emits a diff report.
- The checker enforces the paraphrased house rules only. It cannot certify true ASD-STE100 compliance (the official dictionary is licensed), and it is not presented as doing so.
- Baseline metrics captured before Phase 2 and compared after each phase (violations per file, average sentence length).

## 7. Execution phases

| Phase | Work | Output |
| :--- | :--- | :--- |
| 0. Kickoff | Request official Issue 9 copy; pin exact rule IDs into the house guide; finalize Technical Names register, substitution table, deviations register; write and calibrate the checker; baseline metrics | `docs/style-ste100.md` (house guide), `tools/ste_check.py` |
| 1. Correctness | Execute Section 5 passes 1–10; fix all factual/config drift; decide D1–D7 | Corrected docs at current voice; audit report |
| 2. Pilot STE conversion | introduction, installation, quickstart, getting-started | 4 converted pages; owner review calibrates voice before scale-up |
| 3. Guides + Concepts + Reference | 10 guide/concept pages, 8 reference pages, architecture.mdx, remaining root pages | Converted sections |
| 4. Tutorials | Convert one exemplar tutorial; apply the established pattern to the remaining 27 | Tutorials tab converted |
| 5. API Reference | Rewrite prose sections (descriptions, notes, examples) of all 26 pages; signatures and parameter tables stay as audited in Phase 1 | API tab converted |
| 6. Final QA | Checker zero violations; mint validate / broken-links / a11y; example suite green; screenshots regenerated where charts changed; PR merge | Release |

**PR strategy:** one pull request per phase (or per section within Phase 3/5 if large), stacked or sequential against `main`. Each PR is self-verifying: it includes its checker report and validation output. api-reference phases stay mechanical to keep review load low.

## 8. Acceptance criteria

- Checker reports zero violations across all in-scope files (code fences, identifier tables, and exempted categories excluded).
- No contractions, no banned words, no marketing superlatives, no sentence over the 20/25-word limits in converted prose.
- Every documented signature matches source introspection; audit report is clean or lists accepted exceptions.
- `docs.json` version equals `pyproject.toml` version; `versions` array includes all released versions.
- Chart-function count is consistent with source everywhere it appears.
- `mint validate` and `mint broken-links` pass; `mint a11y` reports no new issues.
- All runnable examples execute without error; network-dependent examples are explicitly labeled.
- Frontmatter titles and descriptions are STE-conformant.
- Deviations register documents every intentional departure from ASD-STE100.

## 9. Risks and mitigations

| Risk | Mitigation |
| :--- | :--- |
| Volume (~276k words) makes the rewrite slow and inconsistent | Phased batches, checker automation, templates for the 28 near-identical tutorials, API tables left untouched |
| Simplification changes technical meaning | Correctness audit (Phase 1) lands before the rewrite; each page's STE edit is reviewed against the audited facts |
| Tone loss on landing pages ("why-tif1", introduction cards) — STE forbids promotional language | Decision D1: strict STE vs. STE body text with factual comparison tables; deviations register records the choice |
| Copyright: reproducing STE rules or dictionary text in the repo | House guide is a paraphrase; rule IDs referenced, never rule text copied |
| SEO impact from removing idioms and keyword-rich phrasing | `keywords` frontmatter preserved; redirects unnecessary because no pages move or get renamed |
| Regressions after conversion | Checker added to pre-commit and CI |

## 10. Decisions (resolved by owner, 2026-09-02)

- **D1 — agreed:** strict STE everywhere. Promotional language is removed from all pages, including card titles on `why-tif1` and `introduction`. Comparisons use facts and cited sources.
- **D2 — yes:** add API-reference pages for `payload_loader`, `session`, `schedule_schema`, `plotting_constants`, `assets`.
- **D3 — keep as is:** `utils` / `utilities` / `core-utils` and `http` / `http-session` pages stay separate; fix their content only.
- **D4 — docs only:** no changes to source code or docstrings.
- **D5 — rerun:** re-run the benchmark suite to regenerate performance claims with evidence.
- **D6 — stacked chain:** one pull request per phase or section, stacked.
- **D7 — include:** `README.md`, `VERSIONING.md`, `design_language.md` are in scope.

## 11. Effort estimate

Approximate working hours by phase (agent-assisted, human-reviewed):

- Phase 0: 6–10 h (guide, register, checker, calibration)
- Phase 1: 12–20 h (scripts, sweep of 26 API pages, example runs, claim audit)
- Phase 2: 4–6 h (4 pilot pages plus calibration review)
- Phase 3: 14–22 h (19 pages, includes the two 13–15k-word concept pages)
- Phase 4: 8–12 h (exemplar + 27 templated tutorials)
- Phase 5: 20–30 h (26 API pages, prose sections only)
- Phase 6: 4–6 h (final QA, regeneration, merge)

Total: roughly 68–106 working hours. Phases 3–5 are parallelizable across agents once Phase 2 fixes the voice.

---

## 12. Progress log

| Date | Entry |
| :--- | :--- |
| 2026-09-02 | Plan drafted; D1–D7 resolved by owner. |
| 2026-09-02 | **Phase 0 complete.** Deliverables: `docs/style-ste100.md` (house ruleset, Technical Names register, decisions and deviations register), `tools/ste_check.py` (heuristic checker: contractions, banned words, marketing terms, sentence length, second-person text), `tools/ste_baseline.json`. Baseline: 80 files, 12,651 sentences, avg 8.8 words/sentence, max 59 words, 1,343 violations, 304 warnings. Pending owner action: request the official ASD-STE100 Issue 9 copy to pin exact rule IDs. |
| 2026-09-02 | **Phase 1 complete.** All Section 5 passes executed; full findings in `DOCS_STE100_PHASE1_AUDIT.md`. Highlights: version/CDN/chart-count drift fixed across ~20 pages; five new API-reference pages (D2); signature sweep clean; example harness found and fixed real doc bugs (nonexistent functions, wrong attributes/columns, Timedelta formatting, deprecated matplotlib kwargs); benchmarks re-run (119/120) and performance claims reworded to benchmark-validated statements; 2,781 malformed closing fences repaired; MDX autolink build blocker fixed; `mint validate` and `broken-links` pass; STE checker wired into CI with a baseline gate. Open: concurrent-reads benchmark failure (environment-sensitive), official Issue 9 copy still pending. |
| 2026-09-02 | **Phase 2 complete.** Pilot STE conversion of the four Get Started pages (introduction, installation, quickstart, getting-started). Checker: 0 violations and 0 warnings on all four pages (from 42 violations / 8 warnings); max sentence 20 words; all 24 code examples pass; `mint validate` and `broken-links` pass; whole-tree baseline gate ok (1300 <= 1343). Voice decisions for Phases 3–5 recorded in `DOCS_STE100_PHASE2_PILOT.md`. Open: official Issue 9 copy still pending; owner calibration review gates Phase 3. |
| 2026-09-03 | **Phase 3 complete.** STE conversion of all 21 guides/concepts/reference/architecture/root pages (392 violations / 41 warnings → 0 / 0; max sentence 21 words; ~67k words). Every fenced code block verified byte-identical via new `tools/check_code_blocks.py`; keywords untouched; BOM removed from 4 more files; `mint validate` and `broken-links` pass; whole-tree gate ok (908 <= 1343). Full metrics and a 9-item factual findings list in `DOCS_STE100_PHASE3_REPORT.md`. Open: official Issue 9 copy pending; concept-page code-block correctness pass outstanding. |
| 2026-09-03 | **Phase 4 complete.** STE conversion of all 28 tutorial pages (101 violations / 34 warnings → 0 / 0; max sentence 35 → 19 words). Exemplar-first method (`tire-degradation.mdx`), then the pattern applied to the remaining 27 in verified batches. Every fenced code block byte-identical to the Phase 3 tip; keywords and titles untouched; every prose line CRLF; example suite failure set unchanged (10 pre-existing optional-dependency failures; the 3 failing pages re-run and identical); `mint validate` and `broken-links` pass; whole-tree gate ok (807 <= 1343). Full metrics and a 4-item findings list in `DOCS_STE100_PHASE4_REPORT.md`. Open: official Issue 9 copy pending; Phase 5 (api-reference prose) and final QA remain. |
| 2026-09-03 | **Phase 5 complete.** STE conversion of the prose in all 29 api-reference pages (785 violations / 229 warnings → 0 / 0; max body sentence 59 → 20 words; ~190k words). Method: scripted mechanical pass (394 contraction/banned-word/e.g. replacements, prose-only), then per-file judgment rewrites (four delegate agents on disjoint file sets plus this thread), then a ~100-instance promotional-word sweep beyond the checker list (comprehensive, intelligent, robust, powerful, intuitive, significantly, dramatically, …). 12 frontmatter descriptions rewritten (≤160 chars); titles and keywords byte-identical; BOM removed from `cli.mdx`; CardGroup bodies end with periods; no new bare-LF prose lines (byte-verified against the Phase 4 tip). Every fenced code block byte-identical to the Phase 4 tip (`tools/check_code_blocks.py` PASS), so example-suite results cannot change by construction; zero table rows changed; signatures and parameter tables untouched as audited in Phase 1. `mint validate` and `broken-links` pass; whole-tree gate ok (22 <= 1343; the 22 remaining are the D7 non-navigation files, Phase 6 scope). Full metrics and a 5-item findings list in `DOCS_STE100_PHASE5_REPORT.md`. Open: official Issue 9 copy pending; Phase 6 (D7 files, a11y, screenshots, merge, CI baseline tightening) remains. |
| 2026-09-03 | **Phase 6 complete.** Final QA on the Phase 5 tip: D7 non-navigation files converted (README 12→0, VERSIONING 10→0, design_language 1 warning→0); whole tree 0 violations / 0 warnings, `--strict` passes; CI baseline regenerated 1,343→0 so any new violation fails CI. All Section 8 acceptance criteria verified: signatures (2 documented false positives), versions array complete, chart count 22, `mint validate` / `broken-links` / `a11y` pass (85 files, no issues), 319 example blocks across 39 pages all green with the page-labeled optional engines and a live PostgreSQL (SQL block wrote 1,425 laps + 160 weather rows), 25/25 image references resolve, D4 held stack-wide so no screenshot regeneration is required. Correctness fixes: phantom `user_agent` config key removed from `config.mdx` (key audit: all other 47 documented keys verified real), unsourced 30-60 s timeout estimate dropped, fastf1 compatibility claim aligned to COMPATIBILITY.md, CLI JSON-output phrasing fixed, two mislabeled links corrected (concepts/cli.mdx, fuzzy.mdx); cdn 3 vs retry 5 thresholds verified correct (distinct components). Full evidence in `DOCS_STE100_PHASE6_REPORT.md`. Open: official Issue 9 copy pending; #42 draft promotion and stack merge remain (order: #42→#43→#44→#46+#48→#47→#49→Phase 6 PR). |

**Next step:** all six phases are complete. Owner actions remaining: promote #42 from draft, merge the stack in order (#42 → #43 → #44 → #46 with #48 → #47 → #49 → Phase 6 PR), and request the official ASD-STE100 Issue 9 copy to pin exact rule IDs into the house guide.
