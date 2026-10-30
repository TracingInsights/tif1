# Phase 2 Pilot — STE Conversion Report

**Date:** 2026-09-02
**Scope:** Plan Section 7, Phase 2 (DOCS_STE100_PLAN.md). Pilot conversion of the four Get Started pages: `introduction.mdx`, `installation.mdx`, `quickstart.mdx`, `getting-started.mdx`.
**Purpose:** Fix the documentation voice before scale-up. The owner reviews these four pages to calibrate Phases 3–5.

---

## Method

Each page was rewritten against `docs/style-ste100.md` with the genre map applied:

| Page | Genre | Sentence cap |
| :--- | :--- | :--- |
| introduction.mdx | Landing (descriptive) | 25 words |
| installation.mdx | Instructional (procedural) | 20 words |
| quickstart.mdx | Instructional (procedural) | 20 words |
| getting-started.mdx | Instructional (procedural) | 20 words |

Code blocks, CLI output, URLs, frontmatter `keywords`, and proper nouns were exempt per the house guide. No code block was modified; all factual content fixed in Phase 1 was preserved unchanged.

## Results

| Page | Violations before | Violations after | Warnings before | Warnings after | Max sentence before | Max sentence after |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| introduction.mdx | 17 | 0 | 1 | 0 | 50 words | 19 words |
| installation.mdx | 9 | 0 | 2 | 0 | 23 words | 19 words |
| quickstart.mdx | 6 | 0 | 2 | 0 | 22 words | 14 words |
| getting-started.mdx | 10 | 0 | 3 | 0 | 22 words | 20 words |
| **Total** | **42** | **0** | **8** | **0** | — | — |

All four pages pass `tools/ste_check.py --strict` with zero violations and zero review warnings.

## What changed (voice decisions for Phases 3–5)

These patterns are the calibration output. Later phases apply them as written:

1. **Second person removed.** "You specify the year" becomes "Specify the year." "Your home directory" becomes "the home directory." Imperatives carry the instruction; `tif1` is the actor in descriptive sentences (DEV-2).
2. **Marketing language removed (D1).** Card title "Blazing Fast" became "Fast"; "Batteries included" became "Charts included"; "Advanced Caching" became "Multi-tier Caching"; "the glue of the F1 data community" became "an established library at the center of the F1 data community"; "the full power of the tif1 API" became "the complete tif1 API"; "making migration a breeze" became "Migration needs few code changes."
3. **Idioms removed (V4).** "Drill down into data" became "Examine the data"; "deep dive into telemetry" became "Examine Telemetry"; "Pro Tip: Speed it up!" became "Speed Up the First Load".
4. **Long sentences split on meaning, not just the limit.** The 44-word benchmark-results sentence was split at its three semicolon-separated claims; each claim keeps its number and qualifier.
5. **Card bodies end with terminal punctuation.** Card groups form single checker blocks when no blank line separates the cards. A body without a period merges every card in the group into one run-on sentence. Short nav-card bodies now end with a period.
6. **Lead-in colons become periods when a component follows.** "Add the `polars` extra for the Polars backend:" inside `<Steps>` joins with the next step title because the colon does not end a sentence. A period keeps the blocks separate. A colon is kept only when the list follows immediately in the same block.
7. **Headings start with imperative verbs, capitalized.** "3. analyze a specific driver" became "3. Analyze a Specific Driver"; "Your first analysis" became "The First Analysis".
8. **Frontmatter descriptions rewritten to imperatives.** "How to install and configure tif1" became "Install and configure tif1". `keywords` untouched (F2); all descriptions are under 160 characters (F3).
9. **Multi-line JSX tags collapsed to one line.** The two-line `<img>` logo tags defeated the checker's tag stripping and produced a false 50-word sentence. Single-line tags with the alt text "tif1 logo" check correctly.
10. **Possessives of proper nouns kept.** "FastF1's platform layout" was restructured to "the FastF1 platform layout" where natural; `tif1`'s possessive uses "the `tif1` ..." phrasing.
11. **File encodings normalized where they broke tooling.** CRLF line endings were preserved as found. The UTF-8 BOM was removed from `installation.mdx` and `getting-started.mdx` because it defeats the checker's frontmatter parser and turns frontmatter into body prose.

## Verification

```bash
python3 tools/ste_check.py docs/introduction.mdx docs/installation.mdx docs/quickstart.mdx docs/getting-started.mdx  # 0 violations, 0 warnings each
python3 tools/ste_check.py docs/ --baseline tools/ste_baseline.json  # ok (1300 <= 1343)
uv run python tools/run_doc_examples.py docs/introduction.mdx docs/installation.mdx docs/quickstart.mdx docs/getting-started.mdx --timeout 420  # 24 blocks, 0 failures
cd docs && npx -y mintlify@latest validate       # build validation passed
cd docs && npx -y mintlify@latest broken-links   # no broken links
```

Whole-tree checker state after Phase 2: 1,300 violations remaining (baseline 1,343), all in pages scheduled for Phases 3–5.

## Open items

1. The official ASD-STE100 Issue 9 copy is still pending (carried from Phase 0). Rule IDs cannot be pinned into the house guide until it arrives. The pilot follows the paraphrased house ruleset.
2. Owner calibration review of these four pages gates Phase 3.
