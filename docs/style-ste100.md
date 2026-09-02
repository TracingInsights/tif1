---
title: "STE100 House Style Guide"
description: "How tif1 documentation is written: a Simplified Technical English ruleset adapted from ASD-STE100 Issue 9."
---

# tif1 STE100 House Style Guide

This guide defines how all documentation under `docs/` is written. It implements the
ASD-STE100 Simplified Technical English (Issue 9, 2025-01-15) approach, adapted for a
Python library reference.

ASD-STE100 and its dictionary are copyrighted by ASD, Brussels. This guide is a
paraphrase produced for internal use. It never reproduces rule or dictionary text.
Request the free official copy at <https://www.asd-ste100.org/STE_downloads.html>.

## 1. Scope

This guide governs every `.mdx` and `.md` file under `docs/`, including the three
non-navigation files (`README.md`, `VERSIONING.md`, `design_language.md`).

The following text is exempt and is never rewritten:

- Fenced code blocks and inline code.
- CLI output and exception messages shown verbatim.
- URLs, file paths, and frontmatter YAML keys.
- Proper nouns: drivers, teams, circuits, companies, CDN names.
- Signatures and identifier names in parameter tables.

Frontmatter `title`, `description`, and `sidebarTitle` values are prose. They follow
this guide. `keywords` values stay unchanged to preserve search indexing.

## 2. Genre map

| Page type | Pages | Rule set |
| :--- | :--- | :--- |
| Instructional | installation, quickstart, getting-started, guides, tutorials | Procedural rules (Section 5): imperative steps, max 20 words per sentence |
| Descriptive | concepts, reference, architecture, api-reference prose | Descriptive rules: present tense, max 25 words per sentence |
| Landing | introduction, why-tif1, examples, migration-from-fastf1 | Descriptive rules, factual statements only (decision D1) |

## 3. Vocabulary rules

- **V1** Use each approved word only as one part of speech.
- **V2** Use each word with one meaning only. Do not alternate synonyms for the same
  concept. Pick one word per concept and use it on every page.
- **V3** Use a word from the Technical Names register (Section 6) only as a noun.
  Never use a technical name as a verb.
- **V4** Do not use jargon, slang, idioms, or figurative language.
- **V5** Do not use marketing or intensity words. State measurable facts instead.
- **V6** Do not use contractions. Write the full form (`do not`, `cannot`).
- **V7** Use the shortest approved form that keeps the meaning.
- **V8** Do not make noun clusters longer than three consecutive nouns.
- **V9** Keep articles (`a`, `an`, `the`). Do not omit them to save space.
- **V10** Address the reader with imperative sentences. Do not use `you` or `we`.
  When an actor is needed, use `tif1` as the subject (`tif1 recommends`).
- **V11** Replace every word in the substitution table (Section 4) with its approved
  form.

## 4. Substitution table

| Non-approved | Approved form |
| :--- | :--- |
| utilize, utilise, employ, leverage | use |
| in order to | to |
| attempt (prose only) | try |
| prior to, subsequent to | before, after |
| facilitates | enables |
| however, thus, therefore, hence, thereby | start a new sentence; use `but` where approved |
| whilst, notwithstanding | while, without |
| "you can X" | imperative: "Do X" |
| "it is recommended that" | "tif1 recommends", or an imperative |
| currently, at this point in time | now, or delete |
| and/or | restructure into separate items |
| as, since (cause or time unclear) | `because` for cause, `when` for time |

## 5. Sentence and structure rules

- **S1** Procedural sentence: maximum 20 words. Descriptive sentence: maximum 25
  words. Split longer sentences.
- **S2** One instruction or one topic per sentence.
- **S3** Start each instructional step with an imperative verb, present simple,
  active voice.
- **S4** Use active voice. Use passive voice only in descriptive text where the actor
  is unknown or not relevant.
- **S5** Use simple tenses. Do not use `will` or `shall` to describe what a procedure
  does.
- **S6** Start warnings and cautions with `Warning:` or `Caution:`. Give one topic
  per warning. Put each warning before the step it qualifies.
- **S7** Give one action per numbered step. Keep steps in execution order.
- **S8** Keep vertical lists short and parallel. Introduce each list with a lead-in
  sentence.
- **S9** Prefer tables for parameter and reference data. Identifier cells are exempt.
- **S10** Do not use ambiguous conjunctions (see V11 last row).

## 6. Technical Names register

Technical names are approved as nouns only.

- **tif1 API:** Session, Laps, Lap, Telemetry, Driver, SessionResults, DriverResult,
  CircuitInfo, LazyTelemetryDict, PayloadLoader, SessionMemo, CDNManager, Config,
  Cache, LRU cache, SQLite cache, cache tier, memo, fast telemetry, mini-sector,
  backend (pandas, polars)
- **Python:** module, package, class, function, method, argument, keyword argument,
  return value, type hint, decorator, exception, traceback, iterator, context
  manager, virtual environment, dependency, DataFrame, Series
- **Formula 1:** telemetry, sector, stint, DRS, downforce, compound, tire
  degradation, lap delta, speed trace, track map, qualifying, race control messages,
  pole position
- **Infrastructure:** CDN, StaticDelivr, jsDelivr, Hugging Face bucket, circuit
  breaker, retry, backoff, jitter, HTTP/2, multiplexing, cache hit, cache miss, hot
  cache

## 7. Frontmatter

- **F1** Write `title`, `description`, and `sidebarTitle` to this guide.
- **F2** Do not change `keywords`.
- **F3** Give every page a `description` of 160 characters or fewer.

## 8. Decisions and deviations register

Owner decisions recorded 2026-09-02:

| ID | Decision |
| :--- | :--- |
| D1 | Strict STE everywhere. Promotional language is removed from all pages, including card titles on `why-tif1` and `introduction`. Comparisons use facts and cited sources. |
| D2 | Add API-reference pages for the undocumented modules: `payload_loader`, `session`, `schedule_schema`, `plotting_constants`, `assets`. |
| D3 | Keep `utils`, `utilities`, `core-utils`, `http`, `http-session` pages separate. Fix their content only. |
| D4 | Rewrite documentation only. Do not change source code or docstrings. |
| D5 | Re-run the benchmark suite to regenerate performance claims with evidence. |
| D6 | Publish the work as a stacked chain of pull requests, one per phase or section. |
| D7 | Include `README.md`, `VERSIONING.md`, `design_language.md` in the rewrite. |

Deviations from ASD-STE100 (each entry records a deliberate, bounded departure):

| ID | Deviation | Reason |
| :--- | :--- | :--- |
| DEV-1 | This guide paraphrases the standard instead of quoting it | ASD copyright; rule IDs are pinned when the official Issue 9 copy arrives |
| DEV-2 | `tif1` is used as a sentence subject | The library is the actor in most sentences; STE has no approved equivalent for a software product name in this role |
| DEV-3 | Tutorial chart names (`gg diagram`, `speed trace`) stay as compound technical names | They are the public names of the `plot_*` functions |

## 9. Checker

`tools/ste_check.py` enforces the rules that a machine can test: contractions,
banned words, marketing terms, sentence length, `and/or`, and second-person text.
It skips code blocks, tables, and inline code. The checker is a heuristic gate.
Human review against this guide is the authority. Run it:

```bash
python3 tools/ste_check.py docs/            # summary report
python3 tools/ste_check.py docs/ --strict   # non-zero exit on any violation
python3 tools/ste_check.py docs/ --json     # machine-readable report
```
