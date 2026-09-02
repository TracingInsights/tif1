#!/usr/bin/env python3
"""STE conformance checker for tif1 documentation.

Enforces the machine-testable rules from docs/style-ste100.md: contractions,
banned words, marketing terms, sentence length, and second-person text.
Fenced code blocks, tables, inline code, and JSX tags are exempt.

This is a heuristic gate, not an ASD-STE100 compliance certificate. Human
review against the house guide is the authority.

Usage:
    python3 tools/ste_check.py docs/
    python3 tools/ste_check.py docs/ --strict
    python3 tools/ste_check.py docs/ --json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

# The style guide lists non-approved words as examples; scanning it would
# flag the rules themselves.
DEFAULT_EXCLUDES = {"docs/style-ste100.md"}

FRONTMATTER_KEY_RE = re.compile(r'^(title|description|sidebarTitle):\s*["\']?(.*?)["\']?\s*$')

BANNED_WORDS: dict[str, str] = {
    "utilize": "use",
    "utilise": "use",
    "utilized": "used",
    "utilised": "used",
    "employ": "use",
    "employs": "uses",
    "employed": "used",
    "leverage": "use",
    "leverages": "uses",
    "leveraging": "using",
    "facilitates": "enables",
    "in order to": "to",
    "prior to": "before",
    "subsequent to": "after",
    "subsequently": "after",
    "notwithstanding": "without",
    "whilst": "while",
    "hence": "start a new sentence",
    "thereby": "start a new sentence",
    "and/or": "restructure into separate items",
    "currently": "now (or delete)",
    "at this point in time": "now",
}

MARKETING_TERMS = (
    "blazing",
    "massive",
    "seamless",
    "seamlessly",
    "effortless",
    "effortlessly",
    "sophisticated",
    "enterprise-grade",
    "a breeze",
    "batteries included",
    "supercharge",
    "supercharged",
    "game-changer",
    "game changer",
    "cutting-edge",
    "state-of-the-art",
    "world-class",
    "unleash",
    "glue of",
    "the full power",
)

REVIEW_WORDS = ("however", "thus", "therefore", "attempt", "attempts", "e.g.", "i.e.")

CONTRACTION_RE = re.compile(
    r"\b\w+n't\b|\b\w+'re\b|\b\w+'ve\b|\b\w+'ll\b|\b\w+'d\b"
    r"|\b(?:it|that|there|what|who|he|she|let)'s\b",
    re.IGNORECASE,
)

SECOND_PERSON_RE = re.compile(
    r"\b(?:you|your|yours|yourself|yourselves|we|our|ours)\b", re.IGNORECASE
)

LIST_ITEM_RE = re.compile(r"^[-*+]\s|^\d+[.)]\s")
FENCE_RE = re.compile(r"^\s*(```|~~~)")
JSX_ATTR_RE = re.compile(r'\b(?:title|description|label|content|alt)="([^"]+)"')

MAX_PROCEDURAL_WORDS = 20
MAX_DESCRIPTIVE_WORDS = 25


@dataclass
class Issue:
    file: str
    line: int
    category: str
    detail: str


@dataclass
class FileReport:
    file: str
    sentences: int = 0
    words: int = 0
    max_sentence_words: int = 0
    violations: list[Issue] = field(default_factory=list)
    warnings: list[Issue] = field(default_factory=list)

    @property
    def avg_sentence_words(self) -> float:
        return round(self.words / self.sentences, 1) if self.sentences else 0.0


def split_frontmatter(text: str) -> tuple[str, str]:
    match = re.match(r"^---\s*\n(.*?)\n---\s*\n", text, re.DOTALL)
    if not match:
        return "", text
    frontmatter, body = match.group(1), text[match.end() :]
    prose = []
    for line in frontmatter.splitlines():
        key_match = FRONTMATTER_KEY_RE.match(line)
        if key_match:
            prose.append(key_match.group(2))
    return " ".join(prose), body


def body_lines(body: str) -> list[tuple[int, str | None]]:
    """Return (line number, text) pairs; None marks a paragraph break."""
    out: list[tuple[int, str | None]] = []
    in_fence = False
    for number, raw in enumerate(body.splitlines(), start=1):
        if FENCE_RE.match(raw):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        if not raw.strip():
            out.append((number, None))
            continue
        if raw.lstrip().startswith("|"):
            continue  # table row: identifier cells are exempt
        line = re.sub(r"\{/\*.*?\*/\}", " ", raw)
        line = re.sub(r"`[^`]*`", " ", line)  # inline code
        attr_prose = " ".join(m.group(1) for m in JSX_ATTR_RE.finditer(line))
        line = re.sub(r"<[^>]*>", " ", line)  # JSX/HTML tags
        if attr_prose:
            line = f"{attr_prose} {line}"
        line = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", line)  # links -> text
        line = re.sub(r"[*_>#`]+", " ", line)  # markdown emphasis/heading markup
        if line.strip():
            out.append((number, line.strip()))
    return out


def check_words(text: str, file: str, line: int) -> tuple[list[Issue], list[Issue]]:
    lowered = " " + text.lower() + " "
    violations = [
        Issue(file, line, "contraction", match.group(0)) for match in CONTRACTION_RE.finditer(text)
    ]
    violations += [
        Issue(file, line, "second-person", f"{match.group(0)} (use imperative)")
        for match in SECOND_PERSON_RE.finditer(text)
    ]
    violations += [
        Issue(file, line, "banned-word", f"{term} -> {replacement}")
        for term, replacement in BANNED_WORDS.items()
        if f" {term} " in lowered
    ]
    violations += [
        Issue(file, line, "marketing", term) for term in MARKETING_TERMS if term in lowered
    ]
    warnings = [
        Issue(file, line, "review-word", term) for term in REVIEW_WORDS if f" {term} " in lowered
    ]
    return violations, warnings


def split_sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+", text)
    return [part.strip() for part in parts if len(part.split()) >= 3]


def check_file(path: Path, root: Path) -> FileReport:
    relative = path.relative_to(root).as_posix()
    report = FileReport(file=relative)
    text = path.read_text(encoding="utf-8").replace("\r\n", "\n")

    frontmatter_prose, body = split_frontmatter(text)
    if frontmatter_prose:
        violations, warnings = check_words(frontmatter_prose, relative, 0)
        report.violations.extend(violations)
        report.warnings.extend(warnings)
        for sentence in split_sentences(frontmatter_prose):
            report.sentences += 1
            count = len(sentence.split())
            report.words += count
            report.max_sentence_words = max(report.max_sentence_words, count)

    lines = body_lines(body)
    block: list[tuple[int, str]] = []
    blocks: list[tuple[int, list[str]]] = []

    def flush() -> None:
        if block:
            blocks.append((block[0][0], [text_part for _, text_part in block]))
            block.clear()

    for number, content in lines:
        if content is None:
            flush()
            continue
        if block and LIST_ITEM_RE.match(content):
            flush()
        block.append((number, content))
    flush()

    for start_line, parts in blocks:
        joined = " ".join(parts)
        violations, warnings = check_words(joined, relative, start_line)
        report.violations.extend(violations)
        report.warnings.extend(warnings)
        for sentence in split_sentences(joined):
            report.sentences += 1
            count = len(sentence.split())
            report.words += count
            report.max_sentence_words = max(report.max_sentence_words, count)
            excerpt = sentence[:70] + ("..." if len(sentence) > 70 else "")
            if count > MAX_DESCRIPTIVE_WORDS:
                report.violations.append(
                    Issue(relative, start_line, "sentence-length", f"{count} words: {excerpt}")
                )
            elif count > MAX_PROCEDURAL_WORDS:
                report.warnings.append(
                    Issue(
                        relative, start_line, "sentence-length-review", f"{count} words: {excerpt}"
                    )
                )

    return report


def iter_doc_files(root: Path) -> list[Path]:
    files = sorted(p for p in root.rglob("*") if p.suffix in {".md", ".mdx"} and p.is_file())
    return [p for p in files if p.as_posix() not in DEFAULT_EXCLUDES]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("path", type=Path, help="docs directory or a single file")
    parser.add_argument("--strict", action="store_true", help="exit 1 on any violation")
    parser.add_argument("--json", action="store_true", help="print JSON instead of text")
    args = parser.parse_args(argv)

    root = args.path
    if root.is_file():
        files = [root]
        base = root.parent
    else:
        files = iter_doc_files(root)
        base = root

    reports = [check_file(path, base) for path in files]
    total_violations = sum(len(r.violations) for r in reports)
    total_warnings = sum(len(r.warnings) for r in reports)
    total_sentences = sum(r.sentences for r in reports)
    total_words = sum(r.words for r in reports)
    max_words = max((r.max_sentence_words for r in reports), default=0)

    if args.json:
        print(
            json.dumps(
                {
                    "files": len(reports),
                    "sentences": total_sentences,
                    "words": total_words,
                    "max_sentence_words": max_words,
                    "violations": total_violations,
                    "warnings": total_warnings,
                    "reports": [asdict(r) for r in reports],
                },
                indent=2,
            )
        )
    else:
        avg = round(total_words / total_sentences, 1) if total_sentences else 0.0
        print("tif1 STE checker - house ruleset (docs/style-ste100.md)")
        print(f"files: {len(reports)}  sentences: {total_sentences}  avg words: {avg}")
        print(f"max sentence: {max_words} words")
        print(f"violations: {total_violations}  warnings: {total_warnings}")
        worst = sorted(reports, key=lambda r: -len(r.violations))[:15]
        print("\ntop files by violations:")
        for report in worst:
            if report.violations:
                print(f"  {report.file}: {len(report.violations)}")
        print("\nfirst violations:")
        shown = 0
        for report in reports:
            for issue in report.violations:
                print(f"  {issue.file}:{issue.line} [{issue.category}] {issue.detail}")
                shown += 1
                if shown >= 30:
                    break
            if shown >= 30:
                break

    return 1 if args.strict and total_violations else 0


if __name__ == "__main__":
    sys.exit(main())
