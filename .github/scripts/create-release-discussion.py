#!/usr/bin/env python3
"""
Create a GitHub discussion for a new release.
This script is called from the publish.yml workflow (create-github-release job).

Expects RELEASE_BODY (inline body) or RELEASE_BODY_FILE (path to a body file).
"""

import json
import os
import subprocess
import sys


def graphql(query: str, variables: dict) -> dict:
    """Execute a GraphQL query using gh CLI."""
    payload = json.dumps({"query": query, "variables": variables})
    result = subprocess.run(
        ["gh", "api", "graphql", "--input", "-"],
        input=payload,
        capture_output=True,
        text=True,
        timeout=30,  # 30 second timeout for network operations
        check=False,
    )
    if result.returncode != 0:
        print("GraphQL error:", result.stderr, file=sys.stderr)
        sys.exit(1)
    data = json.loads(result.stdout)
    if "errors" in data:
        print("GraphQL errors:", data["errors"], file=sys.stderr)
        sys.exit(1)
    return data["data"]


def existing_discussion_url(owner: str, repo: str, title: str) -> str | None:
    """Return the URL of an existing discussion with the given title, if any.

    Used to keep discussion creation idempotent so a re-run of the release job
    never posts a duplicate announcement.
    """
    data = graphql(
        """
        query($owner: String!, $repo: String!) {
          repository(owner: $owner, name: $repo) {
            discussions(first: 100, orderBy: {field: CREATED_AT, direction: DESC}) {
              nodes { title url }
            }
          }
        }
        """,
        {"owner": owner, "repo": repo},
    )
    for node in data["repository"]["discussions"]["nodes"]:
        if node["title"] == title:
            return node["url"]
    return None


def pick_category(cats):
    """Pick the best discussion category for announcements."""
    for c in cats:
        if c["name"].lower() == "announcements":
            return c
    for c in cats:
        if "announce" in c["name"].lower():
            return c
    return cats[0]


def main():
    """Main entry point."""
    # Get environment variables
    owner = os.environ["REPO_OWNER"]
    repo = os.environ["REPO_NAME"]
    tag = os.environ["RELEASE_TAG"]
    name = os.environ["RELEASE_NAME"] or tag
    body_file = os.environ.get("RELEASE_BODY_FILE")
    if body_file:
        with open(body_file, encoding="utf-8") as fh:
            body_raw = fh.read()
    else:
        body_raw = os.environ["RELEASE_BODY"]
    release_url = os.environ["RELEASE_URL"]
    is_prerelease = os.environ["IS_PRERELEASE"].lower() == "true"

    # Resolve repo node ID + discussion categories
    meta = graphql(
        """
        query($owner: String!, $repo: String!) {
          repository(owner: $owner, name: $repo) {
            id
            discussionCategories(first: 20) {
              nodes { id name }
            }
          }
        }
        """,
        {"owner": owner, "repo": repo},
    )

    repo_id = meta["repository"]["id"]
    categories = meta["repository"]["discussionCategories"]["nodes"]

    if not categories:
        print(
            "No discussion categories found. Enable Discussions and add at least one category.",
            file=sys.stderr,
        )
        sys.exit(1)

    category = pick_category(categories)
    print(f"Using discussion category: {category['name']!r}")

    # Build the discussion body
    pip_version = tag.lstrip("v")  # Strip leading 'v' for pip install

    prerelease_banner = ""
    if is_prerelease:
        prerelease_banner = (
            "> [!WARNING]\n"
            "> This is a **pre-release**. APIs may change before the stable release. "
            "Use with caution in production.\n\n"
        )

    if body_raw.strip():
        release_notes_section = f"## What's changed\n\n{body_raw.strip()}\n\n"
    else:
        release_notes_section = "## What's changed\n\n_No release notes provided._\n\n"

    discussion_body = f"""{prerelease_banner}{release_notes_section}---

## Install

```bash
pip install tif1=={pip_version}
```

Or upgrade an existing installation:

```bash
pip install --upgrade tif1
```

---

## Links

- 📋 [Full release notes & assets]({release_url})
- 📦 [tif1 on PyPI](https://pypi.org/project/tif1/{pip_version}/)
- 📚 [Documentation](https://tif1.tracinginsights.com)
- 🐛 [Report a bug](https://github.com/{owner}/{repo}/issues/new?labels=bug&template=bug_report.md)
- 💬 [Ask a question](https://github.com/{owner}/{repo}/discussions/new?category=q-a)

---

*This discussion was automatically created when **{tag}** was published.*
"""

    # Create the discussion, skipping if one already exists for this release
    # (keeps re-runs of the publish workflow idempotent).
    title = f"🚀 {name}" if name != tag else f"🚀 Release {tag}"

    url = existing_discussion_url(owner, repo, title)
    if url is not None:
        print(f"Discussion {title!r} already exists; skipping creation: {url}")
        return

    result = graphql(
        """
        mutation($repoId: ID!, $catId: ID!, $title: String!, $body: String!) {
          createDiscussion(input: {
            repositoryId: $repoId
            categoryId:   $catId
            title:        $title
            body:         $body
          }) {
            discussion { url number }
          }
        }
        """,
        {
            "repoId": repo_id,
            "catId": category["id"],
            "title": title,
            "body": discussion_body,
        },
    )

    discussion = result["createDiscussion"]["discussion"]
    print(f"Discussion created: {discussion['url']}")


if __name__ == "__main__":
    main()
