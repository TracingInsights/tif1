# Git Hooks

This directory contains version-controlled git hooks for the project.

## Installation

Configure git to use this directory for hooks (works on all platforms):

```bash
git config core.hooksPath .githooks
```

This is a one-time setup per clone. The hooks will be used directly from this directory.

### Verify Installation

```bash
git config core.hooksPath
# Should output: .githooks
```

## Available Hooks

### pre-commit

Runs `prek` linting checks on staged Python files before allowing a commit.

- **What it does**: Validates code style and formatting on files being committed
- **On failure**: Blocks the commit and shows what needs to be fixed
- **Quick fix**: Run `uv run prek run --fix <files>` to auto-fix issues
- **Bypass**: Use `git commit --no-verify` (not recommended)

## Adding New Hooks

1. Create a new hook file in `.githooks/` (e.g., `pre-push`, `commit-msg`)
2. Make it executable: `chmod +x .githooks/<hook-name>` (Unix/Mac) or `git add --chmod=+x .githooks/<hook-name>` (Windows)
3. Commit the new hook to version control
4. Hooks are automatically used by git (no installation needed)

## Notes

- Hooks in `.githooks/` are version controlled and shared with the team
- Each developer needs to run `git config core.hooksPath .githooks` once after cloning
- Works on Windows, macOS, and Linux
- No setup scripts needed - git uses the hooks directly from this directory
