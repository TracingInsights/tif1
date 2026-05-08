# Git Hooks Setup Instructions

## Automatic Setup (GitHub Codespaces)

If you're using GitHub Codespaces, the hooks are configured automatically. Nothing to do!

## Manual Setup (Local Development)

After cloning the repository, run this one-time command:

```bash
git config core.hooksPath .githooks
```

### Verify Setup

Check that the configuration is set:

```bash
git config core.hooksPath
```

Expected output: `.githooks`

### Make Hooks Executable (Unix/Mac/WSL)

If you're on Unix, Mac, or WSL, ensure the hooks are executable:

```bash
chmod +x .githooks/pre-commit
```

On Windows with Git Bash, you can also use:

```bash
git add --chmod=+x .githooks/pre-commit
```

## How It Works

- Git will use hooks directly from `.githooks/` directory
- No copying needed - hooks are version controlled
- Works on Windows, macOS, and Linux
- Updates to hooks are automatically picked up

## Testing the Hook

Try making a commit with a Python file that has lint issues:

```bash
# Create a test file with issues
echo "import os" > test.py
echo "x=1" >> test.py

# Try to commit it
git add test.py
git commit -m "test"
```

The pre-commit hook should block the commit and show the lint issues.

## Troubleshooting

### Hook not running?

Check the configuration:
```bash
git config core.hooksPath
```

If empty, run:
```bash
git config core.hooksPath .githooks
```

### Permission denied error?

Make the hook executable:
```bash
chmod +x .githooks/pre-commit
```

### Want to bypass the hook temporarily?

```bash
git commit --no-verify -m "your message"
```

(Not recommended - fix the issues instead!)
