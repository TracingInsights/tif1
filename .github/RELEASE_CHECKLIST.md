# Release Checklist

Use this checklist before creating a new release.

## Pre-Release

### Code Quality
- [ ] All tests pass: `uv run pytest tests/ -v`
- [ ] Linting passes: `uv run ruff check src/ tests/`
- [ ] Formatting is correct: `uv run ruff format --check src/ tests/`
- [ ] Type checking passes: `uv run ty check src/tif1`
- [ ] Coverage meets threshold (80%): Check coverage report
- [ ] No TODO/FIXME comments in critical paths
- [ ] All deprecation warnings addressed

### Documentation
- [ ] README.md is up to date
- [ ] CHANGELOG.md has entry for this version
- [ ] API documentation is current
- [ ] Examples work with new version
- [ ] Migration guide (if breaking changes)

### Version & Metadata
- [ ] Version bumped in `pyproject.toml`
- [ ] Version follows semantic versioning
- [ ] Dependencies are up to date
- [ ] License information is correct
- [ ] Author/maintainer info is current

### Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Property-based tests pass
- [ ] Benchmarks show no regression
- [ ] Manual smoke test completed
- [ ] Tested on Python 3.11, 3.12, 3.13, 3.14

### Build & Package
- [ ] Package builds successfully: `uv build`
- [ ] Package contents verified: `tar -tzf dist/tif1-*.tar.gz`
- [ ] Wheel builds correctly
- [ ] No unnecessary files in distribution
- [ ] Package size is reasonable

## Release Process

Releases are published by dispatching the **Publish to PyPI** workflow
(`.github/workflows/publish.yml`) manually:

1. Bump `version` in `pyproject.toml` and add a `CHANGELOG.md` entry.
2. Dispatch `publish.yml` with `environment: testpypi` to validate.
3. Dispatch `publish.yml` with `environment: pypi`.

The `pypi` dispatch publishes to PyPI, then automatically creates the git tag,
the GitHub release (notes pulled from `CHANGELOG.md`), and a release discussion
in the Announcements category. All steps are idempotent: re-running the dispatch
for an already-published version skips the uploads and backfills any missing
tag/release/discussion, so the checklist steps below only verify the workflow's
output.

### TestPyPI (Recommended First)
- [ ] Published to TestPyPI
- [ ] Installed from TestPyPI successfully
- [ ] Basic functionality tested from TestPyPI install
- [ ] Dependencies resolve correctly

### PyPI
- [ ] Published to PyPI
- [ ] Package visible on PyPI: https://pypi.org/project/tif1/
- [ ] Metadata displays correctly
- [ ] README renders properly
- [ ] Links work (homepage, docs, issues)

### Git
- [ ] Changes committed
- [ ] Version tag created and pushed by the `publish.yml` workflow (e.g., `v0.5.1`)
- [ ] Branch is clean

### GitHub
- [ ] GitHub release created by the `publish.yml` workflow
- [ ] Release notes populated from `CHANGELOG.md`
- [ ] Release discussion opened in the Announcements category
- [ ] Assets uploaded (if any)

## Post-Release

### Verification
- [ ] Install from PyPI works: `pip install tif1`
- [ ] Import works: `python -c "import tif1"`
- [ ] Version correct: `python -c "import tif1; print(tif1.__version__)"`
- [ ] Basic functionality works
- [ ] CLI works: `tif1 --version`

### Communication
- [ ] Announcement prepared
- [ ] Social media posts scheduled


### Maintenance
- [ ] Monitor PyPI download stats
- [ ] Watch for bug reports
- [ ] Check CI/CD status
- [ ] Update project board
- [ ] Close milestone (if using)

### Next Version
- [ ] Bump to next dev version in `pyproject.toml`
- [ ] Create new CHANGELOG.md section
- [ ] Update roadmap
- [ ] Plan next release

## Emergency Rollback

If critical issues are found:

1. **Yank the release on PyPI** (doesn't delete, but prevents new installs)
   ```bash
   # Not directly supported by uv, use twine or PyPI web interface
   ```

2. **Create hotfix release**
   - Fix the issue
   - Bump patch version (e.g., 0.2.0 → 0.2.1)
   - Follow release process

3. **Communicate**
   - Update GitHub release with warning
   - Post announcement about issue
   - Provide workaround if available

## Version-Specific Notes

### v0.7.0 (Performance Release)
- [ ] Validate `import tif1` and `tif1.__version__ == "0.7.0"`
- [ ] Confirm public `validate_laps` / `validate_telemetry` still use pydantic
- [ ] Confirm fetch-path validation is pydantic-free (`validate_*_data`)
- [ ] Confirm legacy SQLite TEXT cache rows still read
- [ ] Confirm default `max_concurrent_requests` is 22 and `cache_commit_interval` is 100
- [ ] Confirm `docs/docs.json` version is `0.7.0` and `0.7.0` is in `versions`

### v0.4.0 (Major Release)
- [ ] Validate `import tif1` and `tif1.__version__ == "0.4.0"`
- [ ] Confirm Python 3.11+ requirement is documented (3.10 support dropped)
- [ ] Verify pandas 3.0 migration notes are accurate
- [ ] Verify native chart functions work correctly
- [ ] Confirm documentation links are correct

### v0.2.0 (Package Rename Release)
- [ ] Verify PyPI/TestPyPI package name is `tif1`
- [ ] Confirm install docs no longer reference `tifone`
- [ ] Validate `import tif1` and `tif1.__version__ == "0.2.0"`
- [ ] Check release notes mention the package rename
- [ ] Performance benchmarks documented

---

**Release Manager:** _________________
**Date:** _________________
**Version:** _________________
