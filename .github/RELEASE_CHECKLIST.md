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
- [ ] Tested on Python 3.10, 3.11, 3.12, 3.13, 3.14

### Build & Package
- [ ] Package builds successfully: `uv build`
- [ ] Package contents verified: `tar -tzf dist/tif1-*.tar.gz`
- [ ] Wheel builds correctly
- [ ] No unnecessary files in distribution
- [ ] Package size is reasonable

## Release Process

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
- [ ] Version tag created: `v0.1.0`
- [ ] Tag pushed to GitHub
- [ ] Branch is clean

### GitHub
- [ ] GitHub release created
- [ ] Release notes added
- [ ] Changelog linked
- [ ] Assets uploaded (if any)

## Post-Release

### Verification
- [ ] Install from PyPI works: `pip install tifone`
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
   - Bump patch version (e.g., 0.1.0 → 0.1.1)
   - Follow release process

3. **Communicate**
   - Update GitHub release with warning
   - Post announcement about issue
   - Provide workaround if available

## Version-Specific Notes

### v0.1.0 (Initial Release)
- [ ] Verify all core features work
- [ ] Double-check API surface is stable
- [ ] Ensure backward compatibility plan
- [ ] Performance benchmarks documented
- [ ] Known issues documented

---

**Release Manager:** _________________
**Date:** _________________
**Version:** _________________
