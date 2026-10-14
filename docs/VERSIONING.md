# Documentation Versioning

This document explains how the tif1 documentation is deployed and how it relates to package releases.

## Overview

The documentation runs on the Mintlify free plan and uses a separate branch deployment strategy. One documentation version is live at a time, and it describes the latest published release.

## Deployment Strategy (Free Plan)

The Mintlify free plan has no API access. Deployment uses a dedicated branch:

1. **Development**: All doc changes happen on the `main` branch
2. **Production**: Mintlify auto-deploys from the `docs-production` branch
3. **Trigger**: GitHub Actions pushes to `docs-production` only on releases

### Reason for This Approach

- **Free plan limitation**: No API key for programmatic deployment
- **Solution**: Use Mintlify's auto-deploy feature with a dedicated branch
- **Benefit**: Docs only update on releases, not every commit

## How It Works

### On Release

1. The owner publishes a new release (for example, `v0.8.0`)
2. The "Deploy Documentation" workflow (`.github/workflows/docs.yml`) triggers
3. The workflow copies `docs/` from the release commit to the `docs-production` branch
4. Mintlify auto-deploys within minutes

### One Live Version

The site has no version selector. `docs.json` does not declare `versions`: Mintlify's `navigation.versions` needs one navigation tree per version, and the project keeps a single tree that always documents the latest release. Readers who need the documentation for an older release can browse the `docs/` directory of that release tag on GitHub.

## Mintlify Configuration

In the Mintlify dashboard, configure:

1. **Git Settings**:
   - Repository: `TracingInsights/tif1`
   - Branch: `docs-production` (important!)
   - Subdirectory: `docs`

2. **Auto-Deploy**:
   - Enable auto-deploy
   - Mintlify watches `docs-production` for changes

## Manual Deployment

To deploy docs without creating a release:

1. Go to GitHub Actions
2. Select "Deploy Documentation" workflow
3. Click "Run workflow"
4. Enter the version (for example, `v0.8.0`); it is only used in the commit message
5. Click "Run workflow"

This is useful for:
- Fixing documentation bugs between releases
- Testing the deployment process

## Best Practices

1. **Always test locally** before merging doc changes:
   ```bash
   cd docs
   npx mint dev
   ```

2. **Validate before merging**: run `npx mint validate` and `npx mint broken-links` in `docs/`, and run `uv run python tools/run_doc_examples.py` from the repository root to execute the code blocks

3. **Match the package**: when a release changes the public API, update the pages in the same pull request. Then the deployed docs match the released package

4. **Do not push to docs-production directly**: Always use the GitHub Actions workflow

5. **Review the PR preview**: The docs-preview workflow validates changes

## Troubleshooting

### Docs Not Deploying

- Check Mintlify dashboard → Git Settings → Branch is `docs-production`
- Verify auto-deploy is enabled
- Check GitHub Actions logs for errors
- Ensure `docs-production` branch exists and has recent commits

### Deployment Delayed

- Mintlify auto-deploy can take 2-5 minutes
- Check Mintlify dashboard for deployment status
- Verify webhook is configured (if applicable)

## Migration from API-Based Deployment

To move to a Pro plan later:

1. Update `.github/workflows/docs.yml` to use the Mintlify CLI
2. Add `MINTLIFY_API_KEY` to GitHub Secrets
3. Change deployment branch back to `main` in Mintlify settings
4. Remove the `docs-production` branch strategy

## Resources

- [Mintlify Git Settings](https://mintlify.com/docs/settings/git)
- [Mintlify Auto-Deploy](https://mintlify.com/docs/settings/git#auto-deploy)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
