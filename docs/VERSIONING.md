# Documentation Versioning

This document explains how versioned documentation works for tif1.

## Overview

The tif1 documentation uses Mintlify's free plan with a separate branch deployment strategy. Documentation is automatically deployed when releases are published.

## Deployment Strategy (Free Plan)

Since we're on Mintlify's free plan (no API access), we use a branch-based deployment:

1. **Development**: All doc changes happen on the `main` branch
2. **Production**: Mintlify auto-deploys from the `docs-production` branch
3. **Trigger**: GitHub Actions pushes to `docs-production` only on releases

### Why This Approach?

- **Free plan limitation**: No API key for programmatic deployment
- **Solution**: Use Mintlify's auto-deploy feature with a dedicated branch
- **Benefit**: Docs only update on releases, not every commit

## How It Works

### On Release

1. You publish a new release (e.g., `v0.2.0`)
2. GitHub Actions workflow triggers
3. Workflow updates `docs.json` with the new version
4. Workflow pushes docs to `docs-production` branch
5. Mintlify auto-deploys within minutes
6. Version appears in the version selector

### Version Selector

Users can switch between documentation versions using the version selector in the navbar:
- Each release creates a versioned snapshot
- Versions are listed in the `versions` array in `docs.json`
- The `version` field shows the currently deployed version

## Mintlify Configuration

In your Mintlify dashboard, configure:

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
4. Enter the version (e.g., `v0.2.0`)
5. Click "Run workflow"

This is useful for:
- Fixing documentation bugs between releases
- Updating docs for an existing version
- Testing the deployment process

## Version Management

### Automatic Updates

The GitHub Actions workflow automatically:
- Extracts version from release tag (e.g., `v0.2.0` → `0.2.0`)
- Updates the `version` field in `docs.json`
- Adds the version to the `versions` array (if not present)
- Commits and pushes to `docs-production`

### Manual Updates

If you need to manually manage versions:

```bash
# Edit docs/docs.json
{
  "version": "0.2.0",
  "versions": ["latest", "0.2.0", "0.1.0"]
}
```

## Best Practices

1. **Always test locally** before merging doc changes:
   ```bash
   cd docs
   npx mintlify dev
   ```

2. **Keep versions in sync**: The workflow handles this, but verify after deployment

3. **Use semantic versioning**: Match your package versions (e.g., `0.2.0`, `0.2.1`)

4. **Don't push to docs-production directly**: Always use the GitHub Actions workflow

5. **Review the PR preview**: The docs-preview workflow validates changes

## Troubleshooting

### Docs Not Deploying

- Check Mintlify dashboard → Git Settings → Branch is `docs-production`
- Verify auto-deploy is enabled
- Check GitHub Actions logs for errors
- Ensure `docs-production` branch exists and has recent commits

### Version Selector Not Showing

- Verify `versionSelector.enabled: true` in `docs.json`
- Check that `versions` array has multiple entries
- Clear browser cache and reload

### Deployment Delayed

- Mintlify auto-deploy can take 2-5 minutes
- Check Mintlify dashboard for deployment status
- Verify webhook is configured (if applicable)

## Migration from API-Based Deployment

If you later upgrade to a Pro plan:

1. Update `.github/workflows/docs.yml` to use Mintlify CLI
2. Add `MINTLIFY_API_KEY` to GitHub Secrets
3. Change deployment branch back to `main` in Mintlify settings
4. Remove the `docs-production` branch strategy

## Resources

- [Mintlify Git Settings](https://mintlify.com/docs/settings/git)
- [Mintlify Auto-Deploy](https://mintlify.com/docs/settings/git#auto-deploy)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
