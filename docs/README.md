# tif1 Documentation

This directory contains the Mintlify documentation for tif1.

## Deployment Strategy (Free Plan)

Since we're on Mintlify's free plan, we use a **separate branch deployment** strategy:

- **Main branch**: Development and PR reviews happen here
- **docs-production branch**: Mintlify auto-deploys from this branch
- **Trigger**: Documentation is pushed to `docs-production` only on releases

### How It Works

1. You make doc changes in PRs on the `main` branch
2. When you publish a release, GitHub Actions automatically:
   - Updates the version in `docs.json`
   - Pushes the docs to the `docs-production` branch
   - Mintlify auto-deploys from `docs-production`

## Local Development

To preview the documentation locally:

```bash
# No installation needed - use npx
cd docs
npx mintlify dev
```

The documentation will be available at `http://localhost:3000`.

## Deployment

### Automatic Deployment (Recommended)

1. Merge your doc changes to `main`
2. Create and publish a new release on GitHub (e.g., `v0.2.0`)
3. GitHub Actions automatically pushes to `docs-production`
4. Mintlify auto-deploys within minutes

### Manual Deployment

If you need to deploy docs without a release:

1. Go to Actions → "Deploy Documentation"
2. Click "Run workflow"
3. Enter the version (e.g., `v0.2.0`)
4. Click "Run workflow"

## Mintlify Configuration

In your Mintlify dashboard:
1. Go to Git Settings
2. Set deployment branch to: `docs-production`
3. Enable auto-deploy
4. Set subdirectory to: `docs`

## Versioning

See [VERSIONING.md](./VERSIONING.md) for details on how documentation versioning works.

The version selector in the navbar allows users to browse docs for different releases.

## Structure

- `docs.json` - Mintlify configuration
- `*.mdx` - Documentation pages
- `api-reference/` - API reference documentation
- `guides/` - User guides
- `tutorials/` - Step-by-step tutorials
- `concepts/` - Conceptual documentation
- `reference/` - Reference documentation
- `assets/` - Images and other assets

## Configuration

The main configuration file is `docs.json`, which includes:
- Site metadata (name, description, theme)
- Navigation structure
- Version configuration (updated automatically on release)
- Styling and appearance
- Analytics integration

## Adding New Pages

1. Create a new `.mdx` file in the appropriate directory
2. Add the page to the navigation in `docs.json`
3. Test locally with `npx mintlify dev`
4. Submit a PR with your changes
5. Docs will deploy when the next release is published

## Version Management

The `versions` array in `docs.json` is automatically updated when you release:
- GitHub Actions adds the new version to the array
- Users can switch between versions using the navbar selector
- The `version` field shows the current deployed version

## Troubleshooting

**Docs not deploying?**
- Check that `docs-production` branch is set in Mintlify Git Settings
- Verify auto-deploy is enabled in Mintlify dashboard
- Check GitHub Actions logs for errors

**Version selector not showing?**
- Ensure `versionSelector.enabled` is `true` in `docs.json`
- Verify multiple versions exist in the `versions` array

**Local preview not working?**
- Make sure you're in the `docs/` directory
- Try `npx mintlify@latest dev` to use the latest version

## Resources

- [Mintlify Documentation](https://mintlify.com/docs)
- [Mintlify Free Plan Deployment](https://mintlify.com/docs/settings/git)
- [MDX Documentation](https://mdxjs.com/)
