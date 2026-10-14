# tif1 Documentation

This directory contains the Mintlify documentation for tif1.

## Deployment Strategy (Free Plan)

The documentation runs on the Mintlify free plan. Deployment uses a **separate branch deployment** strategy:

- **Main branch**: Development and PR reviews happen here
- **docs-production branch**: Mintlify auto-deploys from this branch
- **Trigger**: Documentation is pushed to `docs-production` only on releases

### How It Works

1. Make documentation changes in pull requests on the `main` branch
2. When a release is published, GitHub Actions runs this sequence:
   - Pushes the docs to the `docs-production` branch
   - Mintlify auto-deploys from `docs-production`

## Local Development

To preview the documentation locally:

```bash
# No installation needed - use npx
cd docs
npx mint dev
```

The documentation is then available at `http://localhost:3000`.

## Deployment

### Automatic Deployment (Recommended)

1. Merge the documentation changes to `main`
2. Create and publish a new release on GitHub (for example, `v0.2.0`)
3. GitHub Actions automatically pushes to `docs-production`
4. Mintlify auto-deploys within minutes

### Manual Deployment

To deploy the documentation without a release:

1. Go to Actions → "Deploy Documentation"
2. Click "Run workflow"
3. Enter the version (for example, `v0.2.0`)
4. Click "Run workflow"

## Mintlify Configuration

In the Mintlify dashboard:
1. Go to Git Settings
2. Set deployment branch to: `docs-production`
3. Enable auto-deploy
4. Set subdirectory to: `docs`

## Versioning

See [VERSIONING.md](./VERSIONING.md) for details. One documentation version is live at a time and it describes the latest release; there is no version selector.

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
- Styling and appearance
- Analytics integration

## Adding New Pages

1. Create a new `.mdx` file in the appropriate directory
2. Add the page to the navigation in `docs.json`
3. Test locally with `npx mint dev`
4. Submit a pull request with the changes
5. The documentation deploys when the next release is published

## Troubleshooting

**Documentation Does Not Deploy**
- Check that `docs-production` branch is set in Mintlify Git Settings
- Verify auto-deploy is enabled in Mintlify dashboard
- Check GitHub Actions logs for errors

**Local Preview Does Not Work**
- Make sure the current directory is `docs/`
- Try `npx mint@latest dev` to use the latest version

## Resources

- [Mintlify Documentation](https://mintlify.com/docs)
- [Mintlify Free Plan Deployment](https://mintlify.com/docs/settings/git)
- [MDX Documentation](https://mdxjs.com/)
