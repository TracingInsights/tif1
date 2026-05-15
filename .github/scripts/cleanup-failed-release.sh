#!/bin/bash
set -euo pipefail

# Cleanup script for failed releases
# This script is called when a release workflow fails and needs to clean up
# any partial state (tags, commits) that were created.

# Input validation
if [[ $# -lt 2 || $# -gt 4 ]]; then
    echo "❌ Usage: $0 <tag> <version> [version_commit] [original_commit]"
    echo "   tag: Git tag to clean up (e.g., v1.2.3)"
    echo "   version: Version string (e.g., 1.2.3)"
    echo "   version_commit: Optional commit hash of version bump"
    echo "   original_commit: Optional commit hash to revert to"
    exit 1
fi

TAG="$1"
VERSION="$2"
VERSION_COMMIT="${3:-}"
ORIGINAL_COMMIT="${4:-}"

# Validate tag format
if [[ ! "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
    echo "❌ Invalid tag format: $TAG"
    echo "Expected format: vX.Y.Z or vX.Y.Z-suffix (e.g., v1.2.3 or v1.2.3-beta.1)"
    exit 1
fi

# Validate version format
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
    echo "❌ Invalid version format: $VERSION"
    echo "Expected format: X.Y.Z or X.Y.Z-suffix (e.g., 1.2.3 or 1.2.3-beta.1)"
    exit 1
fi

# Validate commit hashes if provided
if [[ -n "$VERSION_COMMIT" && ! "$VERSION_COMMIT" =~ ^[a-f0-9]{40}$ ]]; then
    echo "❌ Invalid version_commit hash format: $VERSION_COMMIT"
    exit 1
fi

if [[ -n "$ORIGINAL_COMMIT" && ! "$ORIGINAL_COMMIT" =~ ^[a-f0-9]{40}$ ]]; then
    echo "❌ Invalid original_commit hash format: $ORIGINAL_COMMIT"
    exit 1
fi

echo "🧹 Starting cleanup for failed release $VERSION (tag: $TAG)"

cleanup_remote_tag() {
    if git ls-remote --exit-code --tags origin "$TAG" >/dev/null 2>&1; then
        echo "🗑️  Deleting remote tag $TAG..."
        if git push origin ":refs/tags/$TAG"; then
            echo "✅ Successfully deleted remote tag $TAG"
            return 0
        else
            echo "⚠️  Failed to delete remote tag $TAG"
            return 1
        fi
    else
        echo "ℹ️  Tag $TAG was not found on remote"
        return 0
    fi
}

cleanup_local_tag() {
    if git tag -l | grep -q "^$TAG$"; then
        if git tag -d "$TAG"; then
            echo "🗑️  Deleted local tag $TAG"
            return 0
        else
            echo "⚠️  Failed to delete local tag $TAG"
            return 1
        fi
    else
        echo "ℹ️  Local tag $TAG not found"
        return 0
    fi
}

revert_version_commit() {
    local target_commit="$1"
    local commit_description="$2"
    
    echo "🔄 Reverting to $commit_description ($target_commit)..."
    
    if git reset --hard "$target_commit"; then
        echo "✅ Successfully reset to $commit_description"
        
        # Try to update remote
        if git push --force-with-lease origin HEAD; then
            echo "✅ Successfully updated remote branch"
            return 0
        else
            echo "⚠️  Failed to update remote branch (may require manual cleanup)"
            echo "ℹ️  Local repository has been reset to correct state"
            return 1
        fi
    else
        echo "❌ Failed to reset to $commit_description"
        return 1
    fi
}

# Main cleanup logic
main() {
    local cleanup_success=true
    
    # Step 1: Clean up remote tag
    if ! cleanup_remote_tag; then
        cleanup_success=false
    fi
    
    # Step 2: Clean up local tag
    if ! cleanup_local_tag; then
        cleanup_success=false
    fi
    
    # Step 3: Revert version commit if we have precise commit hashes
    if [[ -n "$VERSION_COMMIT" && -n "$ORIGINAL_COMMIT" ]]; then
        # Verify commits exist
        if git cat-file -e "$VERSION_COMMIT" 2>/dev/null && git cat-file -e "$ORIGINAL_COMMIT" 2>/dev/null; then
            if ! revert_version_commit "$ORIGINAL_COMMIT" "original commit"; then
                cleanup_success=false
            fi
        else
            echo "⚠️  One or both commit hashes are invalid, skipping revert"
            cleanup_success=false
        fi
    else
        # Fallback: check if latest commit is a version bump
        local latest_commit_msg
        latest_commit_msg=$(git log -1 --pretty=format:"%s" HEAD 2>/dev/null || echo "")
        local expected_msg="chore: bump version to $VERSION"
        
        if [[ "$latest_commit_msg" == "$expected_msg" ]]; then
            echo "🔄 Found version bump commit as latest commit, reverting..."
            if ! revert_version_commit "HEAD~1" "previous commit"; then
                cleanup_success=false
            fi
        else
            echo "ℹ️  Latest commit is not a version bump, no revert needed"
        fi
    fi
    
    # Summary
    if [[ "$cleanup_success" == "true" ]]; then
        echo "✅ Cleanup completed successfully"
        echo "ℹ️  Repository should be in a clean state for retry"
    else
        echo "⚠️  Cleanup completed with some issues"
        echo "ℹ️  Manual verification may be required before retry"
    fi
    
    echo "❌ Release $VERSION failed. Please verify repository state before retrying."
}

main "$@"