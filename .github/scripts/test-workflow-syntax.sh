#!/bin/bash
set -euo pipefail

# Simple workflow syntax validation script
# This can be run locally to validate YAML syntax and basic structure

echo "🔍 Validating GitHub Actions workflow syntax..."

WORKFLOW_DIR=".github/workflows"
ERRORS=0

validate_yaml() {
    local file="$1"
    echo "  Checking $file..."
    
    # Check if python can parse the YAML (basic syntax check)
    if python3 -c "import yaml; yaml.safe_load(open('$file'))" 2>/dev/null; then
        echo "    ✅ $file syntax OK"
    else
        echo "    ❌ YAML syntax error in $file"
        ((ERRORS++))
        return 1
    fi
    
    # Check for common issues
    if grep -q "actions/checkout@v6" "$file"; then
        echo "    ⚠️  Warning: $file uses checkout@v6, consider using @v4 for consistency"
    fi
    
    if grep -q "python -c.*\$[A-Z_]*" "$file"; then
        echo "    ⚠️  Warning: $file may have shell variable interpolation issues in Python code"
    fi
    
    # Check for missing timeouts in jobs
    if grep -q "runs-on:" "$file" && ! grep -q "timeout-minutes:" "$file"; then
        echo "    ⚠️  Warning: $file has jobs without timeout-minutes specified"
    fi
    
    return 0
}

# Validate all workflow files
for workflow in "$WORKFLOW_DIR"/*.yml "$WORKFLOW_DIR"/*.yaml; do
    if [[ -f "$workflow" ]]; then
        validate_yaml "$workflow"
    fi
done

# Check for executable scripts
echo "🔍 Checking script permissions..."
for script in .github/scripts/*.sh; do
    if [[ -f "$script" ]]; then
        if [[ -x "$script" ]]; then
            echo "  ✅ $script is executable"
        else
            echo "  ⚠️  $script is not executable (will be made executable in workflow)"
        fi
    fi
done

if [[ $ERRORS -eq 0 ]]; then
    echo "✅ All workflow files passed validation"
    exit 0
else
    echo "❌ Found $ERRORS error(s) in workflow files"
    exit 1
fi