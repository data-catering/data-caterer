#!/bin/bash
# Test script for YAML migration tool

set -e

echo "Testing YAML Migration Tool..."
echo "=============================="

# Check Python and PyYAML
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3.6+"
    exit 1
fi

if ! python3 -c "import yaml" 2>/dev/null; then
    echo "⚠️  PyYAML not found. Installing..."
    pip3 install pyyaml
fi

# Determine project root (3 levels up from this script)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
EXAMPLE_PLAN="$PROJECT_ROOT/example/docker/data/custom/plan/simple-json.yaml"

# Test 1: Dry run on example file
echo ""
echo "Test 1: Dry run migration"
echo "-------------------------"
if python3 migrate_yaml.py "$EXAMPLE_PLAN" --dry-run >/dev/null 2>&1; then
    echo "✓ Dry run successful"
else
    echo "❌ Dry run failed"
    exit 1
fi

# Test 2: Actual migration to temp file
echo ""
echo "Test 2: Migrate to temp file"
echo "----------------------------"
TEMP_OUTPUT=$(mktemp /tmp/migrated_XXXXXX.yaml)
if python3 migrate_yaml.py "$EXAMPLE_PLAN" "$TEMP_OUTPUT" 2>&1 | grep -q "Successfully migrated"; then
    echo "✓ Migration successful"

    # Verify output is valid YAML
    if python3 -c "import yaml; yaml.safe_load(open('$TEMP_OUTPUT'))" 2>/dev/null; then
        echo "✓ Output is valid YAML"
    else
        echo "❌ Output is not valid YAML"
        exit 1
    fi

    # Check for version field
    if grep -q "version: '1.0'" "$TEMP_OUTPUT"; then
        echo "✓ Version field present"
    else
        echo "❌ Version field missing"
        exit 1
    fi

    # Check for dataSources
    if grep -q "dataSources:" "$TEMP_OUTPUT"; then
        echo "✓ dataSources field present"
    else
        echo "❌ dataSources field missing"
        exit 1
    fi

    rm -f "$TEMP_OUTPUT"
else
    echo "❌ Migration failed"
    exit 1
fi

# Test 3: Help message
echo ""
echo "Test 3: Help message"
echo "-------------------"
if python3 migrate_yaml.py --help >/dev/null 2>&1; then
    echo "✓ Help message works"
else
    echo "❌ Help message failed"
    exit 1
fi

echo ""
echo "=============================="
echo "✅ All tests passed!"
echo ""
echo "Migration tool is ready to use:"
echo "  python3 migrate_yaml.py <input.yaml> [output.yaml]"
echo ""
echo "See MIGRATION.md for full documentation"
