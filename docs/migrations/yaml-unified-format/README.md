# YAML Unified Format Migration

Automated migration from legacy YAML format (separate plan + task files) to the new unified YAML format (single-file configuration).

## Quick Start

```bash
# Navigate to this directory
cd docs/migrations/yaml-unified-format

# Migrate a single plan file
python3 migrate_yaml.py /path/to/old_plan.yaml /path/to/new_plan.yaml

# Auto-detect output filename
python3 migrate_yaml.py /path/to/old_plan.yaml

# Preview migration without writing files (dry-run)
python3 migrate_yaml.py /path/to/old_plan.yaml --dry-run
```

## Files in This Directory

- **[MIGRATION.md](MIGRATION.md)** - Comprehensive migration guide with format comparison and manual steps
- **[migrate_yaml.py](migrate_yaml.py)** - Python migration script (requires Python 3.6+, PyYAML)
- **[test_migration.sh](test_migration.sh)** - Automated test suite to validate the migration tool

## Prerequisites

- Python 3.6 or higher
- PyYAML library

```bash
# Install PyYAML if needed
pip3 install pyyaml
```

## Usage Examples

### Single File Migration

```bash
# Basic migration
python3 migrate_yaml.py plan.yaml unified_plan.yaml

# With explicit task folder
python3 migrate_yaml.py plan.yaml --task-folder ./tasks
```

### Batch Migration

Migrate entire directories:

```bash
# Migrate all YAML files in a directory
python3 migrate_yaml.py --directory /path/to/old_plans /path/to/new_plans
```

### Dry Run (Preview)

Preview the migration without creating files:

```bash
python3 migrate_yaml.py plan.yaml --dry-run
```

## Testing the Migration Tool

Run the test suite to verify the migration tool works correctly:

```bash
chmod +x test_migration.sh
./test_migration.sh
```

## What Gets Migrated?

The script automatically converts:

- ✅ Plan metadata (name, description)
- ✅ Tasks → Data sources
- ✅ Connection configurations (from task files)
- ✅ Steps and field definitions
- ✅ Configuration flags
- ✅ Validation rules
- ✅ Schema → Fields conversion
- ✅ Generator options flattening

## Format Differences

### Legacy Format (Before)

**plan.yaml:**
```yaml
name: "my_plan"
tasks:
  - name: "my_task"
    dataSourceName: "postgres_db"
```

**task/postgres_db.yaml:**
```yaml
type: "postgres"
url: "jdbc:postgresql://localhost:5432/db"
steps:
  - name: "users"
    schema:
      fields:
        - name: "id"
          generator:
            options:
              regex: "USR[0-9]{6}"
```

### Unified Format (After)

**unified_plan.yaml:**
```yaml
version: "1.0"
name: "my_plan"
dataSources:
  - name: "postgres_db"
    connection:
      type: "postgres"
      options:
        url: "jdbc:postgresql://localhost:5432/db"
    steps:
      - name: "users"
        fields:
          - name: "id"
            options:
              regex: "USR[0-9]{6}"
```

## Troubleshooting

### Issue: "Task folder not found"

**Solution:** Specify the task folder explicitly:
```bash
python3 migrate_yaml.py plan.yaml --task-folder /path/to/tasks
```

### Issue: "Module yaml not found"

**Solution:** Install PyYAML:
```bash
pip3 install pyyaml
```

### Issue: Migration warnings

Review the warnings output. Common warnings:
- Disabled tasks are skipped
- Missing task files are noted

## Testing Migrated Files

After migration, test with Data Caterer:

```bash
# Set the plan file path
export PLAN_FILE_PATH=/path/to/unified_plan.yaml

# Run Data Caterer
cd ../../..  # Return to project root
./gradlew :app:run
```

Or use the manual test runner:

```bash
YAML_FILE=/path/to/unified_plan.yaml ./gradlew :app:manualTest --tests "*YamlFileManualTest"
```

## Getting Help

- **Full Documentation:** [MIGRATION.md](MIGRATION.md)
- **Examples:** Check `misc/schema/examples/` in the project root
- **Issues:** [GitHub Issues](https://github.com/data-catering/data-caterer/issues) with tag `migration:yaml-unified`

## Script Options

Run `python3 migrate_yaml.py --help` to see all available options:

```
usage: migrate_yaml.py [-h] [--task-folder TASK_FOLDER] [--directory] [--dry-run] input [output]

positional arguments:
  input                 Input plan file or directory
  output                Output file or directory (optional)

optional arguments:
  -h, --help            show this help message and exit
  --task-folder TASK_FOLDER
                        Path to task folder (auto-detected if not provided)
  --directory, -d       Migrate entire directory
  --dry-run             Show what would be migrated without writing files
```

---

**Migration Tool Version:** 1.0
**Data Caterer Version:** v1.0+
**Last Updated:** 2026-01-12
