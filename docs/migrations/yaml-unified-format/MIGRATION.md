# Migrating to Unified YAML Format

Data Caterer v1.0 introduces a new **Unified YAML Format** that simplifies configuration by combining plans, tasks, and connections into a single file.

## Why Migrate?

The new unified format provides:

- ✅ **Single-file configuration** - No need for separate plan and task files
- ✅ **Simpler structure** - More intuitive hierarchy and naming
- ✅ **Better validation** - JSON schema support for IDE autocomplete
- ✅ **Easier maintenance** - All configuration in one place
- ✅ **Improved readability** - Cleaner, more concise syntax

## Migration Tool

We provide an automated migration script (`migrate_yaml.py`) that converts your existing YAML plans to the new format.

### Requirements

- Python 3.6 or higher
- PyYAML library (install with `pip install pyyaml`)

### Basic Usage

```bash
# Migrate a single plan file
python3 migrate_yaml.py old_plan.yaml new_plan.yaml

# Auto-generate output filename
python3 migrate_yaml.py old_plan.yaml
# Creates: old_plan_unified.yaml

# Migrate with explicit task folder
python3 migrate_yaml.py old_plan.yaml --task-folder ./tasks

# Dry run (preview without writing)
python3 migrate_yaml.py old_plan.yaml --dry-run
```

### Batch Migration

Migrate entire directories:

```bash
# Migrate all YAML files in a directory
python3 migrate_yaml.py --directory ./old_plans ./new_plans
```

## Format Comparison

### Old Format (Legacy)

**Plan file: `plan.yaml`**
```yaml
name: "my_plan"
description: "Generate test data"
tasks:
  - name: "json_task"
    dataSourceName: "json_output"
    enabled: true

flagsConfig:
  enableGenerateData: true
  enableCount: true
```

**Task file: `task/json_output.yaml`**
```yaml
name: "json_output"
type: "json"
path: "/tmp/data/output"
steps:
  - name: "users"
    count:
      records: 100
    schema:
      fields:
        - name: "id"
          generator:
            type: "regex"
            options:
              regex: "USR[0-9]{6}"
```

### New Format (Unified)

**Single file: `plan_unified.yaml`**
```yaml
version: "1.0"
name: "my_plan"
description: "Generate test data"

configuration:
  flags:
    enableGenerateData: true
    enableCount: true

dataSources:
  - name: "json_output"
    connection:
      type: "json"
      options:
        path: "/tmp/data/output"
    steps:
      - name: "users"
        count:
          records: 100
        fields:
          - name: "id"
            options:
              regex: "USR[0-9]{6}"
```

## Key Changes

### 1. Version Field

All unified YAML files must start with:
```yaml
version: "1.0"
```

### 2. Tasks → Data Sources

**Old**: `tasks` array with references to separate task files
**New**: `dataSources` array with inline connection details

```yaml
# Old
tasks:
  - name: "my_task"
    dataSourceName: "postgres_db"

# New
dataSources:
  - name: "postgres_db"
    connection:
      type: "postgres"
      options:
        url: "jdbc:postgresql://localhost:5432/mydb"
```

### 3. Schema → Fields

**Old**: `schema.fields` with `generator` wrapper
**New**: Direct `fields` with `options`

```yaml
# Old
schema:
  fields:
    - name: "email"
      generator:
        options:
          expression: "#{Internet.emailAddress}"

# New
fields:
  - name: "email"
    options:
      expression: "#{Internet.emailAddress}"
```

### 4. Configuration Consolidation

All configuration now under `configuration` key:

```yaml
configuration:
  flags:
    enableGenerateData: true
    enableFastGeneration: true
  validation:
    numSampleErrorRecords: 5
  saveMode: "append"
```

## Manual Migration Steps

If you prefer to migrate manually:

1. **Create new file** with `version: "1.0"`
2. **Copy plan metadata**: `name`, `description`
3. **Move configuration**: Combine `flagsConfig`, `sinkOptions`, `validationConfig` under `configuration`
4. **Merge tasks**: For each task:
   - Load the corresponding task file from `task/` folder
   - Create a `dataSource` entry
   - Copy connection info to `connection` block
   - Copy `steps` with field definitions
5. **Rename keys**:
   - `schema.fields` → `fields`
   - `generator.options` → `options`
   - Remove `enabled` flags (disabled items should be removed)

## Testing Your Migration

After migrating, verify the unified YAML works:

```bash
# Set the plan file path
export PLAN_FILE_PATH=/path/to/unified_plan.yaml

# Run Data Caterer
./gradlew :app:run
```

Or use the manual test runner:

```bash
YAML_FILE=/path/to/unified_plan.yaml ./gradlew :app:manualTest --tests "*YamlFileManualTest"
```

## Common Issues

### Issue: Missing connection details
**Solution**: The migration script auto-detects task folders. If your tasks are in a non-standard location, use `--task-folder`:
```bash
python3 migrate_yaml.py plan.yaml --task-folder /path/to/tasks
```

### Issue: Unsupported field options
**Solution**: Some advanced options may need manual adjustment. Check the [unified YAML schema](https://github.com/data-catering/data-caterer/blob/176baa3762ccdca05b024ccd2efcd6335359e713/misc/schema/unified-config-schema.json) for supported options.

### Issue: Validation references
**Solution**: Update validation `dataSourceName` to match the new data source names in the unified format.

## Getting Help

- **Documentation**: See [Configuration Guide](../../docs/configuration.md)
- **Examples**: Check `misc/schema/examples/` for unified YAML examples
- **Issues**: Report migration problems at [GitHub Issues](https://github.com/data-catering/data-caterer/issues)

## Backward Compatibility

The old format is still supported in Data Caterer v1.0, but we recommend migrating to the unified format for:
- Better IDE support
- Simplified configuration
- Future-proof your setup

The legacy format will be deprecated in a future release.
