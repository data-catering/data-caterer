# Data Caterer JSON Schema

This folder contains the JSON Schema for validating Data Caterer unified YAML configuration files.

## Schema File

- **`unified-config-schema.json`** - JSON Schema for the unified YAML configuration format

## IDE Integration

Adding JSON Schema support to your IDE provides:
- **Auto-completion** for all configuration options
- **Inline documentation** with descriptions and examples
- **Validation** to catch errors before running
- **Hover information** for field descriptions

### VS Code

#### Option 1: Per-file schema reference (recommended for single files)

Add a schema reference comment at the top of your YAML file:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/data-catering/data-caterer/main/misc/schema/unified-config-schema.json

version: "1.0"
name: "my_plan"
dataSources:
  # ... your configuration
```

#### Option 2: Workspace settings (recommended for projects)

Create or update `.vscode/settings.json` in your project:

```json
{
  "yaml.schemas": {
    "https://raw.githubusercontent.com/data-catering/data-caterer/main/misc/schema/unified-config-schema.json": [
      "**/data-caterer/**/*.yaml",
      "**/data-caterer/**/*.yml"
    ]
  }
}
```

Or for local schema:

```json
{
  "yaml.schemas": {
    "./misc/schema/unified-config-schema.json": [
      "**/*-unified.yaml",
      "**/*-plan.yaml"
    ]
  }
}
```

#### Option 3: User settings (global)

Open VS Code settings (`Cmd+,` on Mac, `Ctrl+,` on Windows/Linux), search for "yaml.schemas", and add:

```json
{
  "yaml.schemas": {
    "https://raw.githubusercontent.com/data-catering/data-caterer/main/misc/schema/unified-config-schema.json": "**/data-caterer/**/*.yaml"
  }
}
```

**Required Extension**: [YAML by Red Hat](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml)

### JetBrains IDEs (IntelliJ IDEA, WebStorm, PyCharm, etc.)

#### Option 1: Per-file schema reference

Add a schema reference comment at the top of your YAML file:

```yaml
# $schema: https://raw.githubusercontent.com/data-catering/data-caterer/main/misc/schema/unified-config-schema.json

version: "1.0"
name: "my_plan"
dataSources:
  # ... your configuration
```

#### Option 2: Settings configuration

1. Go to **Settings/Preferences** → **Languages & Frameworks** → **Schemas and DTDs** → **JSON Schema Mappings**
2. Click **+** to add a new mapping
3. Enter:
   - **Name**: `Data Caterer Unified Config`
   - **Schema file or URL**: `https://raw.githubusercontent.com/data-catering/data-caterer/main/misc/schema/unified-config-schema.json`
   - **Schema version**: `JSON Schema version 7`
4. Add file path patterns:
   - `**/data-caterer/**/*.yaml`
   - `**/*-unified.yaml`
   - `**/*-plan.yaml`

### Vim/Neovim

#### With coc.nvim and coc-yaml

Add to your `coc-settings.json`:

```json
{
  "yaml.schemas": {
    "https://raw.githubusercontent.com/data-catering/data-caterer/main/misc/schema/unified-config-schema.json": "**/data-caterer/**/*.yaml"
  }
}
```

#### With nvim-lspconfig and yaml-language-server

Add to your Neovim config:

```lua
require('lspconfig').yamlls.setup {
  settings = {
    yaml = {
      schemas = {
        ["https://raw.githubusercontent.com/data-catering/data-caterer/main/misc/schema/unified-config-schema.json"] = "**/data-caterer/**/*.yaml"
      }
    }
  }
}
```

### Sublime Text

Install the [LSP](https://packagecontrol.io/packages/LSP) and [LSP-yaml](https://packagecontrol.io/packages/LSP-yaml) packages, then add to your LSP settings:

```json
{
  "clients": {
    "yaml": {
      "settings": {
        "yaml.schemas": {
          "https://raw.githubusercontent.com/data-catering/data-caterer/main/misc/schema/unified-config-schema.json": "**/data-caterer/**/*.yaml"
        }
      }
    }
  }
}
```

### Command-Line Validation

#### Using ajv-cli

```bash
# Install ajv-cli
npm install -g ajv-cli

# Validate a YAML file
ajv validate -s misc/schema/unified-config-schema.json -d your-config.yaml --spec=draft7
```

#### Using check-jsonschema

```bash
# Install check-jsonschema
pip install check-jsonschema

# Validate a YAML file
check-jsonschema --schemafile misc/schema/unified-config-schema.json your-config.yaml
```

#### Using yajsv

```bash
# Install yajsv (Go-based)
go install github.com/neilpa/yajsv@latest

# Validate a YAML file
yajsv -s misc/schema/unified-config-schema.json your-config.yaml
```

## Local Development

If you've cloned the Data Caterer repository, you can reference the schema locally:

```yaml
# yaml-language-server: $schema=./misc/schema/unified-config-schema.json
```

Or with a relative path from your YAML file location.

## Schema Features

The schema covers:

- **Root configuration**: `version`, `name`, `description`, `runId`
- **Runtime config**: flags, folders, generation, metadata, validation, alerts, Spark settings
- **Data sources**: inline connections and step definitions
- **Steps**: tables, topics, files with count and field definitions
- **Fields**: 12+ data types with generation options (regex, expression, oneOf, min/max, etc.)
- **Validations**: 30+ validation types (unique, matches, between, etc.)
- **Foreign keys**: relationships with cardinality and nullability configuration
- **Sink options**: seed and locale for reproducible generation

## Troubleshooting

### Schema not loading

1. Ensure your IDE has YAML language support installed
2. Check that the schema URL is accessible
3. Try using a local copy of the schema file
4. Verify the file pattern matches your configuration files

### Validation errors

1. Check that required fields are present
2. Verify enum values are correctly spelled (case-sensitive)
3. Ensure numeric values are the correct type (integer vs. number)
4. Check that nested objects have the correct structure

### Auto-completion not working

1. Ensure the schema is properly linked to your file
2. Try reloading the IDE window
3. Check that the YAML extension is enabled and updated
