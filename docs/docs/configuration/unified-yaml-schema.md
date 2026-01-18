---
title: "Unified YAML Schema"
description: "Validate unified YAML configurations and enable IDE autocomplete."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Unified YAML Schema

Data Caterer provides a JSON Schema for the unified YAML format to enable IDE autocomplete and validation.

## Schema File

- **Schema URL**: `https://raw.githubusercontent.com/data-catering/data-caterer/main/misc/schema/unified-config-schema.json`
- **Examples**: [Unified YAML examples](https://github.com/data-catering/data-caterer/tree/main/misc/schema/examples)

## IDE Autocomplete

Add a schema reference at the top of your YAML file:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/data-catering/data-caterer/main/misc/schema/unified-config-schema.json
version: "1.0"
name: "my_plan"
dataSources: []
```

## CLI Validation

Validate a file locally using `check-jsonschema`:

```bash
pip install check-jsonschema
check-jsonschema --schemafile misc/schema/unified-config-schema.json your-config.yaml
```

## Related Docs

- [Configuration guide](../configuration.md)
- [Unified YAML migration guide](../../migrations/yaml-unified-format/MIGRATION.md)
