# Feature Catalog

Standardized documentation of all Data Caterer features, extracted semi-automatically from source code.

## Quick Start

```bash
# Generate features.json from source code
python3 scripts/extract_features.py

# Generate Markdown documentation from features.json
python3 scripts/generate_markdown.py
```

## Structure

```
feature-catalog/
├── README.md                              # This file
├── features.json                          # Master feature catalog (generated)
├── schema/
│   └── feature-metadata-schema.json       # JSON Schema for feature metadata
├── scripts/
│   ├── utils.py                           # Shared utilities
│   ├── extract_features.py                # Extract features from source code
│   └── generate_markdown.py               # Generate Markdown from features.json
└── docs/
    ├── index.md                           # Main overview and navigation
    └── categories/
        ├── connectors.md                  # Data source connectors (16)
        ├── generation.md                  # Data generation features (55)
        ├── validation.md                  # Validation features (42)
        ├── configuration.md               # Configuration options (29)
        ├── advanced.md                    # Advanced features (11)
        ├── metadata.md                    # Metadata integration (10)
        └── ui-api.md                      # UI and API features (6)
```

## Features Overview

| Category | Count | Description |
|----------|-------|-------------|
| Data Source Connectors | 16 | Databases, files, messaging, HTTP |
| Data Generation | 55 | Regex, faker, SQL, field options, types |
| Data Validation | 42 | Field, statistical, expression, cross-source |
| Configuration | 29 | Flags, folders, runtime, alerts |
| Advanced | 11 | Foreign keys, streaming, transformations |
| Metadata Integration | 10 | OpenMetadata, Great Expectations, etc. |
| UI/API | 6 | Web UI features |
| **Total** | **169** | |

## How It Works

### Extraction Sources

The extraction script parses these source files:

1. **Constants.scala** - All configuration constants, validation types, data source types
2. **ConfigModels.scala** - Configuration case classes with defaults
3. **Unified config schema** - YAML schema with all supported properties
4. **YAML examples** - Real-world usage patterns

### Feature Metadata Schema

Each feature has:
- **id**: Unique identifier (dot notation, e.g., `generation.field.regex`)
- **name**: Human-readable name
- **category**: One of: connectors, generation, validation, configuration, advanced, metadata, ui_api
- **status**: stable, experimental, deprecated, planned
- **description**: What the feature does
- **configuration**: Array of config options (name, type, default, scope, YAML path)
- **examples**: YAML/Scala code examples
- **tags**: Searchable tags
- **sourceFiles**: Implementation locations
- **relatedFeatures**: Links to related features

## Updating the Catalog

When adding new features to Data Caterer:

1. Add constants to `Constants.scala` / config to `ConfigModels.scala`
2. Run `python3 scripts/extract_features.py` to regenerate `features.json`
3. Run `python3 scripts/generate_markdown.py` to regenerate docs
4. Review and commit

For features not captured by automated extraction, manually add them to `extract_features.py` in the appropriate extraction function.

## Adapting for Other Projects

This system is designed to be reusable:

1. **Replace extraction sources**: Modify `extract_features.py` to parse your config files
2. **Customize schema**: Extend `feature-metadata-schema.json` with custom fields
3. **Adjust categories**: Update `CATEGORY_META` in `generate_markdown.py`
4. **Change output**: Swap `generate_markdown.py` for your preferred format

The core `utils.py` provides generic utilities for:
- Parsing Scala lazy vals and case classes
- Creating standardized feature/config/example objects
- File I/O helpers

## Requirements

- Python 3.10+
- No external dependencies (uses only stdlib)
