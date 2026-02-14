#!/usr/bin/env python3
"""
Generate Markdown documentation from the features.json catalog.

Produces:
    docs/index.md - Main overview and navigation
    docs/categories/<category>.md - Per-category feature documentation

Usage:
    python scripts/generate_markdown.py
"""

import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import FEATURE_CATALOG_DIR, load_json


DOCS_DIR = FEATURE_CATALOG_DIR / "docs"
CATEGORIES_DIR = DOCS_DIR / "categories"

# Category display order and descriptions
CATEGORY_META = {
    'connectors': {
        'title': 'Data Source Connectors',
        'icon': '',
        'description': 'Data Caterer supports connecting to databases, file systems, messaging systems, and HTTP APIs for reading and writing test data.',
    },
    'generation': {
        'title': 'Data Generation',
        'icon': '',
        'description': 'Comprehensive data generation capabilities including regex patterns, faker expressions, SQL computations, and field-level configuration options.',
    },
    'validation': {
        'title': 'Data Validation',
        'icon': '',
        'description': 'Over 30 validation types for verifying generated data quality, schema compliance, statistical properties, and cross-source consistency.',
    },
    'configuration': {
        'title': 'Configuration',
        'icon': '',
        'description': 'Runtime configuration for controlling generation behavior, validation, performance tuning, alerts, and output paths.',
    },
    'advanced': {
        'title': 'Advanced Features',
        'icon': '',
        'description': 'Foreign key relationships, streaming load patterns, custom transformations, metadata-driven generation, and more.',
    },
    'metadata': {
        'title': 'Metadata Integration',
        'icon': '',
        'description': 'Import schemas and metadata from external catalogs, registries, and standards to drive automatic data generation.',
    },
    'ui_api': {
        'title': 'UI and API',
        'icon': '',
        'description': 'Web-based user interface for visual plan creation, execution management, and result viewing.',
    },
}

SUBCATEGORY_TITLES = {
    'databases': 'Databases',
    'files': 'File Formats',
    'messaging': 'Messaging Systems',
    'http': 'HTTP/REST',
    'generators': 'Generator Types',
    'data_types': 'Data Types',
    'field_options': 'Field Options',
    'labels': 'Field Labels (Auto-Detection)',
    'field_validations': 'Field-Level Validations',
    'statistical_validations': 'Statistical Validations',
    'expression_validations': 'Expression Validations',
    'aggregation_validations': 'Aggregation Validations',
    'cross_source_validations': 'Cross-Source Validations',
    'schema_validations': 'Schema Validations',
    'wait_conditions': 'Wait Conditions',
    'flags': 'Feature Flags',
    'folders': 'Folder Paths',
    'generation': 'Generation Settings',
    'metadata': 'Metadata Settings',
    'streaming': 'Streaming Settings',
    'alerts': 'Alert Settings',
    'validation_runtime': 'Validation Runtime',
    'runtime': 'Spark Runtime',
    'sink': 'Sink Options',
    'referential_integrity': 'Foreign Key Relationships',
    'count': 'Record Count',
    'transformation': 'Transformation',
    'step_options': 'Step Options',
    'interfaces': 'Interfaces',
    'configuration': 'Configuration',
    'reference': 'Reference Mode',
    'sources': 'Metadata Sources',
    'web_ui': 'Web UI Features',
}


def generate_config_table(config_options: list[dict]) -> str:
    """Generate a Markdown table for configuration options."""
    if not config_options:
        return ""

    lines = [
        "",
        "| Option | Type | Required | Default | Description |",
        "|--------|------|----------|---------|-------------|",
    ]
    for opt in config_options:
        name = f"`{opt['name']}`"
        type_ = opt.get('type', '')
        required = 'Yes' if opt.get('required') else 'No'
        default = opt.get('default', '-')
        if default is None or default == '':
            default = '-'
        elif isinstance(default, bool):
            default = str(default).lower()
        desc = opt.get('description', '')

        # Add valid values
        valid = opt.get('validValues', [])
        if valid:
            desc += f" Values: {', '.join(f'`{v}`' for v in valid)}"

        # Add YAML path
        yaml_path = opt.get('yamlPath', '')
        if yaml_path:
            desc += f" YAML: `{yaml_path}`"

        lines.append(f"| {name} | {type_} | {required} | `{default}` | {desc} |")

    lines.append("")
    return "\n".join(lines)


def generate_examples(examples: list[dict]) -> str:
    """Generate Markdown code blocks for examples."""
    if not examples:
        return ""

    parts = []
    for ex in examples:
        title = ex.get('title', '')
        desc = ex.get('description', '')
        fmt = ex.get('format', 'yaml')
        code = ex.get('code', '')

        if title:
            parts.append(f"\n**{title}**:")
        elif desc:
            parts.append(f"\n**{desc}**:")

        parts.append(f"```{fmt}")
        parts.append(code.strip())
        parts.append("```")

    return "\n".join(parts)


def generate_feature_section(feature: dict) -> str:
    """Generate Markdown for a single feature."""
    lines = []

    # Feature header
    lines.append(f"### {feature['name']}")
    lines.append("")
    lines.append(f"**ID**: `{feature['id']}`")
    lines.append(f"**Status**: {feature.get('status', 'stable').title()}")
    lines.append("")

    # Description
    lines.append(feature.get('description', ''))
    lines.append("")

    # Use cases
    use_cases = feature.get('useCases', [])
    if use_cases:
        lines.append("**Use Cases**:")
        for uc in use_cases:
            lines.append(f"- {uc}")
        lines.append("")

    # Configuration table
    config = feature.get('configuration', [])
    if config:
        lines.append("**Configuration**:")
        lines.append(generate_config_table(config))

    # Examples
    examples = feature.get('examples', [])
    if examples:
        lines.append("**Examples**:")
        lines.append(generate_examples(examples))
        lines.append("")

    # Source files
    sources = feature.get('sourceFiles', [])
    if sources:
        lines.append("**Source Files**:")
        for src in sources:
            role = src.get('role', '')
            lines.append(f"- `{src['path']}` ({role})")
        lines.append("")

    # Related features
    related = feature.get('relatedFeatures', [])
    if related:
        lines.append("**Related Features**:")
        for r in related:
            lines.append(f"- `{r}`")
        lines.append("")

    # Tags
    tags = feature.get('tags', [])
    if tags:
        lines.append(f"**Tags**: {', '.join(f'`{t}`' for t in tags)}")
        lines.append("")

    # Performance notes
    perf = feature.get('performanceNotes', [])
    if perf:
        lines.append("**Performance Notes**:")
        for p in perf:
            lines.append(f"- {p}")
        lines.append("")

    # Limitations
    limits = feature.get('limitations', [])
    if limits:
        lines.append("**Known Limitations**:")
        for l in limits:
            lines.append(f"- {l}")
        lines.append("")

    lines.append("---")
    lines.append("")

    return "\n".join(lines)


def generate_category_page(category_id: str, features: list[dict]) -> str:
    """Generate a full Markdown page for a category."""
    meta = CATEGORY_META.get(category_id, {})
    title = meta.get('title', category_id.replace('_', ' ').title())
    description = meta.get('description', '')

    lines = []
    lines.append(f"# {title}")
    lines.append("")
    if description:
        lines.append(description)
        lines.append("")

    lines.append(f"**{len(features)} features** in this category.")
    lines.append("")

    # Table of contents
    lines.append("## Table of Contents")
    lines.append("")

    # Group by subcategory
    by_sub = defaultdict(list)
    for f in features:
        sub = f.get('subcategory', 'other')
        by_sub[sub].append(f)

    for sub, sub_features in by_sub.items():
        sub_title = SUBCATEGORY_TITLES.get(sub, sub.replace('_', ' ').title())
        anchor = sub.lower().replace(' ', '-').replace('_', '-')
        lines.append(f"- [{sub_title}](#{anchor}) ({len(sub_features)} features)")

    lines.append("")

    # Feature sections grouped by subcategory
    for sub, sub_features in by_sub.items():
        sub_title = SUBCATEGORY_TITLES.get(sub, sub.replace('_', ' ').title())
        lines.append(f"## {sub_title}")
        lines.append("")

        for f in sub_features:
            lines.append(generate_feature_section(f))

    return "\n".join(lines)


def generate_index(catalog: dict) -> str:
    """Generate the main index.md page."""
    project = catalog.get('project', {})
    categories = catalog.get('categories', [])
    features = catalog.get('features', [])

    lines = []
    lines.append(f"# {project.get('name', 'Project')} Feature Catalog")
    lines.append("")
    lines.append(f"Complete reference for all features in {project.get('name', 'the project')}.")
    lines.append("")
    lines.append(f"- **Version**: {project.get('version', 'N/A')}")
    lines.append(f"- **Total Features**: {len(features)}")
    lines.append(f"- **Last Updated**: {project.get('lastUpdated', 'N/A')}")
    lines.append(f"- **Repository**: {project.get('repository', 'N/A')}")
    lines.append("")

    # Categories overview
    lines.append("## Categories")
    lines.append("")

    # Order categories
    order = ['connectors', 'generation', 'validation', 'configuration', 'advanced', 'metadata', 'ui_api']
    sorted_cats = sorted(categories, key=lambda c: order.index(c['id']) if c['id'] in order else 99)

    for cat in sorted_cats:
        meta = CATEGORY_META.get(cat['id'], {})
        title = meta.get('title', cat['name'])
        desc = meta.get('description', '')
        count = cat.get('featureCount', 0)
        filename = cat['id'].replace('_', '-')
        lines.append(f"### [{title}](categories/{filename}.md)")
        lines.append(f"{count} features | {desc}")
        lines.append("")

    # Quick reference by status
    lines.append("## Feature Summary")
    lines.append("")
    lines.append("| Category | Features | Description |")
    lines.append("|----------|----------|-------------|")
    for cat in sorted_cats:
        meta = CATEGORY_META.get(cat['id'], {})
        title = meta.get('title', cat['name'])
        count = cat.get('featureCount', 0)
        filename = cat['id'].replace('_', '-')
        short_desc = meta.get('description', '')[:80]
        if len(meta.get('description', '')) > 80:
            short_desc += '...'
        lines.append(f"| [{title}](categories/{filename}.md) | {count} | {short_desc} |")
    lines.append("")

    total = sum(c.get('featureCount', 0) for c in sorted_cats)
    lines.append(f"**Total: {total} features**")
    lines.append("")

    # All features alphabetical index
    lines.append("## All Features (Alphabetical)")
    lines.append("")

    sorted_features = sorted(features, key=lambda f: f['name'].lower())
    for f in sorted_features:
        cat_id = f['category'].replace('_', '-')
        sub = f.get('subcategory', '')
        anchor = f['name'].lower().replace(' ', '-').replace('/', '-').replace('(', '').replace(')', '')
        lines.append(f"- [{f['name']}](categories/{cat_id}.md#{anchor}) - `{f['id']}`")

    lines.append("")

    return "\n".join(lines)


def main():
    print("Loading features.json...")
    catalog_path = FEATURE_CATALOG_DIR / "features.json"
    catalog = load_json(catalog_path)

    features = catalog.get('features', [])
    print(f"Loaded {len(features)} features")

    # Create directories
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    CATEGORIES_DIR.mkdir(parents=True, exist_ok=True)

    # Generate index
    print("Generating index.md...")
    index_content = generate_index(catalog)
    (DOCS_DIR / "index.md").write_text(index_content)

    # Group features by category
    by_category = defaultdict(list)
    for f in features:
        by_category[f['category']].append(f)

    # Generate category pages
    for cat_id, cat_features in by_category.items():
        filename = cat_id.replace('_', '-') + '.md'
        print(f"Generating categories/{filename} ({len(cat_features)} features)...")
        content = generate_category_page(cat_id, cat_features)
        (CATEGORIES_DIR / filename).write_text(content)

    print(f"\nDocumentation generated in: {DOCS_DIR}")
    print(f"  - index.md")
    for cat_id in by_category:
        filename = cat_id.replace('_', '-') + '.md'
        print(f"  - categories/{filename}")


if __name__ == '__main__':
    main()
