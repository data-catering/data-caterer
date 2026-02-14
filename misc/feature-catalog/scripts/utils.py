"""
Utility functions for feature extraction from source code.

Generic utilities designed to parse different source formats and produce
standardized feature metadata. Can be adapted for other projects by
replacing language-specific parsers.
"""

import json
import os
import re
from pathlib import Path
from typing import Any


# Project root - adjust for your project
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
FEATURE_CATALOG_DIR = Path(__file__).parent.parent


def load_json(path: Path) -> dict:
    """Load a JSON file."""
    with open(path, 'r') as f:
        return json.load(f)


def save_json(data: dict, path: Path, indent: int = 2):
    """Save data as formatted JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)
    print(f"Saved: {path}")


def read_file(path: Path) -> str:
    """Read a text file."""
    with open(path, 'r') as f:
        return f.read()


def parse_scala_lazy_vals(content: str) -> list[dict]:
    """
    Parse Scala lazy val declarations from source code.

    Returns list of dicts with:
      - name: Scala variable name
      - value: The string/numeric value
      - comment: Any preceding comment
    """
    results = []
    lines = content.split('\n')
    current_comment = ""

    for i, line in enumerate(lines):
        stripped = line.strip()

        # Track comments
        if stripped.startswith('//'):
            current_comment = stripped.lstrip('/ ').strip()
            continue

        # Match lazy val declarations with string values
        match = re.match(
            r'lazy\s+val\s+(\w+)\s*[:=]\s*(?:String\s*=\s*)?["\'](.+?)["\']',
            stripped
        )
        if match:
            results.append({
                'name': match.group(1),
                'value': match.group(2),
                'comment': current_comment,
                'line': i + 1,
            })
            current_comment = ""
            continue

        # Match lazy val with numeric values
        match = re.match(
            r'lazy\s+val\s+(\w+)\s*[:=]\s*(?:\w+\s*=\s*)?(-?[\d.]+)L?',
            stripped
        )
        if match:
            val = match.group(2)
            results.append({
                'name': match.group(1),
                'value': float(val) if '.' in val else int(val),
                'comment': current_comment,
                'line': i + 1,
            })
            current_comment = ""
            continue

        # Match lazy val with boolean values
        match = re.match(
            r'lazy\s+val\s+(\w+)\s*[:=]\s*(?:\w+\s*=\s*)?(true|false)',
            stripped
        )
        if match:
            results.append({
                'name': match.group(1),
                'value': match.group(2) == 'true',
                'comment': current_comment,
                'line': i + 1,
            })
            current_comment = ""
            continue

        # Reset comment if line is not a comment and not a lazy val
        if stripped and not stripped.startswith('//'):
            current_comment = ""

    return results


def parse_scala_case_class(content: str, class_name: str) -> list[dict]:
    """
    Parse a Scala case class to extract fields with types and defaults.

    Returns list of dicts with:
      - name: Field name
      - type: Scala type
      - default: Default value (string representation)
    """
    # Find the case class definition
    pattern = rf'case\s+class\s+{class_name}\s*\((.*?)\)'
    match = re.search(pattern, content, re.DOTALL)
    if not match:
        return []

    body = match.group(1)
    fields = []

    # Split by commas that are not inside brackets
    depth = 0
    current = ""
    for char in body:
        if char in '([{':
            depth += 1
        elif char in ')]}':
            depth -= 1
        elif char == ',' and depth == 0:
            fields.append(current.strip())
            current = ""
            continue
        current += char
    if current.strip():
        fields.append(current.strip())

    results = []
    for field in fields:
        # Parse field: name: Type = default
        match = re.match(r'(\w+)\s*:\s*(\S+(?:\[.*?\])?)\s*(?:=\s*(.+))?', field.strip())
        if match:
            results.append({
                'name': match.group(1),
                'type': match.group(2),
                'default': match.group(3).strip() if match.group(3) else None,
            })

    return results


def make_feature_id(*parts: str) -> str:
    """Create a standardized feature ID from parts."""
    return '.'.join(
        re.sub(r'[^a-z0-9_]', '_', part.lower().strip())
        for part in parts
        if part
    )


def make_feature(
    id: str,
    name: str,
    category: str,
    status: str,
    description: str,
    subcategory: str = "",
    configuration: list[dict] | None = None,
    examples: list[dict] | None = None,
    tags: list[str] | None = None,
    related_features: list[str] | None = None,
    source_files: list[dict] | None = None,
    dependencies: dict | None = None,
    use_cases: list[str] | None = None,
    limitations: list[str] | None = None,
    performance_notes: list[str] | None = None,
) -> dict:
    """Create a standardized feature dict."""
    feature = {
        'id': id,
        'name': name,
        'category': category,
        'status': status,
        'description': description,
    }
    if subcategory:
        feature['subcategory'] = subcategory
    if configuration:
        feature['configuration'] = configuration
    if examples:
        feature['examples'] = examples
    if tags:
        feature['tags'] = tags
    if related_features:
        feature['relatedFeatures'] = related_features
    if source_files:
        feature['sourceFiles'] = source_files
    if dependencies:
        feature['dependencies'] = dependencies
    if use_cases:
        feature['useCases'] = use_cases
    if limitations:
        feature['limitations'] = limitations
    if performance_notes:
        feature['performanceNotes'] = performance_notes
    return feature


def make_config_option(
    name: str,
    type: str,
    description: str,
    required: bool = False,
    default: Any = None,
    scope: str = "global",
    valid_values: list | None = None,
    range: dict | None = None,
    yaml_path: str = "",
    env_var: str = "",
    scala_constant: str = "",
) -> dict:
    """Create a standardized configuration option dict."""
    opt = {
        'name': name,
        'type': type,
        'description': description,
        'required': required,
        'scope': scope,
    }
    if default is not None:
        opt['default'] = default
    if valid_values:
        opt['validValues'] = valid_values
    if range:
        opt['range'] = range
    if yaml_path:
        opt['yamlPath'] = yaml_path
    if env_var:
        opt['envVar'] = env_var
    if scala_constant:
        opt['scalaConstant'] = scala_constant
    return opt


def make_example(format: str, code: str, title: str = "", description: str = "") -> dict:
    """Create a standardized example dict."""
    ex = {'format': format, 'code': code}
    if title:
        ex['title'] = title
    if description:
        ex['description'] = description
    return ex
