#!/usr/bin/env python3
"""
Data Caterer YAML Migration Script
===================================

Migrates legacy YAML plan files to the new unified YAML format (v1.0).

Usage:
    python3 migrate_yaml.py <input_plan.yaml> [output_unified.yaml]
    python3 migrate_yaml.py --directory <input_dir> <output_dir>
    python3 migrate_yaml.py --help

Features:
- Converts old plan/task structure to new unified format
- Merges connection configurations from task folder
- Preserves field options and validations
- Handles foreign keys and metadata
- Generates properly structured unified YAML

Author: Data Caterer Team
License: Apache 2.0
"""

import yaml
import sys
import os
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import OrderedDict


class YAMLMigrator:
    """Migrates legacy Data Caterer YAML to unified format."""

    def __init__(self):
        self.version = "1.0"
        self.warnings = []

    def migrate_plan(self, plan_path: str, task_folder: Optional[str] = None) -> Dict[str, Any]:
        """
        Migrate a legacy plan file to unified format.

        Args:
            plan_path: Path to the old plan YAML file
            task_folder: Optional path to task folder (auto-detected if not provided)

        Returns:
            Dictionary representing the unified YAML structure
        """
        with open(plan_path, 'r') as f:
            old_plan = yaml.safe_load(f)

        # Auto-detect task folder if not provided
        if task_folder is None:
            task_folder = self._detect_task_folder(plan_path)

        # Initialize unified structure
        unified = OrderedDict()
        unified['version'] = self.version
        unified['name'] = old_plan.get('name', 'migrated_plan')

        if 'description' in old_plan:
            unified['description'] = old_plan['description']

        # Migrate configuration if present
        if 'sinkOptions' in old_plan or 'validationConfig' in old_plan or 'flagsConfig' in old_plan:
            unified['configuration'] = self._migrate_configuration(old_plan)

        # Migrate tasks to dataSources
        if 'tasks' in old_plan:
            unified['dataSources'] = self._migrate_tasks(old_plan['tasks'], task_folder)

        # Migrate validations if present
        if 'validations' in old_plan:
            unified['validations'] = self._migrate_validations(old_plan['validations'])

        return unified

    def _detect_task_folder(self, plan_path: str) -> Optional[str]:
        """Auto-detect task folder based on plan location."""
        plan_dir = Path(plan_path).parent

        # Common task folder patterns
        candidates = [
            plan_dir / 'task',
            plan_dir.parent / 'task',
            plan_dir / '../task',
        ]

        for candidate in candidates:
            if candidate.exists() and candidate.is_dir():
                return str(candidate.resolve())

        return None

    def _migrate_configuration(self, old_plan: Dict) -> Dict[str, Any]:
        """Migrate plan configuration."""
        config = OrderedDict()

        # Migrate sink options
        if 'sinkOptions' in old_plan:
            sink_opts = old_plan['sinkOptions']
            if 'saveMode' in sink_opts:
                config['saveMode'] = sink_opts['saveMode']

        # Migrate flags
        if 'flagsConfig' in old_plan:
            flags = old_plan['flagsConfig']
            config['flags'] = OrderedDict()

            flag_mappings = {
                'enableCount': 'enableCount',
                'enableGenerateData': 'enableGenerateData',
                'enableRecordTracking': 'enableRecordTracking',
                'enableDeleteGeneratedRecords': 'enableDeleteGeneratedRecords',
                'enableGeneratePlanAndTasks': 'enableGeneratePlanAndTasks',
                'enableFailOnError': 'enableFailOnError',
                'enableUniqueCheck': 'enableUniqueCheck',
                'enableSinkMetadata': 'enableSinkMetadata',
                'enableSaveReports': 'enableSaveReports',
                'enableValidation': 'enableValidation',
                'enableGenerateValidations': 'enableGenerateValidations',
                'enableRecordTrackingValidation': 'enableRecordTrackingValidation',
                'enableDeleteRecordTrackingFiles': 'enableDeleteRecordTrackingFiles',
                'enableFastGeneration': 'enableFastGeneration',
            }

            for old_key, new_key in flag_mappings.items():
                if old_key in flags:
                    config['flags'][new_key] = flags[old_key]

        # Migrate validation config
        if 'validationConfig' in old_plan:
            val_config = old_plan['validationConfig']
            config['validation'] = OrderedDict()

            if 'numSampleErrorRecords' in val_config:
                config['validation']['numSampleErrorRecords'] = val_config['numSampleErrorRecords']
            if 'enableDeleteRecordTrackingFiles' in val_config:
                config['validation']['enableDeleteRecordTrackingFiles'] = val_config['enableDeleteRecordTrackingFiles']

        return config if config else None

    def _migrate_tasks(self, tasks: List[Dict], task_folder: Optional[str]) -> List[Dict]:
        """Migrate tasks to dataSources."""
        data_sources = []

        for task in tasks:
            if not task.get('enabled', True):
                self.warnings.append(f"Task '{task.get('name')}' is disabled - skipping")
                continue

            # Load task definition from file if needed
            task_config = self._load_task_config(task, task_folder)

            ds = OrderedDict()
            ds['name'] = task.get('dataSourceName', task.get('name', 'unknown'))

            # Migrate connection
            ds['connection'] = self._migrate_connection(task, task_config)

            # Migrate steps
            if 'steps' in task_config or 'steps' in task:
                steps = task_config.get('steps') or task.get('steps', [])
                ds['steps'] = self._migrate_steps(steps)

            data_sources.append(ds)

        return data_sources

    def _load_task_config(self, task: Dict, task_folder: Optional[str]) -> Dict:
        """Load full task configuration from task folder if available."""
        if task_folder is None:
            return task

        task_name = task.get('dataSourceName', task.get('name'))
        task_file = Path(task_folder) / f"{task_name}.yaml"

        if task_file.exists():
            with open(task_file, 'r') as f:
                task_config = yaml.safe_load(f)
                # Merge with inline task config
                return {**task_config, **task}

        return task

    def _migrate_connection(self, task: Dict, task_config: Dict) -> Dict[str, Any]:
        """Migrate connection configuration."""
        conn = OrderedDict()

        # Determine connection type
        conn_type = task_config.get('type') or task.get('type') or 'csv'
        conn['type'] = conn_type

        # Migrate connection options
        options = OrderedDict()

        # Common option mappings
        option_keys = ['url', 'path', 'format', 'driver', 'user', 'password',
                       'topic', 'bootstrapServers', 'schemaRegistryUrl',
                       'host', 'port', 'database', 'schema']

        for key in option_keys:
            if key in task_config:
                options[key] = task_config[key]
            elif key in task:
                options[key] = task[key]

        # Handle nested options
        if 'options' in task_config:
            options.update(task_config['options'])
        if 'options' in task:
            options.update(task['options'])

        if options:
            conn['options'] = options

        return conn

    def _migrate_steps(self, steps: List[Dict]) -> List[Dict]:
        """Migrate steps (tables, topics, files, etc.)."""
        migrated_steps = []

        for step in steps:
            if not step.get('enabled', True):
                continue

            s = OrderedDict()
            s['name'] = step.get('name', step.get('table', 'unknown'))

            # Migrate count configuration
            if 'count' in step:
                s['count'] = self._migrate_count(step['count'])

            # Migrate schema/fields
            if 'schema' in step:
                s['fields'] = self._migrate_schema(step['schema'])
            elif 'fields' in step:
                s['fields'] = self._migrate_fields(step['fields'])

            # Migrate options
            if 'options' in step:
                s['options'] = step['options']

            migrated_steps.append(s)

        return migrated_steps

    def _migrate_count(self, count: Any) -> Dict[str, Any]:
        """Migrate count configuration."""
        if isinstance(count, int):
            return {'records': count}

        c = OrderedDict()

        if isinstance(count, dict):
            if 'records' in count:
                c['records'] = count['records']
            elif 'perColumn' in count:
                c['perColumn'] = count['perColumn']

            # Migrate generated records config
            if 'recordsPerBatch' in count:
                c['recordsPerBatch'] = count['recordsPerBatch']
            if 'recordsPerSecond' in count:
                c['recordsPerSecond'] = count['recordsPerSecond']

        return c if c else {'records': 10}

    def _migrate_schema(self, schema: Dict) -> List[Dict]:
        """Migrate schema to fields."""
        if 'fields' in schema:
            return self._migrate_fields(schema['fields'])
        return []

    def _migrate_fields(self, fields: List[Dict]) -> List[Dict]:
        """Migrate field configurations."""
        migrated_fields = []

        for field in fields:
            f = OrderedDict()
            f['name'] = field.get('name', 'unknown')

            # Copy type if present
            if 'type' in field:
                f['type'] = field['type']

            # Migrate generator/options
            if 'generator' in field:
                f['options'] = field['generator'].get('options', {})
            elif 'options' in field:
                f['options'] = field['options']

            # Handle nested fields for complex types
            if 'fields' in field:
                f['fields'] = self._migrate_fields(field['fields'])

            migrated_fields.append(f)

        return migrated_fields

    def _migrate_validations(self, validations: List[Dict]) -> List[Dict]:
        """Migrate validation configurations."""
        migrated_validations = []

        for validation in validations:
            v = OrderedDict()

            # Copy basic validation properties
            if 'dataSourceName' in validation:
                v['dataSourceName'] = validation['dataSourceName']
            if 'options' in validation:
                v['options'] = validation['options']
            if 'waitCondition' in validation:
                v['waitCondition'] = validation['waitCondition']

            # Migrate validation rules
            if 'validations' in validation:
                v['validations'] = validation['validations']

            migrated_validations.append(v)

        return migrated_validations

    def write_unified_yaml(self, unified: Dict, output_path: str):
        """Write the unified YAML to file with proper formatting."""

        # Write with comments
        with open(output_path, 'w') as f:
            f.write(f"# Migrated to Data Caterer Unified YAML Format v{self.version}\n")
            f.write("# Generated by migrate_yaml.py\n\n")
            yaml.dump(self._convert_ordered_dict(unified), f,
                     default_flow_style=False, sort_keys=False, width=120)

        print(f"✓ Successfully migrated to: {output_path}")

        if self.warnings:
            print("\n⚠ Warnings:")
            for warning in self.warnings:
                print(f"  - {warning}")

    def _convert_ordered_dict(self, obj):
        """Convert OrderedDict to regular dict for cleaner YAML output."""
        if isinstance(obj, OrderedDict):
            return {k: self._convert_ordered_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_ordered_dict(item) for item in obj]
        else:
            return obj


def main():
    parser = argparse.ArgumentParser(
        description='Migrate Data Caterer YAML plans to unified format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate single file
  python3 migrate_yaml.py old_plan.yaml new_plan.yaml

  # Auto-detect output name
  python3 migrate_yaml.py old_plan.yaml

  # Specify task folder
  python3 migrate_yaml.py old_plan.yaml --task-folder ./tasks

  # Migrate directory
  python3 migrate_yaml.py --directory ./old_plans ./new_plans
        """
    )

    parser.add_argument('input', help='Input plan file or directory')
    parser.add_argument('output', nargs='?', help='Output file or directory (optional)')
    parser.add_argument('--task-folder', help='Path to task folder (auto-detected if not provided)')
    parser.add_argument('--directory', '-d', action='store_true',
                       help='Migrate entire directory')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be migrated without writing files')

    args = parser.parse_args()

    migrator = YAMLMigrator()

    if args.directory:
        # Migrate directory
        input_dir = Path(args.input)
        output_dir = Path(args.output) if args.output else input_dir / 'unified'

        if not input_dir.exists():
            print(f"Error: Input directory not found: {input_dir}")
            sys.exit(1)

        output_dir.mkdir(parents=True, exist_ok=True)

        yaml_files = list(input_dir.glob('*.yaml')) + list(input_dir.glob('*.yml'))

        for yaml_file in yaml_files:
            try:
                unified = migrator.migrate_plan(str(yaml_file), args.task_folder)
                output_file = output_dir / f"{yaml_file.stem}_unified.yaml"

                if not args.dry_run:
                    migrator.write_unified_yaml(unified, str(output_file))
                else:
                    print(f"Would migrate: {yaml_file} -> {output_file}")
            except Exception as e:
                print(f"✗ Error migrating {yaml_file}: {e}")

    else:
        # Migrate single file
        if not os.path.exists(args.input):
            print(f"Error: Input file not found: {args.input}")
            sys.exit(1)

        # Auto-generate output name if not provided
        if args.output is None:
            input_path = Path(args.input)
            args.output = str(input_path.parent / f"{input_path.stem}_unified.yaml")

        try:
            unified = migrator.migrate_plan(args.input, args.task_folder)

            if args.dry_run:
                print("Dry run - would generate:")
                print(yaml.dump(unified, default_flow_style=False, sort_keys=False))
            else:
                migrator.write_unified_yaml(unified, args.output)

        except Exception as e:
            print(f"✗ Error: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)


if __name__ == '__main__':
    main()
