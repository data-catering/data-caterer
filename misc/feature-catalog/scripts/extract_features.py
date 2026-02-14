#!/usr/bin/env python3
"""
Feature extraction script for Data Caterer.

Parses source code (Constants.scala, ConfigModels.scala), JSON schema,
and YAML examples to produce a comprehensive features.json catalog.

Usage:
    python scripts/extract_features.py

Output:
    ../features.json
"""

import sys
from pathlib import Path

# Add scripts dir to path
sys.path.insert(0, str(Path(__file__).parent))

from utils import (
    PROJECT_ROOT, FEATURE_CATALOG_DIR, read_file, save_json,
    parse_scala_lazy_vals, parse_scala_case_class, load_json,
    make_feature, make_config_option, make_example, make_feature_id,
)


# Source file paths
CONSTANTS_PATH = PROJECT_ROOT / "api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala"
CONFIG_MODELS_PATH = PROJECT_ROOT / "api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala"
UNIFIED_SCHEMA_PATH = PROJECT_ROOT / "misc/schema/unified-config-schema.json"
EXAMPLES_DIR = PROJECT_ROOT / "misc/schema/examples"


def extract_data_source_features(vals: list[dict]) -> list[dict]:
    """Extract data source connector features from Constants.scala."""
    features = []

    connectors = {
        # Databases
        'postgres': {
            'name': 'PostgreSQL Connector',
            'subcategory': 'databases',
            'description': 'Connect to PostgreSQL databases for reading and writing data. Supports table-level configuration, custom queries, and JDBC options.',
            'tags': ['database', 'jdbc', 'relational', 'sql'],
            'config': [
                make_config_option('url', 'string', 'JDBC connection URL', required=True, scope='datasource', yaml_path='dataSources[].connection.options.url'),
                make_config_option('user', 'string', 'Database username', scope='datasource', yaml_path='dataSources[].connection.options.user'),
                make_config_option('password', 'string', 'Database password', scope='datasource', yaml_path='dataSources[].connection.options.password'),
                make_config_option('driver', 'string', 'JDBC driver class', default='org.postgresql.Driver', scope='datasource'),
                make_config_option('dbtable', 'string', 'Target table (schema.table)', scope='step', yaml_path='dataSources[].steps[].options.dbtable'),
                make_config_option('query', 'string', 'Custom SQL query for reading', scope='step'),
            ],
            'examples': [
                make_example('yaml', '''dataSources:
  - name: my_postgres
    connection:
      type: postgres
      options:
        url: "jdbc:postgresql://localhost:5432/mydb"
        user: "postgres"
        password: "${POSTGRES_PASSWORD}"
    steps:
      - name: customers
        options:
          dbtable: "public.customers"
        count:
          records: 1000''', 'PostgreSQL data generation'),
            ],
        },
        'mysql': {
            'name': 'MySQL Connector',
            'subcategory': 'databases',
            'description': 'Connect to MySQL databases for reading and writing data. Supports table-level configuration and JDBC options.',
            'tags': ['database', 'jdbc', 'relational', 'sql'],
            'config': [
                make_config_option('url', 'string', 'JDBC connection URL', required=True, scope='datasource'),
                make_config_option('user', 'string', 'Database username', scope='datasource'),
                make_config_option('password', 'string', 'Database password', scope='datasource'),
                make_config_option('driver', 'string', 'JDBC driver class', default='com.mysql.cj.jdbc.Driver', scope='datasource'),
                make_config_option('dbtable', 'string', 'Target table', scope='step'),
            ],
            'examples': [
                make_example('yaml', '''connection:
  type: mysql
  options:
    url: "jdbc:mysql://localhost:3306/mydb"
    user: "root"
    password: "${MYSQL_PASSWORD}"''', 'MySQL connection'),
            ],
        },
        'cassandra': {
            'name': 'Cassandra Connector',
            'subcategory': 'databases',
            'description': 'Connect to Apache Cassandra for reading and writing data. Supports keyspace/table configuration, primary key and clustering positions.',
            'tags': ['database', 'nosql', 'wide-column'],
            'config': [
                make_config_option('url', 'string', 'Cassandra contact point URL', required=True, scope='datasource'),
                make_config_option('user', 'string', 'Cassandra username', scope='datasource'),
                make_config_option('password', 'string', 'Cassandra password', scope='datasource'),
                make_config_option('keyspace', 'string', 'Cassandra keyspace', required=True, scope='step'),
                make_config_option('table', 'string', 'Cassandra table', required=True, scope='step'),
            ],
            'examples': [
                make_example('yaml', '''connection:
  type: cassandra
  options:
    url: "localhost:9042"
    user: "cassandra"
    password: "cassandra"''', 'Cassandra connection'),
            ],
        },
        'bigquery': {
            'name': 'BigQuery Connector',
            'subcategory': 'databases',
            'description': 'Connect to Google BigQuery for reading and writing data. Supports direct and indirect write methods.',
            'tags': ['database', 'cloud', 'google', 'data-warehouse'],
            'config': [
                make_config_option('table', 'string', 'BigQuery table (project.dataset.table)', required=True, scope='step'),
                make_config_option('credentialsFile', 'string', 'Path to GCP credentials JSON', scope='datasource'),
                make_config_option('writeMethod', 'string', 'Write method', default='indirect', scope='datasource', valid_values=['direct', 'indirect']),
                make_config_option('temporaryGcsBucket', 'string', 'GCS bucket for indirect writes', scope='datasource'),
                make_config_option('queryJobPriority', 'string', 'Query job priority', default='batch', scope='datasource'),
            ],
            'examples': [],
        },
        # File formats
        'csv': {
            'name': 'CSV File Connector',
            'subcategory': 'files',
            'description': 'Read and write CSV files. Supports headers, delimiters, and other CSV-specific options.',
            'tags': ['file', 'csv', 'delimited', 'text'],
            'config': [
                make_config_option('path', 'string', 'File system path for CSV files', required=True, scope='datasource', yaml_path='dataSources[].connection.options.path'),
            ],
            'examples': [
                make_example('yaml', '''connection:
  type: csv
  options:
    path: "/tmp/data/csv-output"''', 'CSV file output'),
            ],
        },
        'json': {
            'name': 'JSON File Connector',
            'subcategory': 'files',
            'description': 'Read and write JSON files. Supports nested structures, arrays, and unwrapping top-level arrays.',
            'tags': ['file', 'json', 'structured'],
            'config': [
                make_config_option('path', 'string', 'File system path for JSON files', required=True, scope='datasource'),
                make_config_option('unwrapTopLevelArray', 'boolean', 'Output JSON as root-level array instead of object', default=False, scope='step'),
            ],
            'examples': [
                make_example('yaml', '''connection:
  type: json
  options:
    path: "/tmp/data/json-output"''', 'JSON file output'),
            ],
        },
        'parquet': {
            'name': 'Parquet File Connector',
            'subcategory': 'files',
            'description': 'Read and write Apache Parquet columnar files. Efficient for large datasets.',
            'tags': ['file', 'parquet', 'columnar', 'binary'],
            'config': [
                make_config_option('path', 'string', 'File system path for Parquet files', required=True, scope='datasource'),
            ],
            'examples': [],
        },
        'orc': {
            'name': 'ORC File Connector',
            'subcategory': 'files',
            'description': 'Read and write Apache ORC columnar files.',
            'tags': ['file', 'orc', 'columnar', 'binary'],
            'config': [
                make_config_option('path', 'string', 'File system path for ORC files', required=True, scope='datasource'),
            ],
            'examples': [],
        },
        'delta': {
            'name': 'Delta Lake Connector',
            'subcategory': 'files',
            'description': 'Read and write Delta Lake tables. Supports ACID transactions, time travel, and schema evolution.',
            'tags': ['file', 'delta', 'lakehouse', 'acid'],
            'config': [
                make_config_option('path', 'string', 'File system path for Delta tables', required=True, scope='datasource'),
            ],
            'examples': [],
        },
        'iceberg': {
            'name': 'Apache Iceberg Connector',
            'subcategory': 'files',
            'description': 'Read and write Apache Iceberg tables. Supports multiple catalog types (Hadoop, Hive, REST, Glue, JDBC, Nessie).',
            'tags': ['file', 'iceberg', 'lakehouse', 'catalog'],
            'config': [
                make_config_option('path', 'string', 'Table path', required=True, scope='datasource'),
                make_config_option('catalogType', 'string', 'Iceberg catalog type', default='hadoop', scope='datasource',
                                   valid_values=['hadoop', 'hive', 'rest', 'glue', 'jdbc', 'nessie']),
                make_config_option('catalogUri', 'string', 'Catalog URI (for hive/rest/nessie)', scope='datasource'),
                make_config_option('catalogDefaultNamespace', 'string', 'Default namespace', scope='datasource'),
            ],
            'examples': [],
        },
        'hudi': {
            'name': 'Apache Hudi Connector',
            'subcategory': 'files',
            'description': 'Read and write Apache Hudi tables.',
            'tags': ['file', 'hudi', 'lakehouse'],
            'config': [
                make_config_option('path', 'string', 'Table path', required=True, scope='datasource'),
                make_config_option('hoodie.table.name', 'string', 'Hudi table name', required=True, scope='step'),
            ],
            'examples': [],
        },
        'xml': {
            'name': 'XML File Connector',
            'subcategory': 'files',
            'description': 'Read and write XML files.',
            'tags': ['file', 'xml', 'structured'],
            'config': [
                make_config_option('path', 'string', 'File system path for XML files', required=True, scope='datasource'),
            ],
            'examples': [],
        },
        # Messaging
        'kafka': {
            'name': 'Apache Kafka Connector',
            'subcategory': 'messaging',
            'description': 'Connect to Apache Kafka for producing and consuming messages. Supports topics, partitions, headers, key/value serialization, and streaming patterns.',
            'tags': ['messaging', 'kafka', 'streaming', 'event'],
            'config': [
                make_config_option('url', 'string', 'Kafka bootstrap servers', required=True, scope='datasource', yaml_path='dataSources[].connection.options.url'),
                make_config_option('topic', 'string', 'Kafka topic name', required=True, scope='step'),
                make_config_option('schemaLocation', 'string', 'Schema registry URL or file path', scope='datasource'),
            ],
            'examples': [
                make_example('yaml', '''dataSources:
  - name: my_kafka
    connection:
      type: kafka
      options:
        url: "localhost:9092"
    steps:
      - name: orders_topic
        options:
          topic: "orders"
        count:
          duration: "1m"
          rate: 100
          rateUnit: "second"''', 'Kafka streaming'),
            ],
        },
        'solace': {
            'name': 'Solace JMS Connector',
            'subcategory': 'messaging',
            'description': 'Connect to Solace PubSub+ message broker via JMS. Supports queues and topics.',
            'tags': ['messaging', 'jms', 'solace'],
            'config': [
                make_config_option('url', 'string', 'Solace broker URL', required=True, scope='datasource'),
                make_config_option('user', 'string', 'Username', scope='datasource'),
                make_config_option('password', 'string', 'Password', scope='datasource'),
                make_config_option('vpnName', 'string', 'VPN name', default='default', scope='datasource'),
                make_config_option('connectionFactory', 'string', 'JNDI connection factory', default='/jms/cf/default', scope='datasource'),
                make_config_option('initialContextFactory', 'string', 'JNDI context factory', scope='datasource'),
                make_config_option('destinationName', 'string', 'Queue/topic destination', required=True, scope='step'),
            ],
            'examples': [],
        },
        'rabbitmq': {
            'name': 'RabbitMQ Connector',
            'subcategory': 'messaging',
            'description': 'Connect to RabbitMQ message broker via JMS. Supports queues.',
            'tags': ['messaging', 'rabbitmq', 'jms', 'amqp'],
            'config': [
                make_config_option('url', 'string', 'RabbitMQ URL', required=True, scope='datasource'),
                make_config_option('user', 'string', 'Username', default='guest', scope='datasource'),
                make_config_option('password', 'string', 'Password', default='guest', scope='datasource'),
                make_config_option('virtualHost', 'string', 'Virtual host', default='/', scope='datasource'),
                make_config_option('destinationName', 'string', 'Queue name', required=True, scope='step'),
            ],
            'examples': [],
        },
        # HTTP
        'http': {
            'name': 'HTTP/REST API Connector',
            'subcategory': 'http',
            'description': 'Send generated data to HTTP/REST APIs. Supports custom methods, headers, URL path parameters, query parameters, and request bodies.',
            'tags': ['http', 'rest', 'api', 'web'],
            'config': [
                make_config_option('url', 'string', 'Base URL for HTTP requests', required=True, scope='datasource'),
            ],
            'examples': [
                make_example('yaml', '''dataSources:
  - name: my_api
    connection:
      type: http
      options:
        url: "http://localhost:8080"
    steps:
      - name: create_users
        fields:
          - name: httpUrl
            type: struct
            fields:
              - name: url
                static: "http://localhost:8080/api/users"
              - name: method
                static: "POST"
          - name: httpBody
            type: struct
            fields:
              - name: name
                options:
                  expression: "#{Name.fullName}"''', 'HTTP API data generation'),
            ],
        },
    }

    for key, info in connectors.items():
        features.append(make_feature(
            id=make_feature_id('connector', info['subcategory'], key),
            name=info['name'],
            category='connectors',
            subcategory=info['subcategory'],
            status='stable',
            description=info['description'],
            configuration=info['config'],
            examples=info.get('examples', []),
            tags=info.get('tags', []),
            source_files=[
                {'path': 'api/src/main/scala/io/github/datacatering/datacaterer/api/model/Constants.scala', 'role': 'supporting'},
            ],
        ))

    return features


def extract_field_generation_features() -> list[dict]:
    """Extract data generation features for field types and generators."""
    features = []

    # Core generator types
    generators = [
        {
            'id': 'generation.field.regex',
            'name': 'Regex Pattern Generation',
            'description': 'Generate string values matching a regular expression pattern. Supports SQL-based optimization for common patterns with automatic fallback to UDF for complex patterns (lookaheads, backreferences).',
            'config': [
                make_config_option('regex', 'string', 'Regular expression pattern to generate values from', required=True, scope='field', yaml_path='fields[].options.regex'),
            ],
            'examples': [
                make_example('yaml', '''- name: account_id
  options:
    regex: "ACC[0-9]{8}"''', 'Simple regex pattern'),
                make_example('yaml', '''- name: product_code
  options:
    regex: "[A-Z]{3}-[0-9]{4}"''', 'Alphanumeric pattern'),
                make_example('scala', 'field.name("account_id").regex("ACC[0-9]{8}")', 'Scala API'),
            ],
            'tags': ['generation', 'string', 'pattern', 'regex'],
            'related': ['configuration.flags.enable_fast_generation'],
            'performance': ['SQL-based optimization available via enableFastGeneration flag', 'Complex patterns (lookaheads, backreferences) automatically fall back to UDF'],
            'source_files': [
                {'path': 'app/src/main/scala/io/github/datacatering/datacaterer/core/generator/provider/regex/RegexPatternParser.scala', 'role': 'primary'},
            ],
        },
        {
            'id': 'generation.field.expression',
            'name': 'DataFaker Expression',
            'description': 'Generate realistic fake data using DataFaker library expressions. Supports names, addresses, emails, phone numbers, and hundreds of other data types.',
            'config': [
                make_config_option('expression', 'string', 'DataFaker expression (e.g., #{Name.firstName})', required=True, scope='field', yaml_path='fields[].options.expression'),
            ],
            'examples': [
                make_example('yaml', '''- name: full_name
  options:
    expression: "#{Name.fullName}"''', 'Full name generation'),
                make_example('yaml', '''- name: email
  options:
    expression: "#{Internet.emailAddress}"''', 'Email generation'),
                make_example('scala', 'field.name("email").expression("#{Internet.emailAddress}")', 'Scala API'),
            ],
            'tags': ['generation', 'faker', 'realistic', 'expression'],
            'deps': {'libraries': ['net.datafaker:datafaker']},
        },
        {
            'id': 'generation.field.one_of',
            'name': 'One-Of Selection',
            'description': 'Generate values by randomly selecting from a predefined list of options. Useful for categorical data like statuses, types, and enums.',
            'config': [
                make_config_option('oneOf', 'array', 'List of values to randomly select from', required=True, scope='field', yaml_path='fields[].options.oneOf'),
            ],
            'examples': [
                make_example('yaml', '''- name: status
  options:
    oneOf: ["active", "inactive", "pending"]''', 'Enum field'),
                make_example('scala', 'field.name("status").oneOf("active", "inactive", "pending")', 'Scala API'),
            ],
            'tags': ['generation', 'enum', 'categorical', 'selection'],
        },
        {
            'id': 'generation.field.sql',
            'name': 'SQL Expression',
            'description': 'Generate field values using Spark SQL expressions. Supports referencing other fields, date functions, string operations, aggregations, and conditional logic.',
            'config': [
                make_config_option('sql', 'string', 'Spark SQL expression for computed value', required=True, scope='field', yaml_path='fields[].options.sql'),
            ],
            'examples': [
                make_example('yaml', '''- name: year
  type: integer
  options:
    sql: "YEAR(created_at)"''', 'Extract year from date field'),
                make_example('yaml', '''- name: full_name
  type: string
  options:
    sql: "CONCAT(first_name, \' \', last_name)"''', 'Concatenate fields'),
                make_example('yaml', '''- name: total_amount
  type: double
  options:
    sql: "quantity * unit_price"''', 'Computed field'),
            ],
            'tags': ['generation', 'sql', 'computed', 'derived'],
        },
        {
            'id': 'generation.field.static',
            'name': 'Static Value',
            'description': 'Set a fixed static value for all generated records. Useful for constant fields like API endpoints, methods, or content types.',
            'config': [
                make_config_option('static', 'string', 'Fixed value for all records', required=True, scope='field', yaml_path='fields[].static'),
            ],
            'examples': [
                make_example('yaml', '''- name: method
  static: "POST"''', 'Static HTTP method'),
            ],
            'tags': ['generation', 'static', 'constant'],
        },
        {
            'id': 'generation.field.uuid',
            'name': 'UUID Generation',
            'description': 'Generate universally unique identifiers (UUID v4).',
            'config': [
                make_config_option('uuidPattern', 'boolean', 'Enable UUID generation', default=False, scope='field', yaml_path='fields[].options.uuidPattern'),
            ],
            'examples': [
                make_example('yaml', '''- name: id
  options:
    uuidPattern: true''', 'UUID field'),
            ],
            'tags': ['generation', 'uuid', 'identifier', 'unique'],
        },
        {
            'id': 'generation.field.sequence',
            'name': 'Sequential Value Generation',
            'description': 'Generate sequential values with optional prefix and padding. Useful for IDs, batch numbers, and sequential identifiers.',
            'config': [
                make_config_option('sequence', 'object', 'Sequential value configuration with prefix and padding', required=True, scope='field', yaml_path='fields[].options.sequence'),
            ],
            'examples': [
                make_example('yaml', '''- name: order_id
  options:
    sequence:
      start: 1000
      step: 1
      prefix: "ORD-"
      padding: 8''', 'Sequential order IDs'),
            ],
            'tags': ['generation', 'sequence', 'sequential', 'incremental'],
        },
        {
            'id': 'generation.field.conditional_value',
            'name': 'Conditional Value Generation',
            'description': 'Generate values using CASE WHEN logic based on other field values. Enables dependent field generation.',
            'config': [
                make_config_option('conditionalValue', 'object', 'CASE WHEN conditions and result values', required=True, scope='field', yaml_path='fields[].options.conditionalValue'),
            ],
            'examples': [
                make_example('yaml', '''- name: discount
  type: double
  options:
    conditionalValue:
      conditions:
        - expr: "customer_type = \'premium\'"
          value: 0.2
        - expr: "customer_type = \'standard\'"
          value: 0.1
      default: 0.0''', 'Conditional discount'),
            ],
            'tags': ['generation', 'conditional', 'logic', 'derived'],
        },
        {
            'id': 'generation.field.correlated',
            'name': 'Correlated Field Generation',
            'description': 'Generate values that are correlated (or negatively correlated) with another field. Useful for creating realistic relationships between numeric fields.',
            'config': [
                make_config_option('correlatedWith', 'string', 'Field name to correlate with', scope='field', yaml_path='fields[].options.correlatedWith'),
                make_config_option('negativelyCorrelatedWith', 'string', 'Field name to negatively correlate with', scope='field', yaml_path='fields[].options.negativelyCorrelatedWith'),
            ],
            'examples': [
                make_example('yaml', '''- name: revenue
  type: double
  options:
    correlatedWith: "customer_count"''', 'Positively correlated fields'),
            ],
            'tags': ['generation', 'correlation', 'statistical', 'relationship'],
        },
        {
            'id': 'generation.field.mapping',
            'name': 'Value Mapping',
            'description': 'Map values from one field to generate deterministic output in another field.',
            'config': [
                make_config_option('mapping', 'object', 'Mapping configuration from source field to output values', required=True, scope='field', yaml_path='fields[].options.mapping'),
            ],
            'examples': [
                make_example('yaml', '''- name: country_code
  options:
    mapping:
      sourceField: "country"
      mappings:
        "United States": "US"
        "United Kingdom": "UK"''', 'Country code mapping'),
            ],
            'tags': ['generation', 'mapping', 'lookup', 'derived'],
        },
    ]

    for gen in generators:
        features.append(make_feature(
            id=gen['id'],
            name=gen['name'],
            category='generation',
            subcategory='generators',
            status='stable',
            description=gen['description'],
            configuration=gen.get('config', []),
            examples=gen.get('examples', []),
            tags=gen.get('tags', []),
            related_features=gen.get('related', []),
            source_files=gen.get('source_files', []),
            dependencies=gen.get('deps'),
            performance_notes=gen.get('performance', []),
        ))

    return features


def extract_field_option_features() -> list[dict]:
    """Extract field-level configuration options as features."""
    features = []

    # Data types
    data_types = [
        ('string', 'String', 'Text data type. Default field type.'),
        ('integer', 'Integer', '32-bit integer values.'),
        ('long', 'Long', '64-bit long integer values.'),
        ('double', 'Double', 'Double-precision floating point values.'),
        ('float', 'Float', 'Single-precision floating point values.'),
        ('decimal', 'Decimal', 'Fixed-precision decimal values with configurable precision and scale.'),
        ('boolean', 'Boolean', 'True/false boolean values.'),
        ('date', 'Date', 'Date values (year-month-day).'),
        ('timestamp', 'Timestamp', 'Timestamp values with date and time.'),
        ('binary', 'Binary', 'Binary byte array values.'),
        ('array', 'Array', 'Array/list of elements. Configurable element type, min/max length.'),
        ('struct', 'Struct', 'Nested structure with named fields. Supports deep nesting.'),
        ('map', 'Map', 'Key-value map type with configurable key and value types.'),
    ]

    for type_id, name, desc in data_types:
        features.append(make_feature(
            id=make_feature_id('generation', 'type', type_id),
            name=f'{name} Type',
            category='generation',
            subcategory='data_types',
            status='stable',
            description=desc,
            configuration=[
                make_config_option('type', 'string', f'Set field type to "{type_id}"', scope='field', yaml_path='fields[].type'),
            ],
            tags=['generation', 'type', type_id],
        ))

    # Numeric field options
    features.append(make_feature(
        id='generation.option.numeric_range',
        name='Numeric Range',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Constrain numeric fields (integer, long, double, float, decimal) to a minimum and maximum range.',
        configuration=[
            make_config_option('min', 'any', 'Minimum value (inclusive)', scope='field', yaml_path='fields[].options.min', scala_constant='MINIMUM'),
            make_config_option('max', 'any', 'Maximum value (inclusive)', scope='field', yaml_path='fields[].options.max', scala_constant='MAXIMUM'),
        ],
        examples=[
            make_example('yaml', '''- name: age
  type: integer
  options:
    min: 18
    max: 120''', 'Integer range'),
            make_example('yaml', '''- name: price
  type: double
  options:
    min: 9.99
    max: 999.99''', 'Double range'),
        ],
        tags=['generation', 'numeric', 'range', 'constraint'],
    ))

    # Date/time field options
    features.append(make_feature(
        id='generation.option.date_range',
        name='Date/Time Range',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Constrain date and timestamp fields to a minimum and maximum range. Also supports excluding weekends, business hours, within/future days.',
        configuration=[
            make_config_option('min', 'string', 'Minimum date/timestamp', scope='field', yaml_path='fields[].options.min'),
            make_config_option('max', 'string', 'Maximum date/timestamp', scope='field', yaml_path='fields[].options.max'),
            make_config_option('excludeWeekends', 'boolean', 'Exclude Saturday and Sunday', default=False, scope='field', yaml_path='fields[].options.excludeWeekends', scala_constant='DATE_EXCLUDE_WEEKENDS'),
            make_config_option('withinDays', 'integer', 'Generate dates within last N days from now', scope='field', yaml_path='fields[].options.withinDays'),
            make_config_option('futureDays', 'integer', 'Generate dates within next N days from now', scope='field', yaml_path='fields[].options.futureDays'),
            make_config_option('businessHours', 'boolean', 'Restrict to business hours', default=False, scope='field', yaml_path='fields[].options.businessHours'),
            make_config_option('timeBetween', 'object', 'Generate times between start and end', scope='field', yaml_path='fields[].options.timeBetween'),
        ],
        examples=[
            make_example('yaml', '''- name: created_at
  type: timestamp
  options:
    min: "2024-01-01T00:00:00"
    max: "2024-12-31T23:59:59"''', 'Timestamp range'),
        ],
        tags=['generation', 'date', 'timestamp', 'range'],
    ))

    # Null handling
    features.append(make_feature(
        id='generation.option.null_handling',
        name='Null Value Control',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Control whether and how often null values appear in generated data. Configurable null probability per field.',
        configuration=[
            make_config_option('enableNull', 'boolean', 'Allow null values for this field', default=False, scope='field', yaml_path='fields[].options.enableNull', scala_constant='ENABLED_NULL'),
            make_config_option('nullProb', 'double', 'Probability of generating a null value (0-1)', scope='field', yaml_path='fields[].options.nullProb', scala_constant='PROBABILITY_OF_NULL'),
            make_config_option('nullable', 'boolean', 'Whether the field schema allows nulls', default=True, scope='field', yaml_path='fields[].nullable'),
        ],
        examples=[
            make_example('yaml', '''- name: middle_name
  options:
    enableNull: true
    nullProb: 0.3''', '30% null probability'),
        ],
        tags=['generation', 'null', 'nullable', 'probability'],
    ))

    # Edge case handling
    features.append(make_feature(
        id='generation.option.edge_cases',
        name='Edge Case Generation',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Control the probability of generating edge case values (empty strings, boundary values, special characters).',
        configuration=[
            make_config_option('enableEdgeCase', 'boolean', 'Enable edge case generation', default=False, scope='field', scala_constant='ENABLED_EDGE_CASE'),
            make_config_option('edgeCaseProb', 'double', 'Probability of generating edge case values (0-1)', scope='field', scala_constant='PROBABILITY_OF_EDGE_CASE'),
        ],
        tags=['generation', 'edge-case', 'boundary', 'testing'],
    ))

    # String length
    features.append(make_feature(
        id='generation.option.string_length',
        name='String Length Control',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Control the length of generated string values with minimum, maximum, and average length.',
        configuration=[
            make_config_option('minLen', 'integer', 'Minimum string length', scope='field', scala_constant='MINIMUM_LENGTH'),
            make_config_option('maxLen', 'integer', 'Maximum string length', scope='field', scala_constant='MAXIMUM_LENGTH'),
            make_config_option('avgLen', 'integer', 'Average string length', scope='field', scala_constant='AVERAGE_LENGTH'),
        ],
        tags=['generation', 'string', 'length', 'constraint'],
    ))

    # Array options
    features.append(make_feature(
        id='generation.option.array_config',
        name='Array Configuration',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Configure array field generation: element count, element type, uniqueness, empty probability, and weighted selection.',
        configuration=[
            make_config_option('arrayMinLen', 'integer', 'Minimum array length', scope='field', yaml_path='fields[].options.arrayMinLength', scala_constant='ARRAY_MINIMUM_LENGTH'),
            make_config_option('arrayMaxLen', 'integer', 'Maximum array length', scope='field', yaml_path='fields[].options.arrayMaxLength', scala_constant='ARRAY_MAXIMUM_LENGTH'),
            make_config_option('arrayFixedSize', 'integer', 'Fixed array size', scope='field', scala_constant='ARRAY_FIXED_SIZE'),
            make_config_option('arrayEmptyProb', 'double', 'Probability of empty array (0-1)', scope='field', yaml_path='fields[].options.arrayEmptyProbability', scala_constant='ARRAY_EMPTY_PROBABILITY'),
            make_config_option('arrayType', 'string', 'Element data type for array', scope='field', scala_constant='ARRAY_TYPE'),
            make_config_option('arrayOneOf', 'string', 'Comma-separated values for array elements', scope='field', scala_constant='ARRAY_ONE_OF'),
            make_config_option('arrayUniqueFrom', 'string', 'Source for unique array elements', scope='field', scala_constant='ARRAY_UNIQUE_FROM'),
            make_config_option('arrayWeightedOneOf', 'string', 'Weighted selection for elements (e.g., HIGH:0.2,MEDIUM:0.5,LOW:0.3)', scope='field', yaml_path='fields[].options.arrayWeightedOneOf', scala_constant='ARRAY_WEIGHTED_ONE_OF'),
        ],
        tags=['generation', 'array', 'collection', 'nested'],
    ))

    # Map options
    features.append(make_feature(
        id='generation.option.map_config',
        name='Map Configuration',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Configure map field generation with minimum and maximum size.',
        configuration=[
            make_config_option('mapMinSize', 'integer', 'Minimum number of entries', scope='field', scala_constant='MAP_MINIMUM_SIZE'),
            make_config_option('mapMaxSize', 'integer', 'Maximum number of entries', scope='field', scala_constant='MAP_MAXIMUM_SIZE'),
        ],
        tags=['generation', 'map', 'key-value', 'nested'],
    ))

    # Distribution
    features.append(make_feature(
        id='generation.option.distribution',
        name='Value Distribution',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Control the statistical distribution of generated numeric values. Supports uniform, normal, and exponential distributions.',
        configuration=[
            make_config_option('distribution', 'enum', 'Distribution type', scope='field', valid_values=['uniform', 'normal', 'exponential'], scala_constant='DISTRIBUTION'),
            make_config_option('mean', 'double', 'Mean for normal distribution', scope='field', scala_constant='MEAN'),
            make_config_option('stddev', 'double', 'Standard deviation for normal distribution', scope='field', scala_constant='STANDARD_DEVIATION'),
            make_config_option('distributionRateParam', 'double', 'Rate parameter for exponential distribution', scope='field', scala_constant='DISTRIBUTION_RATE_PARAMETER'),
        ],
        tags=['generation', 'distribution', 'statistical', 'normal', 'uniform'],
    ))

    # Uniqueness
    features.append(make_feature(
        id='generation.option.uniqueness',
        name='Uniqueness Constraint',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Enforce unique values for a field using bloom filter-based deduplication.',
        configuration=[
            make_config_option('isUnique', 'boolean', 'Enforce unique values', default=False, scope='field', yaml_path='fields[].options.isUnique', scala_constant='IS_UNIQUE'),
            make_config_option('isPrimaryKey', 'boolean', 'Mark as primary key (implies unique)', default=False, scope='field', yaml_path='fields[].options.isPrimaryKey', scala_constant='IS_PRIMARY_KEY'),
            make_config_option('primaryKeyPos', 'integer', 'Position in composite primary key', scope='field', scala_constant='PRIMARY_KEY_POSITION'),
        ],
        tags=['generation', 'unique', 'primary-key', 'constraint'],
        related_features=['configuration.flags.enable_unique_check'],
    ))

    # Numeric precision
    features.append(make_feature(
        id='generation.option.numeric_precision',
        name='Numeric Precision and Scale',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Control precision and scale for decimal fields, and rounding for numeric fields.',
        configuration=[
            make_config_option('precision', 'integer', 'Numeric precision (total digits)', scope='field', scala_constant='NUMERIC_PRECISION'),
            make_config_option('scale', 'integer', 'Numeric scale (decimal places)', scope='field', scala_constant='NUMERIC_SCALE'),
            make_config_option('round', 'integer', 'Round numeric values to N decimal places', scope='field', scala_constant='ROUND'),
        ],
        tags=['generation', 'numeric', 'precision', 'decimal'],
    ))

    # Omit field
    features.append(make_feature(
        id='generation.option.omit',
        name='Field Omission',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Generate a field for use in computed expressions but omit it from the final output.',
        configuration=[
            make_config_option('omit', 'boolean', 'Omit field from output', default=False, scope='field', yaml_path='fields[].options.omit', scala_constant='OMIT'),
        ],
        tags=['generation', 'omit', 'helper', 'computed'],
    ))

    # Seed
    features.append(make_feature(
        id='generation.option.seed',
        name='Random Seed',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Set a random seed for reproducible data generation per field.',
        configuration=[
            make_config_option('seed', 'integer', 'Random seed for reproducible generation', scope='field', yaml_path='fields[].options.seed', scala_constant='RANDOM_SEED'),
        ],
        tags=['generation', 'seed', 'reproducible', 'deterministic'],
    ))

    # Distinct count / histogram
    features.append(make_feature(
        id='generation.option.distinct_count',
        name='Distinct Value Count',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Control how many distinct values are generated for a field. Used with metadata-driven generation.',
        configuration=[
            make_config_option('distinctCount', 'integer', 'Number of distinct values to generate', scope='field', scala_constant='DISTINCT_COUNT'),
            make_config_option('histogram', 'object', 'Value distribution histogram', scope='field', scala_constant='HISTOGRAM'),
        ],
        tags=['generation', 'distinct', 'cardinality', 'metadata'],
    ))

    # Cassandra-specific
    features.append(make_feature(
        id='generation.option.cassandra_keys',
        name='Cassandra Key Configuration',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Configure Cassandra-specific primary key and clustering positions for fields.',
        configuration=[
            make_config_option('isPrimaryKey', 'boolean', 'Mark as partition key', scope='field', scala_constant='IS_PRIMARY_KEY'),
            make_config_option('primaryKeyPos', 'integer', 'Position in composite partition key', scope='field', scala_constant='PRIMARY_KEY_POSITION'),
            make_config_option('clusteringPos', 'integer', 'Clustering column position', scope='field', scala_constant='CLUSTERING_POSITION'),
        ],
        tags=['generation', 'cassandra', 'primary-key', 'clustering'],
    ))

    # Incremental
    features.append(make_feature(
        id='generation.option.incremental',
        name='Incremental Generation',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Mark a field for incremental generation, tracking the last generated value across runs.',
        configuration=[
            make_config_option('incremental', 'boolean', 'Enable incremental mode', default=False, scope='field', scala_constant='INCREMENTAL'),
        ],
        tags=['generation', 'incremental', 'tracking'],
    ))

    # HTTP parameter type
    features.append(make_feature(
        id='generation.option.http_param_type',
        name='HTTP Parameter Type',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Specify the HTTP parameter type for a field when using the HTTP connector (path, query, or header).',
        configuration=[
            make_config_option('httpParamType', 'enum', 'HTTP parameter placement', scope='field', valid_values=['path', 'query', 'header'], scala_constant='HTTP_PARAMETER_TYPE'),
        ],
        tags=['generation', 'http', 'parameter', 'api'],
    ))

    # Post SQL expression
    features.append(make_feature(
        id='generation.option.post_sql_expression',
        name='Post-SQL Expression',
        category='generation',
        subcategory='field_options',
        status='stable',
        description='Apply a SQL expression to transform the field value after initial generation.',
        configuration=[
            make_config_option('postSqlExpression', 'string', 'SQL expression to apply after generation', scope='field', scala_constant='POST_SQL_EXPRESSION'),
        ],
        tags=['generation', 'sql', 'transform', 'post-processing'],
    ))

    # Semantic version
    features.append(make_feature(
        id='generation.field.semantic_version',
        name='Semantic Version Generation',
        category='generation',
        subcategory='generators',
        status='stable',
        description='Generate semantic version strings (e.g., 1.2.3).',
        configuration=[
            make_config_option('semanticVersion', 'object', 'Semantic version configuration', scope='field', yaml_path='fields[].options.semanticVersion'),
        ],
        tags=['generation', 'version', 'semver'],
    ))

    # Daily batch sequence
    features.append(make_feature(
        id='generation.field.daily_batch_sequence',
        name='Daily Batch Sequence',
        category='generation',
        subcategory='generators',
        status='stable',
        description='Generate daily batch sequence identifiers.',
        configuration=[
            make_config_option('dailyBatchSequence', 'object', 'Daily batch sequence configuration', scope='field', yaml_path='fields[].options.dailyBatchSequence'),
        ],
        tags=['generation', 'batch', 'daily', 'sequence'],
    ))

    return features


def extract_field_label_features() -> list[dict]:
    """Extract field label features for metadata-driven generation."""
    labels = [
        ('name', 'Name', 'Generate person name fields (first name, last name, full name)'),
        ('username', 'Username', 'Generate username fields'),
        ('address', 'Address', 'Generate address fields (street, city, postcode)'),
        ('app', 'Application', 'Generate application-related fields (version)'),
        ('nation', 'Nation', 'Generate nationality, language, capital city'),
        ('money', 'Money', 'Generate currency and financial fields'),
        ('internet', 'Internet', 'Generate email, IP, MAC address fields'),
        ('food', 'Food', 'Generate food and ingredient fields'),
        ('job', 'Job', 'Generate job title, field, position'),
        ('relationship', 'Relationship', 'Generate relationship type fields'),
        ('weather', 'Weather', 'Generate weather description fields'),
        ('phone', 'Phone', 'Generate phone number fields'),
        ('geo', 'Geo', 'Generate geographic coordinate fields'),
    ]

    features = []
    for label_id, name, desc in labels:
        features.append(make_feature(
            id=make_feature_id('generation', 'label', label_id),
            name=f'{name} Label',
            category='generation',
            subcategory='labels',
            status='stable',
            description=f'{desc}. Used for metadata-driven field generation to automatically select appropriate data generators.',
            configuration=[
                make_config_option('label', 'string', f'Set field label to "{label_id}" for auto-detection', scope='field', scala_constant='FIELD_LABEL'),
            ],
            tags=['generation', 'label', 'metadata', label_id],
        ))

    return features


def extract_validation_features() -> list[dict]:
    """Extract validation features from Constants.scala."""
    features = []

    # Field-level validations
    field_validations = [
        ('null', 'Null Check', 'Validate that a field is null (or not null with negate=true).'),
        ('unique', 'Unique Values', 'Validate that all values in a field are unique.'),
        ('equal', 'Equality Check', 'Validate that field values equal a specified value.'),
        ('contains', 'Contains Check', 'Validate that string field values contain a substring.'),
        ('starts_with', 'Starts With', 'Validate that string field values start with a prefix.'),
        ('ends_with', 'Ends With', 'Validate that string field values end with a suffix.'),
        ('less_than', 'Less Than', 'Validate that numeric values are less than a threshold.'),
        ('greater_than', 'Greater Than', 'Validate that numeric values are greater than a threshold.'),
        ('between', 'Between Range', 'Validate that values fall within a min/max range (inclusive).'),
        ('in', 'In Set', 'Validate that values exist in a specified set of allowed values.'),
        ('matches', 'Regex Match', 'Validate that string values match a regular expression pattern.'),
        ('matches_list', 'Regex Match List', 'Validate that string values match one of multiple regex patterns.'),
        ('size', 'Size Check', 'Validate the size/length of a collection or string field.'),
        ('less_than_size', 'Less Than Size', 'Validate that collection size is less than a threshold.'),
        ('greater_than_size', 'Greater Than Size', 'Validate that collection size is greater than a threshold.'),
        ('length_between', 'Length Between', 'Validate that string length falls within a range.'),
        ('length_equal', 'Length Equal', 'Validate that string length equals a specific value.'),
        ('luhn_check', 'Luhn Check', 'Validate values using the Luhn algorithm (credit cards, IDs).'),
        ('has_type', 'Type Check', 'Validate that field values are of a specific data type.'),
        ('has_types', 'Multi-Type Check', 'Validate that field values match one of multiple types.'),
        ('is_decreasing', 'Monotonically Decreasing', 'Validate that values are in decreasing order.'),
        ('is_increasing', 'Monotonically Increasing', 'Validate that values are in increasing order.'),
        ('is_json_parsable', 'JSON Parsable', 'Validate that string values are valid JSON.'),
        ('match_json_schema', 'JSON Schema Match', 'Validate that JSON values conform to a JSON schema.'),
        ('match_date_time_format', 'DateTime Format Match', 'Validate that values match a specific date/time format.'),
        ('distinct_in_set', 'Distinct In Set', 'Validate that all distinct values exist in a specified set.'),
        ('distinct_contains_set', 'Distinct Contains Set', 'Validate that distinct values contain all values from a specified set.'),
        ('distinct_equal', 'Distinct Equal', 'Validate that the set of distinct values exactly equals a specified set.'),
        ('most_common_value_in_set', 'Most Common Value', 'Validate that the most common value is in a specified set.'),
    ]

    for val_id, name, desc in field_validations:
        features.append(make_feature(
            id=make_feature_id('validation', 'field', val_id),
            name=name,
            category='validation',
            subcategory='field_validations',
            status='stable',
            description=desc,
            configuration=[
                make_config_option('type', 'string', f'Set validation type to "{val_id.replace("_", "")}"', scope='field'),
                make_config_option('negate', 'boolean', 'Invert the validation result', default=False, scope='field'),
                make_config_option('errorThreshold', 'double', 'Allowed error rate (0-1)', scope='field'),
                make_config_option('description', 'string', 'Human-readable description', scope='field'),
            ],
            tags=['validation', 'field', val_id.replace('_', '-')],
        ))

    # Statistical validations
    stat_validations = [
        ('max_between', 'Max Between', 'Validate that the maximum value of a field falls within a range.'),
        ('mean_between', 'Mean Between', 'Validate that the mean value of a field falls within a range.'),
        ('median_between', 'Median Between', 'Validate that the median value of a field falls within a range.'),
        ('min_between', 'Min Between', 'Validate that the minimum value of a field falls within a range.'),
        ('std_dev_between', 'Std Dev Between', 'Validate that the standard deviation of a field falls within a range.'),
        ('sum_between', 'Sum Between', 'Validate that the sum of a field falls within a range.'),
        ('unique_values_proportion_between', 'Unique Values Proportion', 'Validate that the proportion of unique values falls within a range.'),
        ('quantile_values_between', 'Quantile Values Between', 'Validate that quantile values fall within specified ranges.'),
    ]

    for val_id, name, desc in stat_validations:
        features.append(make_feature(
            id=make_feature_id('validation', 'statistical', val_id),
            name=name,
            category='validation',
            subcategory='statistical_validations',
            status='stable',
            description=desc,
            configuration=[
                make_config_option('type', 'string', f'Set validation type', scope='field'),
                make_config_option('min', 'any', 'Minimum expected value', scope='field'),
                make_config_option('max', 'any', 'Maximum expected value', scope='field'),
            ],
            tags=['validation', 'statistical', val_id.replace('_', '-')],
        ))

    # Expression validation
    features.append(make_feature(
        id='validation.expression',
        name='SQL Expression Validation',
        category='validation',
        subcategory='expression_validations',
        status='stable',
        description='Validate data using arbitrary Spark SQL expressions that must evaluate to true. The most flexible validation type.',
        configuration=[
            make_config_option('expr', 'string', 'SQL expression that must evaluate to true', required=True, scope='step', yaml_path='validations[].expr'),
            make_config_option('selectExpr', 'array', 'SELECT columns for the expression', scope='step', yaml_path='validations[].selectExpr'),
            make_config_option('preFilterExpr', 'string', 'SQL filter to apply before validation', scope='step', yaml_path='validations[].preFilterExpr'),
            make_config_option('description', 'string', 'Human-readable description', scope='step'),
            make_config_option('errorThreshold', 'double', 'Allowed error rate (0-1)', scope='step'),
        ],
        examples=[
            make_example('yaml', '''validations:
  - expr: "age >= 18 AND age <= 120"
    description: "Age must be valid"''', 'Expression validation'),
        ],
        tags=['validation', 'expression', 'sql', 'flexible'],
    ))

    # Group-by validation
    features.append(make_feature(
        id='validation.group_by',
        name='Group By Aggregation Validation',
        category='validation',
        subcategory='aggregation_validations',
        status='stable',
        description='Validate aggregated data grouped by specified fields. Supports sum, avg, min, max, count, and stddev aggregations.',
        configuration=[
            make_config_option('groupByFields', 'array', 'Fields to group by', required=True, scope='step', yaml_path='validations[].groupByFields'),
            make_config_option('aggField', 'string', 'Field to aggregate', scope='step', yaml_path='validations[].aggField'),
            make_config_option('aggType', 'enum', 'Aggregation function', scope='step', yaml_path='validations[].aggType', valid_values=['sum', 'avg', 'min', 'max', 'count', 'stddev']),
            make_config_option('aggExpr', 'string', 'Custom aggregation expression', scope='step', yaml_path='validations[].aggExpr'),
        ],
        examples=[
            make_example('yaml', '''validations:
  - groupByFields: ["status"]
    aggField: "balance"
    aggType: "avg"
    aggExpr: "avg_balance > 0"
    description: "Average balance per status"''', 'Group by validation'),
        ],
        tags=['validation', 'aggregation', 'group-by', 'statistical'],
    ))

    # Upstream validation
    features.append(make_feature(
        id='validation.upstream',
        name='Upstream Cross-Source Validation',
        category='validation',
        subcategory='cross_source_validations',
        status='stable',
        description='Validate data by joining with an upstream data source. Enables cross-system data consistency checks.',
        configuration=[
            make_config_option('upstreamDataSource', 'string', 'Upstream data source name', required=True, scope='step', yaml_path='validations[].upstreamDataSource'),
            make_config_option('upstreamReadOptions', 'object', 'Read options for upstream source', scope='step', yaml_path='validations[].upstreamReadOptions'),
            make_config_option('joinFields', 'array', 'Fields to join on', scope='step', yaml_path='validations[].joinFields'),
            make_config_option('joinType', 'enum', 'Join type', default='outer', scope='step', yaml_path='validations[].joinType', valid_values=['inner', 'left', 'right', 'full', 'anti', 'semi']),
        ],
        examples=[
            make_example('yaml', '''validations:
  - upstreamDataSource: "source_json"
    joinFields: ["account_id"]
    joinType: "outer"
    validations:
      - expr: "source_json_name == name"''', 'Cross-source validation'),
        ],
        tags=['validation', 'upstream', 'cross-source', 'join'],
    ))

    # Field names validation
    features.append(make_feature(
        id='validation.field_names',
        name='Schema Field Names Validation',
        category='validation',
        subcategory='schema_validations',
        status='stable',
        description='Validate the schema structure by checking field/column names, counts, and ordering.',
        configuration=[
            make_config_option('names', 'array', 'Expected field names', scope='step', yaml_path='validations[].names'),
            make_config_option('fieldNameType', 'enum', 'Validation type for field names', scope='step', valid_values=['fieldCountEqual', 'fieldCountBetween', 'fieldNameMatchOrder', 'fieldNameMatchSet']),
            make_config_option('count', 'integer', 'Expected exact field count', scope='step'),
            make_config_option('min', 'integer', 'Minimum field count', scope='step'),
            make_config_option('max', 'integer', 'Maximum field count', scope='step'),
        ],
        tags=['validation', 'schema', 'field-names', 'structure'],
    ))

    # Wait conditions
    features.append(make_feature(
        id='validation.wait_condition',
        name='Wait Conditions',
        category='validation',
        subcategory='wait_conditions',
        status='stable',
        description='Define conditions to wait for before running validations. Supports pause, file existence, data existence, and webhook checks.',
        configuration=[
            make_config_option('type', 'enum', 'Wait condition type', scope='step', yaml_path='validations[].waitCondition.type', valid_values=['pause', 'fileExists', 'dataExists', 'webhook']),
            make_config_option('pauseInSeconds', 'integer', 'Seconds to pause', scope='step'),
            make_config_option('path', 'string', 'File path to wait for', scope='step'),
            make_config_option('url', 'string', 'Webhook URL', scope='step'),
            make_config_option('method', 'enum', 'HTTP method for webhook', scope='step', valid_values=['GET', 'POST', 'PUT', 'DELETE']),
            make_config_option('statusCodes', 'array', 'Expected HTTP status codes', scope='step'),
            make_config_option('maxRetries', 'integer', 'Maximum retry attempts', scope='step'),
            make_config_option('waitBeforeRetrySeconds', 'integer', 'Seconds between retries', scope='step'),
        ],
        tags=['validation', 'wait', 'condition', 'async'],
    ))

    return features


def extract_configuration_features(constants_content: str, config_content: str) -> list[dict]:
    """Extract configuration flag and setting features."""
    features = []

    # Parse FlagsConfig case class
    flags = parse_scala_case_class(config_content, 'FlagsConfig')

    flag_descriptions = {
        'enableCount': ('Count Records', 'Count the number of records generated for each data source step.', 'ENABLE_COUNT'),
        'enableGenerateData': ('Generate Data', 'Enable or disable data generation. When false, only validation runs.', 'ENABLE_GENERATE_DATA'),
        'enableRecordTracking': ('Record Tracking', 'Track generated records for later cleanup/deletion.', 'ENABLE_RECORD_TRACKING'),
        'enableDeleteGeneratedRecords': ('Delete Generated Records', 'Enable cleanup mode to delete previously generated records.', 'ENABLE_DELETE_GENERATED_RECORDS'),
        'enableGeneratePlanAndTasks': ('Auto-Generate Plan and Tasks', 'Automatically generate plan and tasks from metadata sources.', 'ENABLE_GENERATE_PLAN_AND_TASKS'),
        'enableFailOnError': ('Fail on Error', 'Fail execution immediately when errors occur.', 'ENABLE_FAIL_ON_ERROR'),
        'enableUniqueCheck': ('Unique Check', 'Validate uniqueness constraints during data generation.', 'ENABLE_UNIQUE_CHECK'),
        'enableSinkMetadata': ('Sink Metadata', 'Save metadata about generated data to the sink.', 'ENABLE_SINK_METADATA'),
        'enableSaveReports': ('Save Reports', 'Generate and save execution reports with generation and validation results.', 'ENABLE_SAVE_REPORTS'),
        'enableValidation': ('Data Validation', 'Run data validations after generation completes.', 'ENABLE_VALIDATION'),
        'enableGenerateValidations': ('Suggest Validations', 'Auto-suggest validations based on data analysis.', 'ENABLE_SUGGEST_VALIDATIONS'),
        'enableAlerts': ('Alerts', 'Send alert notifications on completion (supports Slack).', 'ENABLE_ALERTS'),
        'enableUniqueCheckOnlyInBatch': ('Unique Check Only In Batch', 'Check uniqueness only within the current batch for better performance.', 'ENABLE_UNIQUE_CHECK_ONLY_WITHIN_BATCH'),
        'enableFastGeneration': ('Fast Generation', 'Use SQL-based generation for regex patterns instead of UDFs. Dramatically improves performance.', 'ENABLE_FAST_GENERATION'),
    }

    for flag in flags:
        name = flag['name']
        if name in flag_descriptions:
            display_name, desc, env_const = flag_descriptions[name]
            default_val = flag.get('default', '')
            # Resolve default references
            default_map = {
                'DEFAULT_ENABLE_COUNT': True, 'DEFAULT_ENABLE_GENERATE_DATA': True,
                'DEFAULT_ENABLE_RECORD_TRACKING': False, 'DEFAULT_ENABLE_DELETE_GENERATED_RECORDS': False,
                'DEFAULT_ENABLE_GENERATE_PLAN_AND_TASKS': False, 'DEFAULT_ENABLE_FAIL_ON_ERROR': True,
                'DEFAULT_ENABLE_UNIQUE_CHECK': False, 'DEFAULT_ENABLE_SINK_METADATA': False,
                'DEFAULT_ENABLE_SAVE_REPORTS': True, 'DEFAULT_ENABLE_VALIDATION': True,
                'DEFAULT_ENABLE_SUGGEST_VALIDATIONS': False, 'DEFAULT_ENABLE_ALERTS': True,
                'DEFAULT_ENABLE_UNIQUE_CHECK_ONLY_WITHIN_BATCH': False, 'DEFAULT_ENABLE_FAST_GENERATION': False,
            }
            resolved_default = default_map.get(str(default_val), default_val)

            features.append(make_feature(
                id=make_feature_id('configuration', 'flags', name),
                name=display_name,
                category='configuration',
                subcategory='flags',
                status='stable',
                description=desc,
                configuration=[
                    make_config_option(name, 'boolean', desc, default=resolved_default, scope='global',
                                       yaml_path=f'config.flags.{name}', env_var=env_const),
                ],
                tags=['configuration', 'flag', name.replace('enable', '').lower()],
                source_files=[
                    {'path': 'api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala', 'role': 'primary'},
                ],
            ))

    # Folder configuration
    folders = parse_scala_case_class(config_content, 'FoldersConfig')
    for folder in folders:
        features.append(make_feature(
            id=make_feature_id('configuration', 'folders', folder['name']),
            name=folder['name'].replace('FolderPath', ' Folder').replace('FilePath', ' File').replace('Path', ''),
            category='configuration',
            subcategory='folders',
            status='stable',
            description=f'Configuration path for {folder["name"]}.',
            configuration=[
                make_config_option(folder['name'], 'string', f'Path setting for {folder["name"]}', scope='global',
                                   yaml_path=f'config.folders.{folder["name"]}'),
            ],
            tags=['configuration', 'folder', 'path'],
        ))

    # Generation config
    features.append(make_feature(
        id='configuration.generation.batch_size',
        name='Batch Size',
        category='configuration',
        subcategory='generation',
        status='stable',
        description='Control the number of records generated per batch. Affects memory usage and performance.',
        configuration=[
            make_config_option('numRecordsPerBatch', 'long', 'Records per batch', default=100000, scope='global',
                               yaml_path='config.generation.numRecordsPerBatch'),
            make_config_option('numRecordsPerStep', 'long', 'Default records per step/table', scope='global',
                               yaml_path='config.generation.numRecordsPerStep'),
        ],
        tags=['configuration', 'generation', 'batch', 'performance'],
    ))

    features.append(make_feature(
        id='configuration.generation.bloom_filter',
        name='Bloom Filter Configuration',
        category='configuration',
        subcategory='generation',
        status='stable',
        description='Configure bloom filter parameters for uniqueness checking during generation.',
        configuration=[
            make_config_option('uniqueBloomFilterNumItems', 'long', 'Expected number of items in bloom filter', default=10000000, scope='global',
                               yaml_path='config.generation.uniqueBloomFilterNumItems'),
            make_config_option('uniqueBloomFilterFalsePositiveProbability', 'double', 'Bloom filter false positive rate (0-1)', default=0.01, scope='global',
                               yaml_path='config.generation.uniqueBloomFilterFalsePositiveProbability',
                               range={'min': 0, 'max': 1}),
        ],
        tags=['configuration', 'generation', 'bloom-filter', 'uniqueness'],
    ))

    # Metadata config
    features.append(make_feature(
        id='configuration.metadata',
        name='Metadata Analysis Configuration',
        category='configuration',
        subcategory='metadata',
        status='stable',
        description='Configure how metadata is sampled and analyzed for auto-generation of field patterns.',
        configuration=[
            make_config_option('numRecordsFromDataSource', 'integer', 'Sample size from data source', default=10000, scope='global',
                               yaml_path='config.metadata.numRecordsFromDataSource'),
            make_config_option('numRecordsForAnalysis', 'integer', 'Records analyzed for pattern detection', default=10000, scope='global',
                               yaml_path='config.metadata.numRecordsForAnalysis'),
            make_config_option('oneOfDistinctCountVsCountThreshold', 'double', 'Threshold for detecting oneOf fields', default=0.2, scope='global',
                               yaml_path='config.metadata.oneOfDistinctCountVsCountThreshold'),
            make_config_option('oneOfMinCount', 'long', 'Minimum records for oneOf detection', default=1000, scope='global',
                               yaml_path='config.metadata.oneOfMinCount'),
            make_config_option('numGeneratedSamples', 'integer', 'Number of sample records in metadata suggestions', default=10, scope='global',
                               yaml_path='config.metadata.numGeneratedSamples'),
        ],
        tags=['configuration', 'metadata', 'analysis', 'sampling'],
    ))

    # Streaming config
    features.append(make_feature(
        id='configuration.streaming',
        name='Streaming Configuration',
        category='configuration',
        subcategory='streaming',
        status='stable',
        description='Configure streaming/real-time data generation parameters.',
        configuration=[
            make_config_option('maxTimeoutSeconds', 'integer', 'Maximum streaming timeout', default=3600, scope='global'),
            make_config_option('maxAsyncParallelism', 'integer', 'Maximum async parallelism', default=100, scope='global'),
            make_config_option('responseBufferSize', 'integer', 'Response buffer size for streaming', default=10000, scope='global'),
            make_config_option('timestampWindowMs', 'long', 'Timestamp window in milliseconds', default=1000, scope='global'),
        ],
        tags=['configuration', 'streaming', 'real-time', 'performance'],
    ))

    # Alert config
    features.append(make_feature(
        id='configuration.alerts',
        name='Alert Configuration',
        category='configuration',
        subcategory='alerts',
        status='stable',
        description='Configure alert notifications triggered on execution completion. Supports Slack integration.',
        configuration=[
            make_config_option('triggerOn', 'enum', 'When to trigger alerts', default='all', scope='global',
                               yaml_path='config.alert.triggerOn',
                               valid_values=['all', 'failure', 'success', 'generation_failure', 'validation_failure', 'generation_success', 'validation_success']),
            make_config_option('slackToken', 'string', 'Slack API token', scope='global', yaml_path='config.alert.slackToken'),
            make_config_option('slackChannels', 'array', 'Slack channels to notify', scope='global', yaml_path='config.alert.slackChannels'),
        ],
        tags=['configuration', 'alert', 'notification', 'slack'],
    ))

    # Validation runtime config
    features.append(make_feature(
        id='configuration.validation',
        name='Validation Runtime Configuration',
        category='configuration',
        subcategory='validation_runtime',
        status='stable',
        description='Configure validation execution behavior.',
        configuration=[
            make_config_option('numSampleErrorRecords', 'integer', 'Number of sample error records in reports', default=5, scope='global',
                               yaml_path='config.validation.numSampleErrorRecords'),
            make_config_option('enableDeleteRecordTrackingFiles', 'boolean', 'Delete tracking files after validation', default=True, scope='global',
                               yaml_path='config.validation.enableDeleteRecordTrackingFiles'),
        ],
        tags=['configuration', 'validation', 'runtime'],
    ))

    # Spark runtime config
    features.append(make_feature(
        id='configuration.runtime.spark',
        name='Apache Spark Configuration',
        category='configuration',
        subcategory='runtime',
        status='stable',
        description='Configure the Apache Spark runtime for data processing. Set master URL, driver/executor memory, and Spark SQL settings.',
        configuration=[
            make_config_option('master', 'string', 'Spark master URL', default='local[*]', scope='global', yaml_path='config.runtime.master'),
            make_config_option('sparkConfig', 'object', 'Spark configuration key-value pairs', scope='global', yaml_path='config.runtime.sparkConfig'),
        ],
        examples=[
            make_example('yaml', '''config:
  runtime:
    master: "local[4]"
    sparkConfig:
      "spark.driver.memory": "4g"
      "spark.sql.shuffle.partitions": "10"''', 'Spark configuration'),
        ],
        tags=['configuration', 'runtime', 'spark', 'performance'],
    ))

    # Sink options
    features.append(make_feature(
        id='configuration.sink_options',
        name='Global Sink Options',
        category='configuration',
        subcategory='sink',
        status='stable',
        description='Global options for data output: random seed for reproducibility and locale for data generation.',
        configuration=[
            make_config_option('seed', 'string', 'Random seed for reproducible generation', scope='global', yaml_path='sinkOptions.seed'),
            make_config_option('locale', 'string', 'Locale for data generation (affects names, addresses)', scope='global', yaml_path='sinkOptions.locale'),
        ],
        examples=[
            make_example('yaml', '''sinkOptions:
  seed: "42"
  locale: "en-US"''', 'Sink options'),
        ],
        tags=['configuration', 'sink', 'seed', 'locale'],
    ))

    return features


def extract_advanced_features() -> list[dict]:
    """Extract advanced features: foreign keys, count, streaming, transformation, metadata sources."""
    features = []

    # Foreign key relationships
    features.append(make_feature(
        id='advanced.foreign_keys',
        name='Foreign Key Relationships',
        category='advanced',
        subcategory='referential_integrity',
        status='stable',
        description='Define foreign key relationships between data sources to maintain referential integrity. Supports composite keys, cardinality control, nullability, and multiple generation modes.',
        configuration=[
            make_config_option('source', 'object', 'Source table containing primary key', required=True, scope='plan', yaml_path='foreignKeys[].source'),
            make_config_option('generate', 'array', 'Target tables with foreign key references', scope='plan', yaml_path='foreignKeys[].generate'),
            make_config_option('delete', 'array', 'Target tables for cleanup', scope='plan', yaml_path='foreignKeys[].delete'),
        ],
        examples=[
            make_example('yaml', '''foreignKeys:
  - source:
      dataSource: postgres_db
      step: customers
      fields: ["customer_id"]
    generate:
      - dataSource: postgres_db
        step: orders
        fields: ["customer_id"]
        cardinality:
          min: 1
          max: 10
          distribution: "uniform"''', 'Foreign key with cardinality'),
        ],
        tags=['advanced', 'foreign-key', 'referential-integrity', 'relationship'],
    ))

    # Foreign key cardinality
    features.append(make_feature(
        id='advanced.foreign_key_cardinality',
        name='Foreign Key Cardinality Control',
        category='advanced',
        subcategory='referential_integrity',
        status='stable',
        description='Control the cardinality of foreign key relationships. Set min/max records per parent, ratio multipliers, and distribution patterns.',
        configuration=[
            make_config_option('min', 'integer', 'Minimum records per parent key', scope='plan', yaml_path='foreignKeys[].generate[].cardinality.min'),
            make_config_option('max', 'integer', 'Maximum records per parent key', scope='plan', yaml_path='foreignKeys[].generate[].cardinality.max'),
            make_config_option('ratio', 'double', 'Ratio multiplier (e.g., 10.0 = 10x parent records)', scope='plan', yaml_path='foreignKeys[].generate[].cardinality.ratio'),
            make_config_option('distribution', 'enum', 'Cardinality distribution', default='uniform', scope='plan',
                               yaml_path='foreignKeys[].generate[].cardinality.distribution',
                               valid_values=['uniform', 'normal', 'zipf', 'power']),
        ],
        tags=['advanced', 'cardinality', 'distribution', 'foreign-key'],
    ))

    # Foreign key nullability
    features.append(make_feature(
        id='advanced.foreign_key_nullability',
        name='Foreign Key Nullability',
        category='advanced',
        subcategory='referential_integrity',
        status='stable',
        description='Control null value injection in foreign key fields. Configure percentage of nulls and distribution strategy (random, head, tail).',
        configuration=[
            make_config_option('nullPercentage', 'double', 'Percentage of null values (0-1)', scope='plan',
                               yaml_path='foreignKeys[].generate[].nullability.nullPercentage',
                               range={'min': 0, 'max': 1}),
            make_config_option('strategy', 'enum', 'Null distribution strategy', default='random', scope='plan',
                               yaml_path='foreignKeys[].generate[].nullability.strategy',
                               valid_values=['random', 'leading', 'trailing']),
        ],
        tags=['advanced', 'nullability', 'foreign-key', 'null'],
    ))

    # Foreign key generation modes
    features.append(make_feature(
        id='advanced.foreign_key_generation_modes',
        name='Foreign Key Generation Modes',
        category='advanced',
        subcategory='referential_integrity',
        status='stable',
        description='Control how foreign key values are generated. "all-exist" ensures all records have valid FKs, "all-combinations" generates all possible combinations, "partial" creates a mix of valid and invalid references.',
        configuration=[
            make_config_option('generationMode', 'enum', 'FK generation strategy', default='all-exist', scope='plan',
                               valid_values=['all-exist', 'all-combinations', 'partial']),
        ],
        use_cases=[
            'all-exist: Standard referential integrity testing',
            'all-combinations: Comprehensive join testing with all possible combinations',
            'partial: Testing handling of orphan records and broken references',
        ],
        tags=['advanced', 'foreign-key', 'generation-mode'],
    ))

    # Record count configuration
    features.append(make_feature(
        id='advanced.count',
        name='Record Count Configuration',
        category='advanced',
        subcategory='count',
        status='stable',
        description='Configure how many records to generate per step. Supports fixed count, per-field distribution, and streaming rate-based generation.',
        configuration=[
            make_config_option('records', 'integer', 'Total records to generate', default=1000, scope='step', yaml_path='dataSources[].steps[].count.records'),
            make_config_option('perField', 'object', 'Generate records per unique field value', scope='step', yaml_path='dataSources[].steps[].count.perField'),
        ],
        examples=[
            make_example('yaml', '''count:
  records: 5000''', 'Fixed count'),
            make_example('yaml', '''count:
  records: 100
  perField:
    fieldNames: ["account_id"]
    options:
      min: 1
      max: 5''', 'Per-field count distribution'),
        ],
        tags=['advanced', 'count', 'records', 'distribution'],
    ))

    # Streaming / load patterns
    features.append(make_feature(
        id='advanced.streaming_load_patterns',
        name='Streaming Load Patterns',
        category='advanced',
        subcategory='streaming',
        status='stable',
        description='Define time-based data generation patterns for streaming scenarios. Supports ramp, spike, sine, and custom step patterns.',
        configuration=[
            make_config_option('duration', 'string', 'Streaming duration (e.g., 10m, 1h)', scope='step', yaml_path='dataSources[].steps[].count.duration'),
            make_config_option('rate', 'integer', 'Records per time unit', scope='step', yaml_path='dataSources[].steps[].count.rate'),
            make_config_option('rateUnit', 'enum', 'Time unit for rate', scope='step', yaml_path='dataSources[].steps[].count.rateUnit', valid_values=['second', 'minute', 'hour']),
            make_config_option('pattern.type', 'enum', 'Load pattern type', scope='step', yaml_path='dataSources[].steps[].count.pattern.type', valid_values=['ramp', 'spike', 'sine', 'steps']),
            make_config_option('pattern.startRate', 'integer', 'Starting rate for ramp pattern', scope='step'),
            make_config_option('pattern.endRate', 'integer', 'Ending rate for ramp pattern', scope='step'),
            make_config_option('pattern.baseRate', 'integer', 'Base rate for spike pattern', scope='step'),
            make_config_option('pattern.spikeRate', 'integer', 'Spike rate', scope='step'),
            make_config_option('pattern.amplitude', 'integer', 'Amplitude for sine pattern', scope='step'),
            make_config_option('pattern.frequency', 'double', 'Frequency for sine pattern', scope='step'),
            make_config_option('pattern.steps', 'array', 'Custom step definitions with rate and duration', scope='step'),
        ],
        examples=[
            make_example('yaml', '''count:
  duration: "1m"
  rate: 100
  rateUnit: "second"
  pattern:
    type: "ramp"
    startRate: 10
    endRate: 200''', 'Ramp load pattern'),
        ],
        tags=['advanced', 'streaming', 'load-pattern', 'rate'],
    ))

    # Transformation
    features.append(make_feature(
        id='advanced.transformation',
        name='Post-Generation Transformation',
        category='advanced',
        subcategory='transformation',
        status='stable',
        description='Apply custom Java/Scala transformations to generated data before writing to output. Supports whole-file and row-by-row modes.',
        configuration=[
            make_config_option('className', 'string', 'Fully qualified transformation class name', required=True, scope='step', yaml_path='dataSources[].steps[].transformation.className'),
            make_config_option('methodName', 'string', 'Method to call', default='transform', scope='step'),
            make_config_option('mode', 'enum', 'Transformation mode', scope='step', valid_values=['whole-file', 'row-by-row']),
            make_config_option('outputPath', 'string', 'Output directory', scope='step'),
            make_config_option('deleteOriginal', 'boolean', 'Delete input after transformation', scope='step'),
            make_config_option('enabled', 'boolean', 'Enable/disable transformation', default=True, scope='step'),
        ],
        tags=['advanced', 'transformation', 'custom', 'plugin'],
    ))

    # Step options
    features.append(make_feature(
        id='advanced.step_options',
        name='Step Field Filtering',
        category='advanced',
        subcategory='step_options',
        status='stable',
        description='Include or exclude fields from metadata-driven generation using exact names or patterns.',
        configuration=[
            make_config_option('includeFields', 'array', 'List of field names to include', scope='step', scala_constant='INCLUDE_FIELDS'),
            make_config_option('excludeFields', 'array', 'List of field names to exclude', scope='step', scala_constant='EXCLUDE_FIELDS'),
            make_config_option('includeFieldPatterns', 'array', 'Regex patterns for fields to include', scope='step', scala_constant='INCLUDE_FIELD_PATTERNS'),
            make_config_option('excludeFieldPatterns', 'array', 'Regex patterns for fields to exclude', scope='step', scala_constant='EXCLUDE_FIELD_PATTERNS'),
            make_config_option('allCombinations', 'boolean', 'Generate all field value combinations', scope='step', scala_constant='ALL_COMBINATIONS'),
        ],
        tags=['advanced', 'step', 'filtering', 'metadata'],
    ))

    # Metadata sources
    metadata_sources = [
        ('marquez', 'Marquez', 'Apache Marquez open-source metadata service with OpenLineage support.'),
        ('open_metadata', 'OpenMetadata', 'OpenMetadata platform for metadata discovery. Supports multiple auth types (basic, Azure, Google, Okta, Auth0, AWS Cognito).'),
        ('open_api', 'OpenAPI/Swagger', 'Generate data from OpenAPI/Swagger specifications.'),
        ('great_expectations', 'Great Expectations', 'Import data quality expectations from Great Expectations suites.'),
        ('open_data_contract_standard', 'Open Data Contract Standard', 'Import schemas from ODCS format.'),
        ('data_contract_cli', 'Data Contract CLI', 'Import schemas from Data Contract CLI format.'),
        ('amundsen', 'Amundsen', 'Import metadata from Amundsen data catalog.'),
        ('datahub', 'DataHub', 'Import metadata from DataHub data catalog.'),
        ('confluent_schema_registry', 'Confluent Schema Registry', 'Import schemas from Confluent Schema Registry (Avro, Protobuf, JSON Schema).'),
        ('json_schema', 'JSON Schema', 'Generate data from JSON Schema definitions.'),
    ]

    for src_id, name, desc in metadata_sources:
        features.append(make_feature(
            id=make_feature_id('metadata', 'source', src_id),
            name=f'{name} Integration',
            category='metadata',
            subcategory='sources',
            status='stable',
            description=desc,
            tags=['metadata', 'integration', src_id.replace('_', '-')],
        ))

    # Reference mode
    features.append(make_feature(
        id='advanced.reference_mode',
        name='Reference Mode',
        category='advanced',
        subcategory='reference',
        status='stable',
        description='Load existing data as reference for foreign key relationships instead of generating new data. Useful when you need realistic FK values from existing datasets.',
        configuration=[
            make_config_option('enableReferenceMode', 'boolean', 'Enable reference mode for this data source', default=False, scope='datasource'),
            make_config_option('enableDataGeneration', 'boolean', 'Disable generation (use with reference mode)', default=True, scope='datasource'),
        ],
        tags=['advanced', 'reference', 'existing-data', 'foreign-key'],
    ))

    # Plan and run interfaces
    features.append(make_feature(
        id='advanced.interfaces',
        name='Configuration Interfaces',
        category='advanced',
        subcategory='interfaces',
        status='stable',
        description='Data Caterer supports multiple configuration interfaces: Java API, Scala API, YAML configuration, and Web UI.',
        use_cases=[
            'Java API: Programmatic configuration from Java applications',
            'Scala API: Programmatic configuration with Scala builders',
            'YAML: Declarative configuration for CI/CD and automation',
            'Web UI: Visual configuration and execution management',
        ],
        tags=['advanced', 'interface', 'api', 'yaml', 'ui'],
    ))

    # Environment variable substitution
    features.append(make_feature(
        id='advanced.env_substitution',
        name='Environment Variable Substitution',
        category='advanced',
        subcategory='configuration',
        status='stable',
        description='Use ${VAR_NAME} syntax in YAML configuration to substitute environment variables at runtime. Supports default values with ${VAR:-default}.',
        examples=[
            make_example('yaml', '''options:
  password: "${DB_PASSWORD}"
  url: "${KAFKA_BROKERS:-localhost:9092}"''', 'Environment variable substitution'),
        ],
        tags=['advanced', 'environment', 'variable', 'secrets'],
    ))

    return features


def extract_ui_features() -> list[dict]:
    """Extract UI and API features."""
    features = []

    ui_features = [
        ('ui.connection_management', 'Connection Management', 'Create, edit, test, and manage data source connections through the web UI.'),
        ('ui.plan_creation', 'Interactive Plan Creation', 'Build data generation plans interactively with visual field configuration.'),
        ('ui.execution_history', 'Execution History', 'View past execution runs with status, timing, and record counts.'),
        ('ui.results_viewing', 'Real-time Results', 'View generation and validation results in real-time during execution.'),
        ('ui.sample_data', 'Sample Data Generation', 'Preview generated sample data before running full generation.'),
        ('ui.report_generation', 'Report Generation', 'Generate detailed HTML reports with generation statistics and validation results.'),
    ]

    for fid, name, desc in ui_features:
        features.append(make_feature(
            id=fid,
            name=name,
            category='ui_api',
            subcategory='web_ui',
            status='stable',
            description=desc,
            tags=['ui', 'web', fid.split('.')[-1].replace('_', '-')],
            source_files=[
                {'path': 'app/src/main/scala/io/github/datacatering/datacaterer/core/ui/', 'role': 'primary'},
            ],
        ))

    return features


def build_catalog() -> dict:
    """Build the complete feature catalog."""
    print("Reading source files...")
    constants_content = read_file(CONSTANTS_PATH)
    config_content = read_file(CONFIG_MODELS_PATH)

    print("Extracting features...")
    all_features = []

    # Data source connectors
    constants_vals = parse_scala_lazy_vals(constants_content)
    all_features.extend(extract_data_source_features(constants_vals))
    print(f"  Connectors: {len([f for f in all_features if f['category'] == 'connectors'])}")

    # Field generation
    all_features.extend(extract_field_generation_features())
    all_features.extend(extract_field_option_features())
    all_features.extend(extract_field_label_features())
    gen_count = len([f for f in all_features if f['category'] == 'generation'])
    print(f"  Generation: {gen_count}")

    # Validation
    all_features.extend(extract_validation_features())
    val_count = len([f for f in all_features if f['category'] == 'validation'])
    print(f"  Validation: {val_count}")

    # Configuration
    all_features.extend(extract_configuration_features(constants_content, config_content))
    config_count = len([f for f in all_features if f['category'] == 'configuration'])
    print(f"  Configuration: {config_count}")

    # Advanced features
    all_features.extend(extract_advanced_features())
    adv_count = len([f for f in all_features if f['category'] in ('advanced', 'metadata')])
    print(f"  Advanced + Metadata: {adv_count}")

    # UI features
    all_features.extend(extract_ui_features())
    ui_count = len([f for f in all_features if f['category'] == 'ui_api'])
    print(f"  UI/API: {ui_count}")

    # Build categories summary
    category_counts = {}
    for f in all_features:
        cat = f['category']
        category_counts[cat] = category_counts.get(cat, 0) + 1

    categories = [
        {'id': cat, 'name': cat.replace('_', ' ').title(), 'featureCount': count}
        for cat, count in sorted(category_counts.items())
    ]

    catalog = {
        'project': {
            'name': 'Data Caterer',
            'version': '0.19.0',
            'repository': 'https://github.com/data-catering/data-caterer',
            'lastUpdated': '2026-02-11',
        },
        'categories': categories,
        'features': all_features,
    }

    print(f"\nTotal features extracted: {len(all_features)}")
    return catalog


def main():
    catalog = build_catalog()
    output_path = FEATURE_CATALOG_DIR / "features.json"
    save_json(catalog, output_path)
    print(f"\nFeature catalog saved to: {output_path}")


if __name__ == '__main__':
    main()
