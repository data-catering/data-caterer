# Data Generation

Comprehensive data generation capabilities including regex patterns, faker expressions, SQL computations, and field-level configuration options.

**55 features** in this category.

## Table of Contents

- [Generator Types](#generators) (12 features)
- [Data Types](#data-types) (13 features)
- [Field Options](#field-options) (17 features)
- [Field Labels (Auto-Detection)](#labels) (13 features)

## Generator Types

### Regex Pattern Generation

**ID**: `generation.field.regex`
**Status**: Stable

Generate string values matching a regular expression pattern. Supports SQL-based optimization for common patterns with automatic fallback to UDF for complex patterns (lookaheads, backreferences).

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `regex` | string | Yes | `-` | Regular expression pattern to generate values from YAML: `fields[].options.regex` |

**Examples**:

**Simple regex pattern**:
```yaml
- name: account_id
  options:
    regex: "ACC[0-9]{8}"
```

**Alphanumeric pattern**:
```yaml
- name: product_code
  options:
    regex: "[A-Z]{3}-[0-9]{4}"
```

**Scala API**:
```scala
field.name("account_id").regex("ACC[0-9]{8}")
```

**Source Files**:
- `app/src/main/scala/io/github/datacatering/datacaterer/core/generator/provider/regex/RegexPatternParser.scala` (primary)

**Related Features**:
- `configuration.flags.enable_fast_generation`

**Tags**: `generation`, `string`, `pattern`, `regex`

**Performance Notes**:
- SQL-based optimization available via enableFastGeneration flag
- Complex patterns (lookaheads, backreferences) automatically fall back to UDF

---

### DataFaker Expression

**ID**: `generation.field.expression`
**Status**: Stable

Generate realistic fake data using DataFaker library expressions. Supports names, addresses, emails, phone numbers, and hundreds of other data types.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `expression` | string | Yes | `-` | DataFaker expression (e.g., #{Name.firstName}) YAML: `fields[].options.expression` |

**Examples**:

**Full name generation**:
```yaml
- name: full_name
  options:
    expression: "#{Name.fullName}"
```

**Email generation**:
```yaml
- name: email
  options:
    expression: "#{Internet.emailAddress}"
```

**Scala API**:
```scala
field.name("email").expression("#{Internet.emailAddress}")
```

**Tags**: `generation`, `faker`, `realistic`, `expression`

---

### One-Of Selection

**ID**: `generation.field.one_of`
**Status**: Stable

Generate values by randomly selecting from a predefined list of options. Useful for categorical data like statuses, types, and enums.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `oneOf` | array | Yes | `-` | List of values to randomly select from YAML: `fields[].options.oneOf` |

**Examples**:

**Enum field**:
```yaml
- name: status
  options:
    oneOf: ["active", "inactive", "pending"]
```

**Scala API**:
```scala
field.name("status").oneOf("active", "inactive", "pending")
```

**Tags**: `generation`, `enum`, `categorical`, `selection`

---

### SQL Expression

**ID**: `generation.field.sql`
**Status**: Stable

Generate field values using Spark SQL expressions. Supports referencing other fields, date functions, string operations, aggregations, and conditional logic.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `sql` | string | Yes | `-` | Spark SQL expression for computed value YAML: `fields[].options.sql` |

**Examples**:

**Extract year from date field**:
```yaml
- name: year
  type: integer
  options:
    sql: "YEAR(created_at)"
```

**Concatenate fields**:
```yaml
- name: full_name
  type: string
  options:
    sql: "CONCAT(first_name, ' ', last_name)"
```

**Computed field**:
```yaml
- name: total_amount
  type: double
  options:
    sql: "quantity * unit_price"
```

**Tags**: `generation`, `sql`, `computed`, `derived`

---

### Static Value

**ID**: `generation.field.static`
**Status**: Stable

Set a fixed static value for all generated records. Useful for constant fields like API endpoints, methods, or content types.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `static` | string | Yes | `-` | Fixed value for all records YAML: `fields[].static` |

**Examples**:

**Static HTTP method**:
```yaml
- name: method
  static: "POST"
```

**Tags**: `generation`, `static`, `constant`

---

### UUID Generation

**ID**: `generation.field.uuid`
**Status**: Stable

Generate universally unique identifiers (UUID v4).

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `uuidPattern` | boolean | No | `false` | Enable UUID generation YAML: `fields[].options.uuidPattern` |

**Examples**:

**UUID field**:
```yaml
- name: id
  options:
    uuidPattern: true
```

**Tags**: `generation`, `uuid`, `identifier`, `unique`

---

### Sequential Value Generation

**ID**: `generation.field.sequence`
**Status**: Stable

Generate sequential values with optional prefix and padding. Useful for IDs, batch numbers, and sequential identifiers.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `sequence` | object | Yes | `-` | Sequential value configuration with prefix and padding YAML: `fields[].options.sequence` |

**Examples**:

**Sequential order IDs**:
```yaml
- name: order_id
  options:
    sequence:
      start: 1000
      step: 1
      prefix: "ORD-"
      padding: 8
```

**Tags**: `generation`, `sequence`, `sequential`, `incremental`

---

### Conditional Value Generation

**ID**: `generation.field.conditional_value`
**Status**: Stable

Generate values using CASE WHEN logic based on other field values. Enables dependent field generation.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `conditionalValue` | object | Yes | `-` | CASE WHEN conditions and result values YAML: `fields[].options.conditionalValue` |

**Examples**:

**Conditional discount**:
```yaml
- name: discount
  type: double
  options:
    conditionalValue:
      conditions:
        - expr: "customer_type = 'premium'"
          value: 0.2
        - expr: "customer_type = 'standard'"
          value: 0.1
      default: 0.0
```

**Tags**: `generation`, `conditional`, `logic`, `derived`

---

### Correlated Field Generation

**ID**: `generation.field.correlated`
**Status**: Stable

Generate values that are correlated (or negatively correlated) with another field. Useful for creating realistic relationships between numeric fields.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `correlatedWith` | string | No | `-` | Field name to correlate with YAML: `fields[].options.correlatedWith` |
| `negativelyCorrelatedWith` | string | No | `-` | Field name to negatively correlate with YAML: `fields[].options.negativelyCorrelatedWith` |

**Examples**:

**Positively correlated fields**:
```yaml
- name: revenue
  type: double
  options:
    correlatedWith: "customer_count"
```

**Tags**: `generation`, `correlation`, `statistical`, `relationship`

---

### Value Mapping

**ID**: `generation.field.mapping`
**Status**: Stable

Map values from one field to generate deterministic output in another field.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `mapping` | object | Yes | `-` | Mapping configuration from source field to output values YAML: `fields[].options.mapping` |

**Examples**:

**Country code mapping**:
```yaml
- name: country_code
  options:
    mapping:
      sourceField: "country"
      mappings:
        "United States": "US"
        "United Kingdom": "UK"
```

**Tags**: `generation`, `mapping`, `lookup`, `derived`

---

### Semantic Version Generation

**ID**: `generation.field.semantic_version`
**Status**: Stable

Generate semantic version strings (e.g., 1.2.3).

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `semanticVersion` | object | No | `-` | Semantic version configuration YAML: `fields[].options.semanticVersion` |

**Tags**: `generation`, `version`, `semver`

---

### Daily Batch Sequence

**ID**: `generation.field.daily_batch_sequence`
**Status**: Stable

Generate daily batch sequence identifiers.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `dailyBatchSequence` | object | No | `-` | Daily batch sequence configuration YAML: `fields[].options.dailyBatchSequence` |

**Tags**: `generation`, `batch`, `daily`, `sequence`

---

## Data Types

### String Type

**ID**: `generation.type.string`
**Status**: Stable

Text data type. Default field type.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set field type to "string" YAML: `fields[].type` |

**Tags**: `generation`, `type`, `string`

---

### Integer Type

**ID**: `generation.type.integer`
**Status**: Stable

32-bit integer values.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set field type to "integer" YAML: `fields[].type` |

**Tags**: `generation`, `type`, `integer`

---

### Long Type

**ID**: `generation.type.long`
**Status**: Stable

64-bit long integer values.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set field type to "long" YAML: `fields[].type` |

**Tags**: `generation`, `type`, `long`

---

### Double Type

**ID**: `generation.type.double`
**Status**: Stable

Double-precision floating point values.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set field type to "double" YAML: `fields[].type` |

**Tags**: `generation`, `type`, `double`

---

### Float Type

**ID**: `generation.type.float`
**Status**: Stable

Single-precision floating point values.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set field type to "float" YAML: `fields[].type` |

**Tags**: `generation`, `type`, `float`

---

### Decimal Type

**ID**: `generation.type.decimal`
**Status**: Stable

Fixed-precision decimal values with configurable precision and scale.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set field type to "decimal" YAML: `fields[].type` |

**Tags**: `generation`, `type`, `decimal`

---

### Boolean Type

**ID**: `generation.type.boolean`
**Status**: Stable

True/false boolean values.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set field type to "boolean" YAML: `fields[].type` |

**Tags**: `generation`, `type`, `boolean`

---

### Date Type

**ID**: `generation.type.date`
**Status**: Stable

Date values (year-month-day).

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set field type to "date" YAML: `fields[].type` |

**Tags**: `generation`, `type`, `date`

---

### Timestamp Type

**ID**: `generation.type.timestamp`
**Status**: Stable

Timestamp values with date and time.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set field type to "timestamp" YAML: `fields[].type` |

**Tags**: `generation`, `type`, `timestamp`

---

### Binary Type

**ID**: `generation.type.binary`
**Status**: Stable

Binary byte array values.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set field type to "binary" YAML: `fields[].type` |

**Tags**: `generation`, `type`, `binary`

---

### Array Type

**ID**: `generation.type.array`
**Status**: Stable

Array/list of elements. Configurable element type, min/max length.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set field type to "array" YAML: `fields[].type` |

**Tags**: `generation`, `type`, `array`

---

### Struct Type

**ID**: `generation.type.struct`
**Status**: Stable

Nested structure with named fields. Supports deep nesting.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set field type to "struct" YAML: `fields[].type` |

**Tags**: `generation`, `type`, `struct`

---

### Map Type

**ID**: `generation.type.map`
**Status**: Stable

Key-value map type with configurable key and value types.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set field type to "map" YAML: `fields[].type` |

**Tags**: `generation`, `type`, `map`

---

## Field Options

### Numeric Range

**ID**: `generation.option.numeric_range`
**Status**: Stable

Constrain numeric fields (integer, long, double, float, decimal) to a minimum and maximum range.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `min` | any | No | `-` | Minimum value (inclusive) YAML: `fields[].options.min` |
| `max` | any | No | `-` | Maximum value (inclusive) YAML: `fields[].options.max` |

**Examples**:

**Integer range**:
```yaml
- name: age
  type: integer
  options:
    min: 18
    max: 120
```

**Double range**:
```yaml
- name: price
  type: double
  options:
    min: 9.99
    max: 999.99
```

**Tags**: `generation`, `numeric`, `range`, `constraint`

---

### Date/Time Range

**ID**: `generation.option.date_range`
**Status**: Stable

Constrain date and timestamp fields to a minimum and maximum range. Also supports excluding weekends, business hours, within/future days.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `min` | string | No | `-` | Minimum date/timestamp YAML: `fields[].options.min` |
| `max` | string | No | `-` | Maximum date/timestamp YAML: `fields[].options.max` |
| `excludeWeekends` | boolean | No | `false` | Exclude Saturday and Sunday YAML: `fields[].options.excludeWeekends` |
| `withinDays` | integer | No | `-` | Generate dates within last N days from now YAML: `fields[].options.withinDays` |
| `futureDays` | integer | No | `-` | Generate dates within next N days from now YAML: `fields[].options.futureDays` |
| `businessHours` | boolean | No | `false` | Restrict to business hours YAML: `fields[].options.businessHours` |
| `timeBetween` | object | No | `-` | Generate times between start and end YAML: `fields[].options.timeBetween` |

**Examples**:

**Timestamp range**:
```yaml
- name: created_at
  type: timestamp
  options:
    min: "2024-01-01T00:00:00"
    max: "2024-12-31T23:59:59"
```

**Tags**: `generation`, `date`, `timestamp`, `range`

---

### Null Value Control

**ID**: `generation.option.null_handling`
**Status**: Stable

Control whether and how often null values appear in generated data. Configurable null probability per field.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableNull` | boolean | No | `false` | Allow null values for this field YAML: `fields[].options.enableNull` |
| `nullProb` | double | No | `-` | Probability of generating a null value (0-1) YAML: `fields[].options.nullProb` |
| `nullable` | boolean | No | `true` | Whether the field schema allows nulls YAML: `fields[].nullable` |

**Examples**:

**30% null probability**:
```yaml
- name: middle_name
  options:
    enableNull: true
    nullProb: 0.3
```

**Tags**: `generation`, `null`, `nullable`, `probability`

---

### Edge Case Generation

**ID**: `generation.option.edge_cases`
**Status**: Stable

Control the probability of generating edge case values (empty strings, boundary values, special characters).

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableEdgeCase` | boolean | No | `false` | Enable edge case generation |
| `edgeCaseProb` | double | No | `-` | Probability of generating edge case values (0-1) |

**Tags**: `generation`, `edge-case`, `boundary`, `testing`

---

### String Length Control

**ID**: `generation.option.string_length`
**Status**: Stable

Control the length of generated string values with minimum, maximum, and average length.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `minLen` | integer | No | `-` | Minimum string length |
| `maxLen` | integer | No | `-` | Maximum string length |
| `avgLen` | integer | No | `-` | Average string length |

**Tags**: `generation`, `string`, `length`, `constraint`

---

### Array Configuration

**ID**: `generation.option.array_config`
**Status**: Stable

Configure array field generation: element count, element type, uniqueness, empty probability, and weighted selection.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `arrayMinLen` | integer | No | `-` | Minimum array length YAML: `fields[].options.arrayMinLength` |
| `arrayMaxLen` | integer | No | `-` | Maximum array length YAML: `fields[].options.arrayMaxLength` |
| `arrayFixedSize` | integer | No | `-` | Fixed array size |
| `arrayEmptyProb` | double | No | `-` | Probability of empty array (0-1) YAML: `fields[].options.arrayEmptyProbability` |
| `arrayType` | string | No | `-` | Element data type for array |
| `arrayOneOf` | string | No | `-` | Comma-separated values for array elements |
| `arrayUniqueFrom` | string | No | `-` | Source for unique array elements |
| `arrayWeightedOneOf` | string | No | `-` | Weighted selection for elements (e.g., HIGH:0.2,MEDIUM:0.5,LOW:0.3) YAML: `fields[].options.arrayWeightedOneOf` |

**Tags**: `generation`, `array`, `collection`, `nested`

---

### Map Configuration

**ID**: `generation.option.map_config`
**Status**: Stable

Configure map field generation with minimum and maximum size.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `mapMinSize` | integer | No | `-` | Minimum number of entries |
| `mapMaxSize` | integer | No | `-` | Maximum number of entries |

**Tags**: `generation`, `map`, `key-value`, `nested`

---

### Value Distribution

**ID**: `generation.option.distribution`
**Status**: Stable

Control the statistical distribution of generated numeric values. Supports uniform, normal, and exponential distributions.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `distribution` | enum | No | `-` | Distribution type Values: `uniform`, `normal`, `exponential` |
| `mean` | double | No | `-` | Mean for normal distribution |
| `stddev` | double | No | `-` | Standard deviation for normal distribution |
| `distributionRateParam` | double | No | `-` | Rate parameter for exponential distribution |

**Tags**: `generation`, `distribution`, `statistical`, `normal`, `uniform`

---

### Uniqueness Constraint

**ID**: `generation.option.uniqueness`
**Status**: Stable

Enforce unique values for a field using bloom filter-based deduplication.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `isUnique` | boolean | No | `false` | Enforce unique values YAML: `fields[].options.isUnique` |
| `isPrimaryKey` | boolean | No | `false` | Mark as primary key (implies unique) YAML: `fields[].options.isPrimaryKey` |
| `primaryKeyPos` | integer | No | `-` | Position in composite primary key |

**Related Features**:
- `configuration.flags.enable_unique_check`

**Tags**: `generation`, `unique`, `primary-key`, `constraint`

---

### Numeric Precision and Scale

**ID**: `generation.option.numeric_precision`
**Status**: Stable

Control precision and scale for decimal fields, and rounding for numeric fields.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `precision` | integer | No | `-` | Numeric precision (total digits) |
| `scale` | integer | No | `-` | Numeric scale (decimal places) |
| `round` | integer | No | `-` | Round numeric values to N decimal places |

**Tags**: `generation`, `numeric`, `precision`, `decimal`

---

### Field Omission

**ID**: `generation.option.omit`
**Status**: Stable

Generate a field for use in computed expressions but omit it from the final output.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `omit` | boolean | No | `false` | Omit field from output YAML: `fields[].options.omit` |

**Tags**: `generation`, `omit`, `helper`, `computed`

---

### Random Seed

**ID**: `generation.option.seed`
**Status**: Stable

Set a random seed for reproducible data generation per field.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `seed` | integer | No | `-` | Random seed for reproducible generation YAML: `fields[].options.seed` |

**Tags**: `generation`, `seed`, `reproducible`, `deterministic`

---

### Distinct Value Count

**ID**: `generation.option.distinct_count`
**Status**: Stable

Control how many distinct values are generated for a field. Used with metadata-driven generation.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `distinctCount` | integer | No | `-` | Number of distinct values to generate |
| `histogram` | object | No | `-` | Value distribution histogram |

**Tags**: `generation`, `distinct`, `cardinality`, `metadata`

---

### Cassandra Key Configuration

**ID**: `generation.option.cassandra_keys`
**Status**: Stable

Configure Cassandra-specific primary key and clustering positions for fields.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `isPrimaryKey` | boolean | No | `-` | Mark as partition key |
| `primaryKeyPos` | integer | No | `-` | Position in composite partition key |
| `clusteringPos` | integer | No | `-` | Clustering column position |

**Tags**: `generation`, `cassandra`, `primary-key`, `clustering`

---

### Incremental Generation

**ID**: `generation.option.incremental`
**Status**: Stable

Mark a field for incremental generation, tracking the last generated value across runs.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `incremental` | boolean | No | `false` | Enable incremental mode |

**Tags**: `generation`, `incremental`, `tracking`

---

### HTTP Parameter Type

**ID**: `generation.option.http_param_type`
**Status**: Stable

Specify the HTTP parameter type for a field when using the HTTP connector (path, query, or header).

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `httpParamType` | enum | No | `-` | HTTP parameter placement Values: `path`, `query`, `header` |

**Tags**: `generation`, `http`, `parameter`, `api`

---

### Post-SQL Expression

**ID**: `generation.option.post_sql_expression`
**Status**: Stable

Apply a SQL expression to transform the field value after initial generation.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `postSqlExpression` | string | No | `-` | SQL expression to apply after generation |

**Tags**: `generation`, `sql`, `transform`, `post-processing`

---

## Field Labels (Auto-Detection)

### Name Label

**ID**: `generation.label.name`
**Status**: Stable

Generate person name fields (first name, last name, full name). Used for metadata-driven field generation to automatically select appropriate data generators.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `label` | string | No | `-` | Set field label to "name" for auto-detection |

**Tags**: `generation`, `label`, `metadata`, `name`

---

### Username Label

**ID**: `generation.label.username`
**Status**: Stable

Generate username fields. Used for metadata-driven field generation to automatically select appropriate data generators.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `label` | string | No | `-` | Set field label to "username" for auto-detection |

**Tags**: `generation`, `label`, `metadata`, `username`

---

### Address Label

**ID**: `generation.label.address`
**Status**: Stable

Generate address fields (street, city, postcode). Used for metadata-driven field generation to automatically select appropriate data generators.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `label` | string | No | `-` | Set field label to "address" for auto-detection |

**Tags**: `generation`, `label`, `metadata`, `address`

---

### Application Label

**ID**: `generation.label.app`
**Status**: Stable

Generate application-related fields (version). Used for metadata-driven field generation to automatically select appropriate data generators.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `label` | string | No | `-` | Set field label to "app" for auto-detection |

**Tags**: `generation`, `label`, `metadata`, `app`

---

### Nation Label

**ID**: `generation.label.nation`
**Status**: Stable

Generate nationality, language, capital city. Used for metadata-driven field generation to automatically select appropriate data generators.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `label` | string | No | `-` | Set field label to "nation" for auto-detection |

**Tags**: `generation`, `label`, `metadata`, `nation`

---

### Money Label

**ID**: `generation.label.money`
**Status**: Stable

Generate currency and financial fields. Used for metadata-driven field generation to automatically select appropriate data generators.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `label` | string | No | `-` | Set field label to "money" for auto-detection |

**Tags**: `generation`, `label`, `metadata`, `money`

---

### Internet Label

**ID**: `generation.label.internet`
**Status**: Stable

Generate email, IP, MAC address fields. Used for metadata-driven field generation to automatically select appropriate data generators.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `label` | string | No | `-` | Set field label to "internet" for auto-detection |

**Tags**: `generation`, `label`, `metadata`, `internet`

---

### Food Label

**ID**: `generation.label.food`
**Status**: Stable

Generate food and ingredient fields. Used for metadata-driven field generation to automatically select appropriate data generators.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `label` | string | No | `-` | Set field label to "food" for auto-detection |

**Tags**: `generation`, `label`, `metadata`, `food`

---

### Job Label

**ID**: `generation.label.job`
**Status**: Stable

Generate job title, field, position. Used for metadata-driven field generation to automatically select appropriate data generators.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `label` | string | No | `-` | Set field label to "job" for auto-detection |

**Tags**: `generation`, `label`, `metadata`, `job`

---

### Relationship Label

**ID**: `generation.label.relationship`
**Status**: Stable

Generate relationship type fields. Used for metadata-driven field generation to automatically select appropriate data generators.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `label` | string | No | `-` | Set field label to "relationship" for auto-detection |

**Tags**: `generation`, `label`, `metadata`, `relationship`

---

### Weather Label

**ID**: `generation.label.weather`
**Status**: Stable

Generate weather description fields. Used for metadata-driven field generation to automatically select appropriate data generators.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `label` | string | No | `-` | Set field label to "weather" for auto-detection |

**Tags**: `generation`, `label`, `metadata`, `weather`

---

### Phone Label

**ID**: `generation.label.phone`
**Status**: Stable

Generate phone number fields. Used for metadata-driven field generation to automatically select appropriate data generators.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `label` | string | No | `-` | Set field label to "phone" for auto-detection |

**Tags**: `generation`, `label`, `metadata`, `phone`

---

### Geo Label

**ID**: `generation.label.geo`
**Status**: Stable

Generate geographic coordinate fields. Used for metadata-driven field generation to automatically select appropriate data generators.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `label` | string | No | `-` | Set field label to "geo" for auto-detection |

**Tags**: `generation`, `label`, `metadata`, `geo`

---
