# Data Validation

Over 30 validation types for verifying generated data quality, schema compliance, statistical properties, and cross-source consistency.

**42 features** in this category.

## Table of Contents

- [Field-Level Validations](#field-validations) (29 features)
- [Statistical Validations](#statistical-validations) (8 features)
- [Expression Validations](#expression-validations) (1 features)
- [Aggregation Validations](#aggregation-validations) (1 features)
- [Cross-Source Validations](#cross-source-validations) (1 features)
- [Schema Validations](#schema-validations) (1 features)
- [Wait Conditions](#wait-conditions) (1 features)

## Field-Level Validations

### Null Check

**ID**: `validation.field.null`
**Status**: Stable

Validate that a field is null (or not null with negate=true).

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "null" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `null`

---

### Unique Values

**ID**: `validation.field.unique`
**Status**: Stable

Validate that all values in a field are unique.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "unique" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `unique`

---

### Equality Check

**ID**: `validation.field.equal`
**Status**: Stable

Validate that field values equal a specified value.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "equal" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `equal`

---

### Contains Check

**ID**: `validation.field.contains`
**Status**: Stable

Validate that string field values contain a substring.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "contains" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `contains`

---

### Starts With

**ID**: `validation.field.starts_with`
**Status**: Stable

Validate that string field values start with a prefix.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "startswith" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `starts-with`

---

### Ends With

**ID**: `validation.field.ends_with`
**Status**: Stable

Validate that string field values end with a suffix.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "endswith" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `ends-with`

---

### Less Than

**ID**: `validation.field.less_than`
**Status**: Stable

Validate that numeric values are less than a threshold.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "lessthan" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `less-than`

---

### Greater Than

**ID**: `validation.field.greater_than`
**Status**: Stable

Validate that numeric values are greater than a threshold.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "greaterthan" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `greater-than`

---

### Between Range

**ID**: `validation.field.between`
**Status**: Stable

Validate that values fall within a min/max range (inclusive).

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "between" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `between`

---

### In Set

**ID**: `validation.field.in`
**Status**: Stable

Validate that values exist in a specified set of allowed values.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "in" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `in`

---

### Regex Match

**ID**: `validation.field.matches`
**Status**: Stable

Validate that string values match a regular expression pattern.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "matches" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `matches`

---

### Regex Match List

**ID**: `validation.field.matches_list`
**Status**: Stable

Validate that string values match one of multiple regex patterns.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "matcheslist" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `matches-list`

---

### Size Check

**ID**: `validation.field.size`
**Status**: Stable

Validate the size/length of a collection or string field.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "size" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `size`

---

### Less Than Size

**ID**: `validation.field.less_than_size`
**Status**: Stable

Validate that collection size is less than a threshold.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "lessthansize" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `less-than-size`

---

### Greater Than Size

**ID**: `validation.field.greater_than_size`
**Status**: Stable

Validate that collection size is greater than a threshold.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "greaterthansize" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `greater-than-size`

---

### Length Between

**ID**: `validation.field.length_between`
**Status**: Stable

Validate that string length falls within a range.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "lengthbetween" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `length-between`

---

### Length Equal

**ID**: `validation.field.length_equal`
**Status**: Stable

Validate that string length equals a specific value.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "lengthequal" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `length-equal`

---

### Luhn Check

**ID**: `validation.field.luhn_check`
**Status**: Stable

Validate values using the Luhn algorithm (credit cards, IDs).

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "luhncheck" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `luhn-check`

---

### Type Check

**ID**: `validation.field.has_type`
**Status**: Stable

Validate that field values are of a specific data type.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "hastype" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `has-type`

---

### Multi-Type Check

**ID**: `validation.field.has_types`
**Status**: Stable

Validate that field values match one of multiple types.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "hastypes" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `has-types`

---

### Monotonically Decreasing

**ID**: `validation.field.is_decreasing`
**Status**: Stable

Validate that values are in decreasing order.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "isdecreasing" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `is-decreasing`

---

### Monotonically Increasing

**ID**: `validation.field.is_increasing`
**Status**: Stable

Validate that values are in increasing order.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "isincreasing" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `is-increasing`

---

### JSON Parsable

**ID**: `validation.field.is_json_parsable`
**Status**: Stable

Validate that string values are valid JSON.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "isjsonparsable" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `is-json-parsable`

---

### JSON Schema Match

**ID**: `validation.field.match_json_schema`
**Status**: Stable

Validate that JSON values conform to a JSON schema.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "matchjsonschema" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `match-json-schema`

---

### DateTime Format Match

**ID**: `validation.field.match_date_time_format`
**Status**: Stable

Validate that values match a specific date/time format.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "matchdatetimeformat" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `match-date-time-format`

---

### Distinct In Set

**ID**: `validation.field.distinct_in_set`
**Status**: Stable

Validate that all distinct values exist in a specified set.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "distinctinset" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `distinct-in-set`

---

### Distinct Contains Set

**ID**: `validation.field.distinct_contains_set`
**Status**: Stable

Validate that distinct values contain all values from a specified set.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "distinctcontainsset" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `distinct-contains-set`

---

### Distinct Equal

**ID**: `validation.field.distinct_equal`
**Status**: Stable

Validate that the set of distinct values exactly equals a specified set.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "distinctequal" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `distinct-equal`

---

### Most Common Value

**ID**: `validation.field.most_common_value_in_set`
**Status**: Stable

Validate that the most common value is in a specified set.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type to "mostcommonvalueinset" |
| `negate` | boolean | No | `false` | Invert the validation result |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |
| `description` | string | No | `-` | Human-readable description |

**Tags**: `validation`, `field`, `most-common-value-in-set`

---

## Statistical Validations

### Max Between

**ID**: `validation.statistical.max_between`
**Status**: Stable

Validate that the maximum value of a field falls within a range.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type |
| `min` | any | No | `-` | Minimum expected value |
| `max` | any | No | `-` | Maximum expected value |

**Tags**: `validation`, `statistical`, `max-between`

---

### Mean Between

**ID**: `validation.statistical.mean_between`
**Status**: Stable

Validate that the mean value of a field falls within a range.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type |
| `min` | any | No | `-` | Minimum expected value |
| `max` | any | No | `-` | Maximum expected value |

**Tags**: `validation`, `statistical`, `mean-between`

---

### Median Between

**ID**: `validation.statistical.median_between`
**Status**: Stable

Validate that the median value of a field falls within a range.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type |
| `min` | any | No | `-` | Minimum expected value |
| `max` | any | No | `-` | Maximum expected value |

**Tags**: `validation`, `statistical`, `median-between`

---

### Min Between

**ID**: `validation.statistical.min_between`
**Status**: Stable

Validate that the minimum value of a field falls within a range.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type |
| `min` | any | No | `-` | Minimum expected value |
| `max` | any | No | `-` | Maximum expected value |

**Tags**: `validation`, `statistical`, `min-between`

---

### Std Dev Between

**ID**: `validation.statistical.std_dev_between`
**Status**: Stable

Validate that the standard deviation of a field falls within a range.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type |
| `min` | any | No | `-` | Minimum expected value |
| `max` | any | No | `-` | Maximum expected value |

**Tags**: `validation`, `statistical`, `std-dev-between`

---

### Sum Between

**ID**: `validation.statistical.sum_between`
**Status**: Stable

Validate that the sum of a field falls within a range.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type |
| `min` | any | No | `-` | Minimum expected value |
| `max` | any | No | `-` | Maximum expected value |

**Tags**: `validation`, `statistical`, `sum-between`

---

### Unique Values Proportion

**ID**: `validation.statistical.unique_values_proportion_between`
**Status**: Stable

Validate that the proportion of unique values falls within a range.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type |
| `min` | any | No | `-` | Minimum expected value |
| `max` | any | No | `-` | Maximum expected value |

**Tags**: `validation`, `statistical`, `unique-values-proportion-between`

---

### Quantile Values Between

**ID**: `validation.statistical.quantile_values_between`
**Status**: Stable

Validate that quantile values fall within specified ranges.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | string | No | `-` | Set validation type |
| `min` | any | No | `-` | Minimum expected value |
| `max` | any | No | `-` | Maximum expected value |

**Tags**: `validation`, `statistical`, `quantile-values-between`

---

## Expression Validations

### SQL Expression Validation

**ID**: `validation.expression`
**Status**: Stable

Validate data using arbitrary Spark SQL expressions that must evaluate to true. The most flexible validation type.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `expr` | string | Yes | `-` | SQL expression that must evaluate to true YAML: `validations[].expr` |
| `selectExpr` | array | No | `-` | SELECT columns for the expression YAML: `validations[].selectExpr` |
| `preFilterExpr` | string | No | `-` | SQL filter to apply before validation YAML: `validations[].preFilterExpr` |
| `description` | string | No | `-` | Human-readable description |
| `errorThreshold` | double | No | `-` | Allowed error rate (0-1) |

**Examples**:

**Expression validation**:
```yaml
validations:
  - expr: "age >= 18 AND age <= 120"
    description: "Age must be valid"
```

**Tags**: `validation`, `expression`, `sql`, `flexible`

---

## Aggregation Validations

### Group By Aggregation Validation

**ID**: `validation.group_by`
**Status**: Stable

Validate aggregated data grouped by specified fields. Supports sum, avg, min, max, count, and stddev aggregations.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `groupByFields` | array | Yes | `-` | Fields to group by YAML: `validations[].groupByFields` |
| `aggField` | string | No | `-` | Field to aggregate YAML: `validations[].aggField` |
| `aggType` | enum | No | `-` | Aggregation function Values: `sum`, `avg`, `min`, `max`, `count`, `stddev` YAML: `validations[].aggType` |
| `aggExpr` | string | No | `-` | Custom aggregation expression YAML: `validations[].aggExpr` |

**Examples**:

**Group by validation**:
```yaml
validations:
  - groupByFields: ["status"]
    aggField: "balance"
    aggType: "avg"
    aggExpr: "avg_balance > 0"
    description: "Average balance per status"
```

**Tags**: `validation`, `aggregation`, `group-by`, `statistical`

---

## Cross-Source Validations

### Upstream Cross-Source Validation

**ID**: `validation.upstream`
**Status**: Stable

Validate data by joining with an upstream data source. Enables cross-system data consistency checks.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `upstreamDataSource` | string | Yes | `-` | Upstream data source name YAML: `validations[].upstreamDataSource` |
| `upstreamReadOptions` | object | No | `-` | Read options for upstream source YAML: `validations[].upstreamReadOptions` |
| `joinFields` | array | No | `-` | Fields to join on YAML: `validations[].joinFields` |
| `joinType` | enum | No | `outer` | Join type Values: `inner`, `left`, `right`, `full`, `anti`, `semi` YAML: `validations[].joinType` |

**Examples**:

**Cross-source validation**:
```yaml
validations:
  - upstreamDataSource: "source_json"
    joinFields: ["account_id"]
    joinType: "outer"
    validations:
      - expr: "source_json_name == name"
```

**Tags**: `validation`, `upstream`, `cross-source`, `join`

---

## Schema Validations

### Schema Field Names Validation

**ID**: `validation.field_names`
**Status**: Stable

Validate the schema structure by checking field/column names, counts, and ordering.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `names` | array | No | `-` | Expected field names YAML: `validations[].names` |
| `fieldNameType` | enum | No | `-` | Validation type for field names Values: `fieldCountEqual`, `fieldCountBetween`, `fieldNameMatchOrder`, `fieldNameMatchSet` |
| `count` | integer | No | `-` | Expected exact field count |
| `min` | integer | No | `-` | Minimum field count |
| `max` | integer | No | `-` | Maximum field count |

**Tags**: `validation`, `schema`, `field-names`, `structure`

---

## Wait Conditions

### Wait Conditions

**ID**: `validation.wait_condition`
**Status**: Stable

Define conditions to wait for before running validations. Supports pause, file existence, data existence, and webhook checks.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | enum | No | `-` | Wait condition type Values: `pause`, `fileExists`, `dataExists`, `webhook` YAML: `validations[].waitCondition.type` |
| `pauseInSeconds` | integer | No | `-` | Seconds to pause |
| `path` | string | No | `-` | File path to wait for |
| `url` | string | No | `-` | Webhook URL |
| `method` | enum | No | `-` | HTTP method for webhook Values: `GET`, `POST`, `PUT`, `DELETE` |
| `statusCodes` | array | No | `-` | Expected HTTP status codes |
| `maxRetries` | integer | No | `-` | Maximum retry attempts |
| `waitBeforeRetrySeconds` | integer | No | `-` | Seconds between retries |

**Tags**: `validation`, `wait`, `condition`, `async`

---
