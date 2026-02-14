# Advanced Features

Foreign key relationships, streaming load patterns, custom transformations, metadata-driven generation, and more.

**11 features** in this category.

## Table of Contents

- [Foreign Key Relationships](#referential-integrity) (4 features)
- [Record Count](#count) (1 features)
- [Streaming Settings](#streaming) (1 features)
- [Transformation](#transformation) (1 features)
- [Step Options](#step-options) (1 features)
- [Reference Mode](#reference) (1 features)
- [Interfaces](#interfaces) (1 features)
- [Configuration](#configuration) (1 features)

## Foreign Key Relationships

### Foreign Key Relationships

**ID**: `advanced.foreign_keys`
**Status**: Stable

Define foreign key relationships between data sources to maintain referential integrity. Supports composite keys, cardinality control, nullability, and multiple generation modes.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `source` | object | Yes | `-` | Source table containing primary key YAML: `foreignKeys[].source` |
| `generate` | array | No | `-` | Target tables with foreign key references YAML: `foreignKeys[].generate` |
| `delete` | array | No | `-` | Target tables for cleanup YAML: `foreignKeys[].delete` |

**Examples**:

**Foreign key with cardinality**:
```yaml
foreignKeys:
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
          distribution: "uniform"
```

**Tags**: `advanced`, `foreign-key`, `referential-integrity`, `relationship`

---

### Foreign Key Cardinality Control

**ID**: `advanced.foreign_key_cardinality`
**Status**: Stable

Control the cardinality of foreign key relationships. Set min/max records per parent, ratio multipliers, and distribution patterns.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `min` | integer | No | `-` | Minimum records per parent key YAML: `foreignKeys[].generate[].cardinality.min` |
| `max` | integer | No | `-` | Maximum records per parent key YAML: `foreignKeys[].generate[].cardinality.max` |
| `ratio` | double | No | `-` | Ratio multiplier (e.g., 10.0 = 10x parent records) YAML: `foreignKeys[].generate[].cardinality.ratio` |
| `distribution` | enum | No | `uniform` | Cardinality distribution Values: `uniform`, `normal`, `zipf`, `power` YAML: `foreignKeys[].generate[].cardinality.distribution` |

**Tags**: `advanced`, `cardinality`, `distribution`, `foreign-key`

---

### Foreign Key Nullability

**ID**: `advanced.foreign_key_nullability`
**Status**: Stable

Control null value injection in foreign key fields. Configure percentage of nulls and distribution strategy (random, head, tail).

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `nullPercentage` | double | No | `-` | Percentage of null values (0-1) YAML: `foreignKeys[].generate[].nullability.nullPercentage` |
| `strategy` | enum | No | `random` | Null distribution strategy Values: `random`, `leading`, `trailing` YAML: `foreignKeys[].generate[].nullability.strategy` |

**Tags**: `advanced`, `nullability`, `foreign-key`, `null`

---

### Foreign Key Generation Modes

**ID**: `advanced.foreign_key_generation_modes`
**Status**: Stable

Control how foreign key values are generated. "all-exist" ensures all records have valid FKs, "all-combinations" generates all possible combinations, "partial" creates a mix of valid and invalid references.

**Use Cases**:
- all-exist: Standard referential integrity testing
- all-combinations: Comprehensive join testing with all possible combinations
- partial: Testing handling of orphan records and broken references

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `generationMode` | enum | No | `all-exist` | FK generation strategy Values: `all-exist`, `all-combinations`, `partial` |

**Tags**: `advanced`, `foreign-key`, `generation-mode`

---

## Record Count

### Record Count Configuration

**ID**: `advanced.count`
**Status**: Stable

Configure how many records to generate per step. Supports fixed count, per-field distribution, and streaming rate-based generation.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `records` | integer | No | `1000` | Total records to generate YAML: `dataSources[].steps[].count.records` |
| `perField` | object | No | `-` | Generate records per unique field value YAML: `dataSources[].steps[].count.perField` |

**Examples**:

**Fixed count**:
```yaml
count:
  records: 5000
```

**Per-field count distribution**:
```yaml
count:
  records: 100
  perField:
    fieldNames: ["account_id"]
    options:
      min: 1
      max: 5
```

**Tags**: `advanced`, `count`, `records`, `distribution`

---

## Streaming Settings

### Streaming Load Patterns

**ID**: `advanced.streaming_load_patterns`
**Status**: Stable

Define time-based data generation patterns for streaming scenarios. Supports ramp, spike, sine, and custom step patterns.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `duration` | string | No | `-` | Streaming duration (e.g., 10m, 1h) YAML: `dataSources[].steps[].count.duration` |
| `rate` | integer | No | `-` | Records per time unit YAML: `dataSources[].steps[].count.rate` |
| `rateUnit` | enum | No | `-` | Time unit for rate Values: `second`, `minute`, `hour` YAML: `dataSources[].steps[].count.rateUnit` |
| `pattern.type` | enum | No | `-` | Load pattern type Values: `ramp`, `spike`, `sine`, `steps` YAML: `dataSources[].steps[].count.pattern.type` |
| `pattern.startRate` | integer | No | `-` | Starting rate for ramp pattern |
| `pattern.endRate` | integer | No | `-` | Ending rate for ramp pattern |
| `pattern.baseRate` | integer | No | `-` | Base rate for spike pattern |
| `pattern.spikeRate` | integer | No | `-` | Spike rate |
| `pattern.amplitude` | integer | No | `-` | Amplitude for sine pattern |
| `pattern.frequency` | double | No | `-` | Frequency for sine pattern |
| `pattern.steps` | array | No | `-` | Custom step definitions with rate and duration |

**Examples**:

**Ramp load pattern**:
```yaml
count:
  duration: "1m"
  rate: 100
  rateUnit: "second"
  pattern:
    type: "ramp"
    startRate: 10
    endRate: 200
```

**Tags**: `advanced`, `streaming`, `load-pattern`, `rate`

---

## Transformation

### Post-Generation Transformation

**ID**: `advanced.transformation`
**Status**: Stable

Apply custom Java/Scala transformations to generated data before writing to output. Supports whole-file and row-by-row modes.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `className` | string | Yes | `-` | Fully qualified transformation class name YAML: `dataSources[].steps[].transformation.className` |
| `methodName` | string | No | `transform` | Method to call |
| `mode` | enum | No | `-` | Transformation mode Values: `whole-file`, `row-by-row` |
| `outputPath` | string | No | `-` | Output directory |
| `deleteOriginal` | boolean | No | `-` | Delete input after transformation |
| `enabled` | boolean | No | `true` | Enable/disable transformation |

**Tags**: `advanced`, `transformation`, `custom`, `plugin`

---

## Step Options

### Step Field Filtering

**ID**: `advanced.step_options`
**Status**: Stable

Include or exclude fields from metadata-driven generation using exact names or patterns.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `includeFields` | array | No | `-` | List of field names to include |
| `excludeFields` | array | No | `-` | List of field names to exclude |
| `includeFieldPatterns` | array | No | `-` | Regex patterns for fields to include |
| `excludeFieldPatterns` | array | No | `-` | Regex patterns for fields to exclude |
| `allCombinations` | boolean | No | `-` | Generate all field value combinations |

**Tags**: `advanced`, `step`, `filtering`, `metadata`

---

## Reference Mode

### Reference Mode

**ID**: `advanced.reference_mode`
**Status**: Stable

Load existing data as reference for foreign key relationships instead of generating new data. Useful when you need realistic FK values from existing datasets.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableReferenceMode` | boolean | No | `false` | Enable reference mode for this data source |
| `enableDataGeneration` | boolean | No | `true` | Disable generation (use with reference mode) |

**Tags**: `advanced`, `reference`, `existing-data`, `foreign-key`

---

## Interfaces

### Configuration Interfaces

**ID**: `advanced.interfaces`
**Status**: Stable

Data Caterer supports multiple configuration interfaces: Java API, Scala API, YAML configuration, and Web UI.

**Use Cases**:
- Java API: Programmatic configuration from Java applications
- Scala API: Programmatic configuration with Scala builders
- YAML: Declarative configuration for CI/CD and automation
- Web UI: Visual configuration and execution management

**Tags**: `advanced`, `interface`, `api`, `yaml`, `ui`

---

## Configuration

### Environment Variable Substitution

**ID**: `advanced.env_substitution`
**Status**: Stable

Use ${VAR_NAME} syntax in YAML configuration to substitute environment variables at runtime. Supports default values with ${VAR:-default}.

**Examples**:

**Environment variable substitution**:
```yaml
options:
  password: "${DB_PASSWORD}"
  url: "${KAFKA_BROKERS:-localhost:9092}"
```

**Tags**: `advanced`, `environment`, `variable`, `secrets`

---
