# Configuration

Runtime configuration for controlling generation behavior, validation, performance tuning, alerts, and output paths.

**29 features** in this category.

## Table of Contents

- [Feature Flags](#flags) (14 features)
- [Folder Paths](#folders) (7 features)
- [Generation Settings](#generation) (2 features)
- [Metadata Settings](#metadata) (1 features)
- [Streaming Settings](#streaming) (1 features)
- [Alert Settings](#alerts) (1 features)
- [Validation Runtime](#validation-runtime) (1 features)
- [Spark Runtime](#runtime) (1 features)
- [Sink Options](#sink) (1 features)

## Feature Flags

### Count Records

**ID**: `configuration.flags.enablecount`
**Status**: Stable

Count the number of records generated for each data source step.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableCount` | boolean | No | `true` | Count the number of records generated for each data source step. YAML: `config.flags.enableCount` |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala` (primary)

**Tags**: `configuration`, `flag`, `count`

---

### Generate Data

**ID**: `configuration.flags.enablegeneratedata`
**Status**: Stable

Enable or disable data generation. When false, only validation runs.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableGenerateData` | boolean | No | `true` | Enable or disable data generation. When false, only validation runs. YAML: `config.flags.enableGenerateData` |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala` (primary)

**Tags**: `configuration`, `flag`, `generatedata`

---

### Record Tracking

**ID**: `configuration.flags.enablerecordtracking`
**Status**: Stable

Track generated records for later cleanup/deletion.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableRecordTracking` | boolean | No | `false` | Track generated records for later cleanup/deletion. YAML: `config.flags.enableRecordTracking` |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala` (primary)

**Tags**: `configuration`, `flag`, `recordtracking`

---

### Delete Generated Records

**ID**: `configuration.flags.enabledeletegeneratedrecords`
**Status**: Stable

Enable cleanup mode to delete previously generated records.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableDeleteGeneratedRecords` | boolean | No | `false` | Enable cleanup mode to delete previously generated records. YAML: `config.flags.enableDeleteGeneratedRecords` |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala` (primary)

**Tags**: `configuration`, `flag`, `deletegeneratedrecords`

---

### Auto-Generate Plan and Tasks

**ID**: `configuration.flags.enablegenerateplanandtasks`
**Status**: Stable

Automatically generate plan and tasks from metadata sources.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableGeneratePlanAndTasks` | boolean | No | `false` | Automatically generate plan and tasks from metadata sources. YAML: `config.flags.enableGeneratePlanAndTasks` |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala` (primary)

**Tags**: `configuration`, `flag`, `generateplanandtasks`

---

### Fail on Error

**ID**: `configuration.flags.enablefailonerror`
**Status**: Stable

Fail execution immediately when errors occur.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableFailOnError` | boolean | No | `true` | Fail execution immediately when errors occur. YAML: `config.flags.enableFailOnError` |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala` (primary)

**Tags**: `configuration`, `flag`, `failonerror`

---

### Unique Check

**ID**: `configuration.flags.enableuniquecheck`
**Status**: Stable

Validate uniqueness constraints during data generation.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableUniqueCheck` | boolean | No | `false` | Validate uniqueness constraints during data generation. YAML: `config.flags.enableUniqueCheck` |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala` (primary)

**Tags**: `configuration`, `flag`, `uniquecheck`

---

### Sink Metadata

**ID**: `configuration.flags.enablesinkmetadata`
**Status**: Stable

Save metadata about generated data to the sink.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableSinkMetadata` | boolean | No | `false` | Save metadata about generated data to the sink. YAML: `config.flags.enableSinkMetadata` |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala` (primary)

**Tags**: `configuration`, `flag`, `sinkmetadata`

---

### Save Reports

**ID**: `configuration.flags.enablesavereports`
**Status**: Stable

Generate and save execution reports with generation and validation results.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableSaveReports` | boolean | No | `true` | Generate and save execution reports with generation and validation results. YAML: `config.flags.enableSaveReports` |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala` (primary)

**Tags**: `configuration`, `flag`, `savereports`

---

### Data Validation

**ID**: `configuration.flags.enablevalidation`
**Status**: Stable

Run data validations after generation completes.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableValidation` | boolean | No | `true` | Run data validations after generation completes. YAML: `config.flags.enableValidation` |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala` (primary)

**Tags**: `configuration`, `flag`, `validation`

---

### Suggest Validations

**ID**: `configuration.flags.enablegeneratevalidations`
**Status**: Stable

Auto-suggest validations based on data analysis.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableGenerateValidations` | boolean | No | `false` | Auto-suggest validations based on data analysis. YAML: `config.flags.enableGenerateValidations` |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala` (primary)

**Tags**: `configuration`, `flag`, `generatevalidations`

---

### Alerts

**ID**: `configuration.flags.enablealerts`
**Status**: Stable

Send alert notifications on completion (supports Slack).

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableAlerts` | boolean | No | `true` | Send alert notifications on completion (supports Slack). YAML: `config.flags.enableAlerts` |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala` (primary)

**Tags**: `configuration`, `flag`, `alerts`

---

### Unique Check Only In Batch

**ID**: `configuration.flags.enableuniquecheckonlyinbatch`
**Status**: Stable

Check uniqueness only within the current batch for better performance.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableUniqueCheckOnlyInBatch` | boolean | No | `false` | Check uniqueness only within the current batch for better performance. YAML: `config.flags.enableUniqueCheckOnlyInBatch` |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala` (primary)

**Tags**: `configuration`, `flag`, `uniquecheckonlyinbatch`

---

### Fast Generation

**ID**: `configuration.flags.enablefastgeneration`
**Status**: Stable

Use SQL-based generation for regex patterns instead of UDFs. Dramatically improves performance.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enableFastGeneration` | boolean | No | `false` | Use SQL-based generation for regex patterns instead of UDFs. Dramatically improves performance. YAML: `config.flags.enableFastGeneration` |

**Source Files**:
- `api/src/main/scala/io/github/datacatering/datacaterer/api/model/ConfigModels.scala` (primary)

**Tags**: `configuration`, `flag`, `fastgeneration`

---

## Folder Paths

### plan File

**ID**: `configuration.folders.planfilepath`
**Status**: Stable

Configuration path for planFilePath.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `planFilePath` | string | No | `-` | Path setting for planFilePath YAML: `config.folders.planFilePath` |

**Tags**: `configuration`, `folder`, `path`

---

### task Folder

**ID**: `configuration.folders.taskfolderpath`
**Status**: Stable

Configuration path for taskFolderPath.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `taskFolderPath` | string | No | `-` | Path setting for taskFolderPath YAML: `config.folders.taskFolderPath` |

**Tags**: `configuration`, `folder`, `path`

---

### generatedPlanAndTask Folder

**ID**: `configuration.folders.generatedplanandtaskfolderpath`
**Status**: Stable

Configuration path for generatedPlanAndTaskFolderPath.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `generatedPlanAndTaskFolderPath` | string | No | `-` | Path setting for generatedPlanAndTaskFolderPath YAML: `config.folders.generatedPlanAndTaskFolderPath` |

**Tags**: `configuration`, `folder`, `path`

---

### generatedReports Folder

**ID**: `configuration.folders.generatedreportsfolderpath`
**Status**: Stable

Configuration path for generatedReportsFolderPath.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `generatedReportsFolderPath` | string | No | `-` | Path setting for generatedReportsFolderPath YAML: `config.folders.generatedReportsFolderPath` |

**Tags**: `configuration`, `folder`, `path`

---

### recordTracking Folder

**ID**: `configuration.folders.recordtrackingfolderpath`
**Status**: Stable

Configuration path for recordTrackingFolderPath.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `recordTrackingFolderPath` | string | No | `-` | Path setting for recordTrackingFolderPath YAML: `config.folders.recordTrackingFolderPath` |

**Tags**: `configuration`, `folder`, `path`

---

### validation Folder

**ID**: `configuration.folders.validationfolderpath`
**Status**: Stable

Configuration path for validationFolderPath.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `validationFolderPath` | string | No | `-` | Path setting for validationFolderPath YAML: `config.folders.validationFolderPath` |

**Tags**: `configuration`, `folder`, `path`

---

### recordTrackingForValidation Folder

**ID**: `configuration.folders.recordtrackingforvalidationfolderpath`
**Status**: Stable

Configuration path for recordTrackingForValidationFolderPath.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `recordTrackingForValidationFolderPath` | string | No | `-` | Path setting for recordTrackingForValidationFolderPath YAML: `config.folders.recordTrackingForValidationFolderPath` |

**Tags**: `configuration`, `folder`, `path`

---

## Generation Settings

### Batch Size

**ID**: `configuration.generation.batch_size`
**Status**: Stable

Control the number of records generated per batch. Affects memory usage and performance.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `numRecordsPerBatch` | long | No | `100000` | Records per batch YAML: `config.generation.numRecordsPerBatch` |
| `numRecordsPerStep` | long | No | `-` | Default records per step/table YAML: `config.generation.numRecordsPerStep` |

**Tags**: `configuration`, `generation`, `batch`, `performance`

---

### Bloom Filter Configuration

**ID**: `configuration.generation.bloom_filter`
**Status**: Stable

Configure bloom filter parameters for uniqueness checking during generation.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `uniqueBloomFilterNumItems` | long | No | `10000000` | Expected number of items in bloom filter YAML: `config.generation.uniqueBloomFilterNumItems` |
| `uniqueBloomFilterFalsePositiveProbability` | double | No | `0.01` | Bloom filter false positive rate (0-1) YAML: `config.generation.uniqueBloomFilterFalsePositiveProbability` |

**Tags**: `configuration`, `generation`, `bloom-filter`, `uniqueness`

---

## Metadata Settings

### Metadata Analysis Configuration

**ID**: `configuration.metadata`
**Status**: Stable

Configure how metadata is sampled and analyzed for auto-generation of field patterns.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `numRecordsFromDataSource` | integer | No | `10000` | Sample size from data source YAML: `config.metadata.numRecordsFromDataSource` |
| `numRecordsForAnalysis` | integer | No | `10000` | Records analyzed for pattern detection YAML: `config.metadata.numRecordsForAnalysis` |
| `oneOfDistinctCountVsCountThreshold` | double | No | `0.2` | Threshold for detecting oneOf fields YAML: `config.metadata.oneOfDistinctCountVsCountThreshold` |
| `oneOfMinCount` | long | No | `1000` | Minimum records for oneOf detection YAML: `config.metadata.oneOfMinCount` |
| `numGeneratedSamples` | integer | No | `10` | Number of sample records in metadata suggestions YAML: `config.metadata.numGeneratedSamples` |

**Tags**: `configuration`, `metadata`, `analysis`, `sampling`

---

## Streaming Settings

### Streaming Configuration

**ID**: `configuration.streaming`
**Status**: Stable

Configure streaming/real-time data generation parameters.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `maxTimeoutSeconds` | integer | No | `3600` | Maximum streaming timeout |
| `maxAsyncParallelism` | integer | No | `100` | Maximum async parallelism |
| `responseBufferSize` | integer | No | `10000` | Response buffer size for streaming |
| `timestampWindowMs` | long | No | `1000` | Timestamp window in milliseconds |

**Tags**: `configuration`, `streaming`, `real-time`, `performance`

---

## Alert Settings

### Alert Configuration

**ID**: `configuration.alerts`
**Status**: Stable

Configure alert notifications triggered on execution completion. Supports Slack integration.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `triggerOn` | enum | No | `all` | When to trigger alerts Values: `all`, `failure`, `success`, `generation_failure`, `validation_failure`, `generation_success`, `validation_success` YAML: `config.alert.triggerOn` |
| `slackToken` | string | No | `-` | Slack API token YAML: `config.alert.slackToken` |
| `slackChannels` | array | No | `-` | Slack channels to notify YAML: `config.alert.slackChannels` |

**Tags**: `configuration`, `alert`, `notification`, `slack`

---

## Validation Runtime

### Validation Runtime Configuration

**ID**: `configuration.validation`
**Status**: Stable

Configure validation execution behavior.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `numSampleErrorRecords` | integer | No | `5` | Number of sample error records in reports YAML: `config.validation.numSampleErrorRecords` |
| `enableDeleteRecordTrackingFiles` | boolean | No | `true` | Delete tracking files after validation YAML: `config.validation.enableDeleteRecordTrackingFiles` |

**Tags**: `configuration`, `validation`, `runtime`

---

## Spark Runtime

### Apache Spark Configuration

**ID**: `configuration.runtime.spark`
**Status**: Stable

Configure the Apache Spark runtime for data processing. Set master URL, driver/executor memory, and Spark SQL settings.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `master` | string | No | `local[*]` | Spark master URL YAML: `config.runtime.master` |
| `sparkConfig` | object | No | `-` | Spark configuration key-value pairs YAML: `config.runtime.sparkConfig` |

**Examples**:

**Spark configuration**:
```yaml
config:
  runtime:
    master: "local[4]"
    sparkConfig:
      "spark.driver.memory": "4g"
      "spark.sql.shuffle.partitions": "10"
```

**Tags**: `configuration`, `runtime`, `spark`, `performance`

---

## Sink Options

### Global Sink Options

**ID**: `configuration.sink_options`
**Status**: Stable

Global options for data output: random seed for reproducibility and locale for data generation.

**Configuration**:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `seed` | string | No | `-` | Random seed for reproducible generation YAML: `sinkOptions.seed` |
| `locale` | string | No | `-` | Locale for data generation (affects names, addresses) YAML: `sinkOptions.locale` |

**Examples**:

**Sink options**:
```yaml
sinkOptions:
  seed: "42"
  locale: "en-US"
```

**Tags**: `configuration`, `sink`, `seed`, `locale`

---
