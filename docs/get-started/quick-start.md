---
title: "Quick start"
description: "Quick start for Data Catering data generation and testing tool that can automatically discover, generate and validate for files, databases, HTTP APIs and messaging systems."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# Quick Start

Get started with Data Caterer in minutes. Choose your preferred approach:

<div class="grid cards" markdown>

-   :material-language-java: :simple-scala: __[Java/Scala API (Recommended)]__

    ---

    Full programmatic control for complex scenarios and test integration.

-   :simple-yaml: __[YAML]__

    ---

    Configuration-based approach. Great for CI/CD pipelines.

-   :material-monitor-dashboard: __[UI]__

    ---

    Point-and-click interface. No coding required.

</div>

  [Java/Scala API (Recommended)]: #javascala-api
  [YAML]: #yaml
  [UI]: #ui

---

## Java/Scala API

The recommended approach for full control over data generation. Write your data generation logic in Scala or Java.

### Run

```shell
git clone git@github.com:data-catering/data-caterer.git
cd data-caterer/example
./run.sh
```

Press Enter to run the default example, or enter a class name (e.g., `CsvPlan`).

### What Happens

1. Builds your Scala/Java code into a JAR
2. Runs it via Docker with the Data Caterer engine
3. Generates data and reports to `docker/sample/`

### Example Code

```scala
class CsvPlan extends PlanRun {
  val accountTask = csv("accounts", "/opt/app/data/accounts", Map("header" -> "true"))
    .fields(
      field.name("account_id").regex("ACC[0-9]{8}").unique(true),
      field.name("name").expression("#{Name.name}"),
      field.name("balance").`type`(DoubleType).min(10).max(1000),
      field.name("status").oneOf("open", "closed", "pending")
    )
    .count(count.records(100))

  execute(accountTask)
}
```

### More Examples

| Class | Description |
|-------|-------------|
| `DocumentationPlanRun` | JSON + CSV with foreign keys (default) |
| `CsvPlan` | CSV files with relationships |
| `PostgresPlanRun` | PostgreSQL tables |
| `KafkaPlanRun` | Kafka messages |
| `ValidationPlanRun` | Generate and validate data |

Run any example: `./run.sh <ClassName>`

All example classes are in `src/main/scala/io/github/datacatering/plan/`.

---

## YAML

Define data generation using YAML configuration files.

### Run

```shell
git clone git@github.com:data-catering/data-caterer.git
cd data-caterer/example
./run.sh csv.yaml
```

### What Happens

1. Builds the example JAR
2. Runs the YAML plan via Docker
3. Generates data and reports to `docker/data/custom/`

### Example YAML

**Plan file** (`docker/data/custom/plan/csv.yaml`):
```yaml
name: "csv_example_plan"
description: "Create transaction data in CSV file"
tasks:
  - name: "csv_transaction_file"
    dataSourceName: "csv"
    enabled: true
```

**Task file** (`docker/data/custom/task/file/csv/`):
```yaml
name: "csv_transaction_file"
steps:
  - name: "transactions"
    type: "csv"
    options:
      path: "/opt/app/data/transactions"
      header: "true"
    count:
      records: 1000
    fields:
      - name: "account_id"
        options:
          regex: "ACC[0-9]{8}"
      - name: "amount"
        type: "double"
        options:
          min: 10
          max: 1000
```

### More Examples

| Plan File | Description |
|-----------|-------------|
| `csv.yaml` | CSV files |
| `parquet.yaml` | Parquet files |
| `postgres.yaml` | PostgreSQL tables |
| `kafka.yaml` | Kafka messages |
| `foreign-key.yaml` | Data with relationships |
| `validation.yaml` | Generate and validate |

Run any example: `./run.sh <filename>.yaml`

All plan files are in `docker/data/custom/plan/`. Task definitions are in `docker/data/custom/task/`.

---

## UI

A web interface for creating and running data generation plans.

### Run

```shell
docker run -d -p 9898:9898 -e DEPLOY_MODE=standalone --name datacaterer datacatering/data-caterer:0.18.0
```

Open [http://localhost:9898](http://localhost:9898) in your browser.

### What You Can Do

- Create connections to databases, files, Kafka, and more
- Define data schemas with field types and constraints
- Generate test data with a single click
- View results and reports in the browser

[**Try the UI demo**](https://data.catering/latest/sample/ui/)

---

## View Results

After running, check the generated report:

- **Java/Scala examples:** `docker/sample/report/index.html`
- **YAML examples:** `docker/data/custom/report/index.html`

[**Sample report preview**](../sample/report/html/index.html)

---

## Next Steps

<div class="grid cards" markdown>

-   :material-school: __Step-by-Step Guide__

    ---

    [First data generation guide](../docs/guide/scenario/first-data-generation.md) - learn Data Caterer's full capabilities.

-   :material-book-open: __All Guides__

    ---

    [Browse all guides](../docs/guide/index.md) for specific use cases and data sources.

-   :material-connection: __Data Sources__

    ---

    [Supported connections](../docs/connection/index.md) - databases, files, messaging, and HTTP.

</div>
