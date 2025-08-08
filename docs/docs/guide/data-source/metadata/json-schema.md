---
title: "Using JSON Schema with Data Caterer"
description: "Example of using JSON Schema for data generation and testing in Data Caterer."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# JSON Schema Source

![Data Caterer reading from JSON Schema file for schema metadata](../../../../diagrams/data-source/high_level_flow-run-config-basic-flow-odcs-support.svg)

Creating a data generator for JSON files based on metadata stored in [JSON Schema](https://json-schema.org/) format. JSON Schema provides a powerful way to describe and validate the structure of JSON data, making it an excellent metadata source for generating realistic test data.

## Requirements

- 10 minutes
- Git
- Gradle
- Docker

## Get Started

First, we will clone the data-caterer-example repo which will already have the base project setup required.

=== "Java"

    ```shell
    git clone git@github.com:data-catering/data-caterer-example.git
    ```

=== "Scala"

    ```shell
    git clone git@github.com:data-catering/data-caterer-example.git
    ```

=== "YAML"

    ```shell
    git clone git@github.com:data-catering/data-caterer-example.git
    ```

=== "UI"

    [Run Data Caterer UI via the 'Quick Start' found here.](../../../../get-started/quick-start.md)

### JSON Schema Setup

We will be using a JSON Schema file that defines the structure for a financial payment system. You can use your own JSON Schema file by placing it in the appropriate mount folder and following the steps below.

Example JSON Schema structure:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "customer_direct_debit_initiation_v11": {
      "type": "object",
      "properties": {
        "group_header": {
          "type": "object",
          "properties": {
            "message_identification": { "type": "string" },
            "creation_date_time": { "type": "string", "format": "date-time" },
            "number_of_transactions": { "type": "integer" },
            "control_sum": { "type": "number" },
            "initiating_party": {
              "type": "object",
              "properties": {
                "name": { "type": "string" }
              }
            }
          }
        },
        "payment_information": {
          "type": "object",
          "properties": {
            "payment_information_identification": { "type": "string" },
            "payment_method": { "type": "string" },
            "batch_booking": { "type": "boolean" },
            "direct_debit_transaction_information": {
              "type": "object",
              "properties": {
                "payment_identification": {
                  "type": "object",
                  "properties": {
                    "end_to_end_identification": { "type": "string" }
                  }
                },
                "instructed_amount": {
                  "type": "object",
                  "properties": {
                    "value": { "type": "number" },
                    "currency": { "type": "string" }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
```

### Plan Setup

Create a file depending on which interface you want to use.

- Java: `src/main/java/io/github/datacatering/plan/MyJSONSchemaJavaPlan.java`
- Scala: `src/main/scala/io/github/datacatering/plan/MyJSONSchemaPlan.scala`
- YAML: `docker/data/custom/plan/my-json-schema.yaml`

=== "Java"

    ```java
    import io.github.datacatering.datacaterer.java.api.PlanRun;

    public class MyJSONSchemaJavaPlan extends PlanRun {
        {
            var conf = configuration().enableGeneratePlanAndTasks(true)
                .generatedReportsFolderPath("/opt/app/data/report");
        }
    }
    ```

=== "Scala"

    ```scala
    import io.github.datacatering.datacaterer.api.PlanRun

    class MyJSONSchemaPlan extends PlanRun {
      val conf = configuration.enableGeneratePlanAndTasks(true)
        .generatedReportsFolderPath("/opt/app/data/report")
    }
    ```

=== "YAML"

    In `docker/data/custom/plan/my-json-schema.yaml`:
    ```yaml
    name: "my_json_schema_plan"
    description: "Create JSON data via JSON Schema metadata"
    tasks:
      - name: "json_schema_task"
        dataSourceName: "my_json_schema"
    ```

    In `docker/data/custom/application.conf`:
    ```
    flags {
      enableGeneratePlanAndTasks = true
      enableUniqueCheck = true
    }
    folders {
      generatedReportsFolderPath = "/opt/app/data/report"
    }
    ```

=== "UI"

    1. Click on `Advanced Configuration` towards the bottom of the screen
    1. Click on `Flag` and click on `Generate Plan And Tasks`
    1. Click on `Flag` and click on `Unique Check`
    1. Click on `Folder` and enter `/tmp/data-caterer/report` for `Generated Reports Folder Path`

We will enable generate plan and tasks so that we can read from external sources for metadata and save the reports under a folder we can easily access.

#### Connection Configuration

Within our class, we can start by defining the connection properties to read/write from/to JSON and specify the JSON Schema metadata source.

=== "Java"

    ```java
    var jsonSchemaTask = json(
        "my_json_schema",                           //name
        "/opt/app/data/json-schema-output",         //path
        Map.of("saveMode", "overwrite")             //additional options
    );
    ```

=== "Scala"

    ```scala
    val jsonSchemaTask = json(
      "my_json_schema",                           //name
      "/opt/app/data/json-schema-output",         //path
      Map("saveMode" -> "overwrite")              //additional options
    )
    ```

=== "YAML"

    In `docker/data/custom/application.conf`:
    ```
    json {
        my_json_schema {
            "saveMode": "overwrite"
        }
    }
    ```

=== "UI"

    1. Click on `Connection` towards the top of the screen
    1. For connection name, set to `my_json_schema`
    1. Click on `Select data source type..` and select `JSON`
    1. Set `Path` as `/tmp/custom/json-schema/output`
    1. Click on `Create`

#### Schema

We can point the schema of a data source to our JSON Schema file. The metadata source will automatically parse the JSON Schema and generate appropriate field definitions.

=== "Java"

    ```java
    var jsonSchemaTask = json("my_json_schema", "/opt/app/data/json-schema-output", Map.of("saveMode", "overwrite"))
            .fields(metadataSource().jsonSchema("/opt/app/mount/json-schema/payment-schema.json"))
            .count(count().records(10));
    ```

=== "Scala"

    ```scala
    val jsonSchemaTask = json("my_json_schema", "/opt/app/data/json-schema-output", Map("saveMode" -> "overwrite"))
      .fields(metadataSource.jsonSchema("/opt/app/mount/json-schema/payment-schema.json"))
      .count(count.records(10))
    ```

=== "YAML"

    In `docker/data/custom/task/file/json/json-schema-task.yaml`:
    ```yaml
    name: "json_schema_task"
    steps:
      - name: "json_data"
        type: "json"
        options:
          path: "/opt/app/data/json-schema-output"
          saveMode: "overwrite"
          metadataSourceType: "jsonSchema"
          jsonSchemaFile: "/opt/app/mount/json-schema/payment-schema.json"
        count:
          records: 10
    ```

=== "UI"

    1. Click on `Connection` tab at the top
    1. Select `JSON Schema` as the data source and enter `my-json-schema-metadata`
    1. Create your JSON Schema file at `/tmp/json-schema/payment-schema.json`
    1. Enter `/tmp/json-schema/payment-schema.json` as the `Schema File`
    1. Click on `Generation` and select the metadata source connection

#### Field Filtering Options

JSON Schema metadata source supports powerful field filtering capabilities to control which fields are included or excluded from data generation:

=== "Java"

    ```java
    var jsonSchemaTask = json("my_json_schema", "/opt/app/data/json-schema-output", Map.of("saveMode", "overwrite"))
            .fields(metadataSource().jsonSchema("/opt/app/mount/json-schema/payment-schema.json"))
            // Include specific fields only
            .includeFields(List.of(
                "customer_direct_debit_initiation_v11.group_header.message_identification",
                "customer_direct_debit_initiation_v11.group_header.creation_date_time",
                "customer_direct_debit_initiation_v11.payment_information.payment_information_identification",
                "customer_direct_debit_initiation_v11.payment_information.direct_debit_transaction_information.instructed_amount.value"
            ))
            // Or exclude specific fields
            // .excludeFields(List.of(
            //     "customer_direct_debit_initiation_v11.group_header.control_sum",
            //     "customer_direct_debit_initiation_v11.payment_information.batch_booking"
            // ))
            // Or include fields matching patterns
            // .includeFieldPatterns(List.of(".*amount.*", ".*identification.*"))
            // Or exclude fields matching patterns
            // .excludeFieldPatterns(List.of(".*internal.*", ".*debug.*"))
            .count(count().records(10));
    ```

=== "Scala"

    ```scala
    val jsonSchemaTask = json("my_json_schema", "/opt/app/data/json-schema-output", Map("saveMode" -> "overwrite"))
      .fields(metadataSource.jsonSchema("/opt/app/mount/json-schema/payment-schema.json"))
      // Include specific fields only
      .includeFields(
        "customer_direct_debit_initiation_v11.group_header.message_identification",
        "customer_direct_debit_initiation_v11.group_header.creation_date_time",
        "customer_direct_debit_initiation_v11.payment_information.payment_information_identification",
        "customer_direct_debit_initiation_v11.payment_information.direct_debit_transaction_information.instructed_amount.value"
      )
      // Or exclude specific fields
      // .excludeFields(
      //   "customer_direct_debit_initiation_v11.group_header.control_sum",
      //   "customer_direct_debit_initiation_v11.payment_information.batch_booking"
      // )
      // Or include fields matching patterns
      // .includeFieldPatterns(".*amount.*", ".*identification.*")
      // Or exclude fields matching patterns
      // .excludeFieldPatterns(".*internal.*", ".*debug.*")
      .count(count.records(10))
    ```

=== "YAML"

    In `docker/data/custom/task/file/json/json-schema-task.yaml`:
    ```yaml
    name: "json_schema_task"
    steps:
      - name: "json_data"
        type: "json"
        options:
          path: "/opt/app/data/json-schema-output"
          saveMode: "overwrite"
          metadataSourceType: "jsonSchema"
          jsonSchemaFile: "/opt/app/mount/json-schema/payment-schema.json"
          # Include specific fields only
          includeFields:
            - "customer_direct_debit_initiation_v11.group_header.message_identification"
            - "customer_direct_debit_initiation_v11.group_header.creation_date_time"
            - "customer_direct_debit_initiation_v11.payment_information.payment_information_identification"
          # Or exclude specific fields
          # excludeFields:
          #   - "customer_direct_debit_initiation_v11.group_header.control_sum"
          #   - "customer_direct_debit_initiation_v11.payment_information.batch_booking"
          # Or include fields matching patterns
          # includeFieldPatterns:
          #   - ".*amount.*"
          #   - ".*identification.*"
          # Or exclude fields matching patterns
          # excludeFieldPatterns:
          #   - ".*internal.*"
          #   - ".*debug.*"
        count:
          records: 10
    ```

=== "UI"

    1. In the connection configuration, expand `Advanced Options`
    1. Add `Include Fields` and enter comma-separated field paths
    1. Or add `Exclude Fields` for fields to exclude
    1. Or use `Include Field Patterns` with regex patterns
    1. Or use `Exclude Field Patterns` with regex patterns

The field filtering options provide flexibility to:

- **includeFields**: Only generate data for the specified field paths
- **excludeFields**: Generate data for all fields except the specified ones
- **includeFieldPatterns**: Include fields matching the regex patterns
- **excludeFieldPatterns**: Exclude fields matching the regex patterns

Field paths use dot notation to navigate nested structures (e.g., `parent.child.grandchild`).

#### Additional Configurations

At the end of data generation, a report gets generated that summarises the actions it performed. We can control the output folder of that report via configurations.

=== "Java"

    ```java
    var config = configuration()
            .generatedReportsFolderPath("/opt/app/data/report")
            .enableGeneratePlanAndTasks(true)
            .enableUniqueCheck(true);

    execute(config, jsonSchemaTask);
    ```

=== "Scala"

    ```scala
    val config = configuration
      .generatedReportsFolderPath("/opt/app/data/report")
      .enableGeneratePlanAndTasks(true)
      .enableUniqueCheck(true)

    execute(config, jsonSchemaTask)
    ```

=== "YAML"

    In `docker/data/custom/application.conf`:
    ```
    flags {
      enableGeneratePlanAndTasks = true
      enableUniqueCheck = true
    }
    folders {
      generatedReportsFolderPath = "/opt/app/data/report"
    }
    ```

=== "UI"

    1. Click on `Advanced Configuration` towards the bottom of the screen
    1. Click on `Flag` and click on `Generate Plan And Tasks`
    1. Click on `Flag` and click on `Unique Check`
    1. Click on `Folder` and enter `/tmp/data-caterer/report` for `Generated Reports Folder Path`

### Run

Now we can run via the script `./run.sh` that is in the top level directory of the `data-caterer-example` to run the class we just created.

=== "Java"

    ```shell
    ./run.sh MyJSONSchemaJavaPlan
    head docker/sample/json-schema-output/part-00000-*
    ```

=== "Scala"

    ```shell
    ./run.sh MyJSONSchemaPlan
    head docker/sample/json-schema-output/part-00000-*
    ```

=== "YAML"

    ```shell
    ./run.sh my-json-schema.yaml
    head docker/sample/json-schema-output/part-00000-*
    ```

=== "UI"

    1. Click the button `Execute` at the top
    1. Progress updates will show in the bottom right corner
    1. Click on `History` at the top
    1. Check for your plan name and see the result summary
    1. Click on `Report` on the right side to see more details of what was executed

It should look something like this.

```json
{
  "customer_direct_debit_initiation_v11": {
    "group_header": {
      "message_identification": "MSG001",
      "creation_date_time": "2024-03-15T10:30:45Z",
      "number_of_transactions": 1,
      "control_sum": 100.5,
      "initiating_party": {
        "name": "ACME Corp"
      }
    },
    "payment_information": {
      "payment_information_identification": "PMT001",
      "payment_method": "DD",
      "batch_booking": true,
      "direct_debit_transaction_information": {
        "payment_identification": {
          "end_to_end_identification": "TXN001"
        },
        "instructed_amount": {
          "value": 100.5,
          "currency": "EUR"
        }
      }
    }
  }
}
```

Congratulations! You have now made a data generator that uses JSON Schema as a metadata source to generate realistic test data following your schema specifications.

## JSON Schema Support

This section provides comprehensive documentation about Data Caterer's JSON Schema metadata support, including which features are supported, how they map to data generation, and current limitations.

### Supported JSON Schema Versions

Data Caterer supports the following JSON Schema versions:

- **Draft 4** (`http://json-schema.org/draft-04/schema#`)
- **Draft 6** (`http://json-schema.org/draft-06/schema#`)
- **Draft 7** (`http://json-schema.org/draft-07/schema#`)
- **Draft 2019-09** (`https://json-schema.org/draft/2019-09/schema`)
- **Draft 2020-12** (`https://json-schema.org/draft/2020-12/schema`) - _Default_

If no `$schema` is specified, Data Caterer defaults to Draft 2020-12.

### Data Type Mapping

JSON Schema types are mapped to Data Caterer data types as follows:

| JSON Schema Type | Data Caterer Type       | Generated Data Examples                |
| ---------------- | ----------------------- | -------------------------------------- |
| `string`         | `StringType`            | Random strings, format-specific values |
| `integer`        | `IntegerType`           | Random integers within constraints     |
| `number`         | `DoubleType`            | Random decimal numbers                 |
| `boolean`        | `BooleanType`           | `true` or `false`                      |
| `array`          | `ArrayType`             | Arrays of the specified item type      |
| `object`         | `StructType`            | Nested objects with defined properties |
| `null`           | `StringType` (nullable) | Treated as nullable string             |

### Core Schema Features

#### Basic Properties

```json
{
  "type": "object",
  "properties": {
    "name": { "type": "string" },
    "age": { "type": "integer" },
    "active": { "type": "boolean" }
  },
  "required": ["name"]
}
```

- **Supported**: All basic types, nested objects, required fields
- **Data Generation**: Required fields are non-nullable, optional fields are nullable

#### Arrays

```json
{
  "type": "array",
  "items": {
    "type": "string"
  },
  "minItems": 1,
  "maxItems": 10,
  "uniqueItems": true
}
```

- **Supported**: Arrays of primitives and objects, nested arrays
- **Array Constraints**: `minItems`, `maxItems`, `uniqueItems`
- **Data Generation**: Generates arrays within specified size bounds

#### Nested Objects

```json
{
  "type": "object",
  "properties": {
    "address": {
      "type": "object",
      "properties": {
        "street": { "type": "string" },
        "city": { "type": "string" }
      },
      "required": ["street"]
    }
  }
}
```

- **Supported**: Multi-level nesting, required fields at any level
- **Data Generation**: Preserves nested structure and constraints

### Validation Constraints

#### String Constraints

| Constraint  | Support    | Data Generation Effect                   |
| ----------- | ---------- | ---------------------------------------- |
| `pattern`   | ✅ Full    | Generates strings matching regex pattern |
| `minLength` | ✅ Full    | Minimum string length                    |
| `maxLength` | ✅ Full    | Maximum string length                    |
| `enum`      | ✅ Full    | Selects from predefined values           |
| `const`     | ✅ Full    | Always generates the constant value      |
| `format`    | ✅ Partial | Format-specific generators (see below)   |

**Format Support:**

- `email` - Generates realistic email addresses
- `uri`/`url` - Generates valid URLs
- `uuid` - Generates UUID strings
- `date` - Generates date strings (YYYY-MM-DD)
- `date-time` - Generates timestamp strings
- `time` - Generates time strings (HH:MM:SS)
- `ipv4` - Generates IPv4 addresses
- `ipv6` - Generates IPv6 addresses
- `hostname` - Generates hostname strings

#### Numeric Constraints

| Constraint         | Support          | Data Generation Effect    |
| ------------------ | ---------------- | ------------------------- |
| `minimum`          | ✅ Full          | Minimum value (inclusive) |
| `maximum`          | ✅ Full          | Maximum value (inclusive) |
| `exclusiveMinimum` | ❌ Not supported | Treated as minimum        |
| `exclusiveMaximum` | ❌ Not supported | Treated as maximum        |
| `multipleOf`       | ❌ Not supported | Ignored                   |

#### Array Constraints

| Constraint    | Support          | Data Generation Effect            |
| ------------- | ---------------- | --------------------------------- |
| `minItems`    | ✅ Full          | Minimum array length              |
| `maxItems`    | ✅ Full          | Maximum array length              |
| `uniqueItems` | ✅ Full          | Ensures array elements are unique |
| `contains`    | ❌ Not supported | Ignored                           |
| `minContains` | ❌ Not supported | Ignored                           |
| `maxContains` | ❌ Not supported | Ignored                           |

#### Object Constraints

| Constraint             | Support          | Data Generation Effect                  |
| ---------------------- | ---------------- | --------------------------------------- |
| `required`             | ✅ Full          | Makes fields non-nullable               |
| `minProperties`        | ❌ Not supported | Ignored                                 |
| `maxProperties`        | ❌ Not supported | Ignored                                 |
| `additionalProperties` | ⚠️ Recognized    | Does not generate additional properties |

### Advanced Features

#### References ($ref)

Data Caterer supports JSON Schema references for reusable schema components:

```json
{
  "type": "object",
  "properties": {
    "user": { "$ref": "#/definitions/User" }
  },
  "definitions": {
    "User": {
      "type": "object",
      "properties": {
        "id": { "type": "integer" },
        "name": { "type": "string" }
      },
      "required": ["id"]
    }
  }
}
```

**Reference Support:**

- ✅ `#/definitions/DefinitionName` - Internal definitions
- ❌ External references (different files/URLs)
- ❌ JSON Pointer references other than `#/definitions/`

**Data Generation**: References are resolved and constraints from referenced schemas are preserved.

#### Schema Composition

##### allOf (Full Support)

```json
{
  "allOf": [
    { "$ref": "#/definitions/BaseEntity" },
    { "$ref": "#/definitions/TimestampFields" },
    {
      "properties": {
        "description": { "type": "string" }
      }
    }
  ]
}
```

**Data Generation**: Merges all schemas and generates fields from all combined properties.

##### oneOf/anyOf (Limited Support)

```json
{
  "oneOf": [
    { "$ref": "#/definitions/CreditCardPayment" },
    { "$ref": "#/definitions/BankTransferPayment" }
  ]
}
```

**Data Generation**: Uses the **first schema** from the oneOf/anyOf array. Other schemas are ignored.

##### not (Not Supported)

`not` schemas are not supported and will be ignored.

### Field Filtering

Data Caterer provides powerful field filtering options for JSON Schema metadata sources:

#### Include/Exclude Specific Fields

```scala
// Include only specific fields
.includeFields("profile.name", "profile.email", "addresses.street")

// Exclude specific fields
.excludeFields("profile.createdDate", "sessionId")
```

#### Pattern-Based Filtering

```scala
// Include fields matching patterns
.includeFieldPatterns(".*email.*", ".*name.*")

// Exclude fields matching patterns
.excludeFieldPatterns(".*internal.*", ".*debug.*")
```

**Field Path Format**: Use dot notation for nested fields (e.g., `profile.address.street`).

### Constraints Preservation

Data Caterer preserves constraints through:

1. **Direct mapping**: Simple constraints map directly to Data Caterer options
2. **Reference resolution**: Constraints from referenced schemas are maintained
3. **Composition merging**: allOf compositions merge constraints from all schemas
4. **Nested structures**: Multi-level constraints are preserved at appropriate levels

#### Example: Complex Constraint Preservation

```json
{
  "properties": {
    "users": {
      "type": "array",
      "items": {
        "$ref": "#/definitions/User"
      },
      "minItems": 1,
      "maxItems": 100
    }
  },
  "definitions": {
    "User": {
      "properties": {
        "email": {
          "type": "string",
          "format": "email"
        },
        "age": {
          "type": "integer",
          "minimum": 18,
          "maximum": 65
        }
      }
    }
  }
}
```

**Generated Data**:

- Array will have 1-100 items
- Each user will have a valid email format
- Ages will be between 18-65

### Unsupported Features

The following JSON Schema features are **not currently supported**:

#### Schema Composition

- `not` schemas
- Complex `anyOf`/`oneOf` resolution (only first option used)

#### Advanced Constraints

- `exclusiveMinimum`/`exclusiveMaximum`
- `multipleOf`
- `contains`, `minContains`, `maxContains`
- `minProperties`/`maxProperties`
- `unevaluatedProperties`/`unevaluatedItems`

#### Advanced Features

- `if`/`then`/`else` conditional schemas
- `dependentRequired`/`dependentSchemas`
- `prefixItems` (Draft 2020-12)
- External references
- Dynamic references
- Schema recursion detection

#### Format Extensions

- Custom formats beyond the built-in list
- `idn-email`, `idn-hostname`
- `iri`, `iri-reference`
- `duration`

## Validation

If you want to validate data against a JSON Schema, you can use the generated data with validation frameworks or tools that support JSON Schema validation.

The JSON Schema metadata source in Data Caterer focuses on data generation based on the schema structure, ensuring that the generated data conforms to the defined schema constraints and types.
