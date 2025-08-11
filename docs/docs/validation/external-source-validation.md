---
title: "External Source Validations such as Great Expectations, OpenMetadata, Soda, DBT"
description: "Examples of external validations from sources such as Great Expectations, OpenMetadata, Soda or DBT, to run for data in files, databases, HTTP APIs or messaging systems via Data Catering."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

# External Source Validations

Use validations that are defined in external sources such as Great Expectations or OpenMetadata. This allows you to
generate data for your upstream data sources and validate your pipelines based on the same rules that would be applied
in production.

![Example flow with validations from external source](../../diagrams/high_level_flow-external-source-validation.svg)

## Supported Sources

| Source                                                                                            | Support                                   |
|---------------------------------------------------------------------------------------------------|-------------------------------------------|
| [OpenMetadata](https://docs.open-metadata.org/v1.2.x/connectors/ingestion/workflows/data-quality) | :white_check_mark:                        |
| [Great Expectations](https://greatexpectations.io/)                                               | :white_check_mark:                        |
| [DBT Constraints](https://docs.getdbt.com/reference/resource-properties/constraints)              | :octicons-x-circle-fill-12:{ .red-cross } |
| [SodaCL](https://docs.soda.io/soda-cl/soda-cl-overview.html)                                      | :octicons-x-circle-fill-12:{ .red-cross } |
| [MonteCarlo](https://docs.getmontecarlo.com/docs/monitors-as-code)                                | :octicons-x-circle-fill-12:{ .red-cross } |

## OpenMetadata

Use data quality rules defined from OpenMetadata to execute over dataset.

=== "Java"

    ```java
    var jsonTask = json("my_json", "/opt/app/data/json")
        .validations(metadataSource().openMetadata(
            "http://host.docker.internal:8585/api",
            Constants.OPEN_METADATA_AUTH_TYPE_OPEN_METADATA(),
            Map.of(
                    Constants.OPEN_METADATA_JWT_TOKEN(), "abc123",
                    Constants.OPEN_METADATA_TABLE_FQN(), "sample_data.ecommerce_db.shopify.raw_customer"
            )
        ));

    var conf = configuration().enableGenerateValidations(true);
    ```

=== "Scala"

    ```scala
    val jsonTask = json("my_json", "/opt/app/data/json")
      .validations(metadataSource.openMetadata(
        "http://host.docker.internal:8585/api",
        OPEN_METADATA_AUTH_TYPE_OPEN_METADATA,
        Map(
          OPEN_METADATA_JWT_TOKEN -> "abc123", //find under settings/bots/ingestion-bot/token
          OPEN_METADATA_TABLE_FQN -> "sample_data.ecommerce_db.shopify.raw_customer"
        )
      ))

    val conf = configuration.enableGenerateValidations(true)
    ```

=== "YAML"

    ```yaml
    name: "account_checks"
    dataSources:
      my_json:
        - options:
            metadataSourceType: "openMetadata"
            authType: "openMetadataJwtToken"
            openMetadataJwtToken: "abc123"
            tableFqn: "sample_data.ecommerce_db.shopify.raw_customer"
    ```

## Great Expectations

Use data quality rules defined from OpenMetadata to execute over dataset.

=== "Java"

    ```java
    var jsonTask = json("my_json", "/opt/app/data/json")
        .validations(metadataSource().greatExpectations("great-expectations/taxi-expectations.json"));

    var conf = configuration().enableGenerateValidations(true);
    ```

=== "Scala"

    ```scala
    val jsonTask = json("my_json", "/opt/app/data/json")
      .validations(metadataSource.greatExpectations("great-expectations/taxi-expectations.json"))

    val conf = configuration.enableGenerateValidations(true)
    ```

=== "YAML"

    ```yaml
    name: "account_checks"
    dataSources:
      my_json:
        - options:
            metadataSourceType: "greatExpectations"
            expectationsFile: "great-expectations/taxi-expectations.json"
    ```
