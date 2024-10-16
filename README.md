# Data Caterer - Test Data Management Tool

![Data Catering](misc/banner/logo_landscape_banner.svg)

## Overview

A test data management tool with automated data generation, validation and cleanup.

![Basic data flow for Data Caterer](misc/design/high_level_flow-run-config-basic-flow.svg)

[Generate data](https://data.catering/setup/generator/data-generator/) for databases, files, messaging systems or HTTP 
requests via UI, Scala/Java SDK or YAML input and executed via Spark. Run 
[data validations](https://data.catering/setup/validation/) after generating data to ensure it is consumed correctly. 
[Clean up generated data or consumed data](https://data.catering/setup/delete-data/) in downstream data sources to keep 
your environments tidy. [Define alerts](https://data.catering/setup/report/alert/) to get notified when failures occur 
and deep dive into issues [from the generated report](https://data.catering/sample/report/html/).

[**Full docs can be found here**](https://data.catering/setup/).
  
[**A demo of the UI found here**](https://data.catering/sample/ui/index.html).
  
[**Scala/Java examples found here**](https://github.com/data-catering/data-caterer-example).

## Features

- [Batch and/or event data generation](https://data.catering/setup/connection/)
- [Maintain relationships across any dataset](https://data.catering/setup/foreign-key/)
- [Create custom data generation/validation scenarios](https://data.catering/setup/generator/data-generator/)
- [Data validation](https://data.catering/setup/validation/)
- [Clean up generated and downstream data](https://data.catering/setup/delete-data/)
- [Suggest data validations](https://data.catering/setup/validation/)
- [Metadata discovery](https://data.catering/setup/guide/scenario/auto-generate-connection/)
- [Detailed report of generated data and validation results](https://data.catering/sample/report/html/)
- [Alerts to be notified of results](https://data.catering/setup/report/alert/)
- [Run as GitHub Action](https://github.com/data-catering/insta-integration)

![Basic flow](misc/design/basic_data_caterer_flow_medium.gif)

## Quick start

1. [UI App: Mac download](https://nightly.link/data-catering/data-caterer/workflows/build/main/data-caterer-mac.zip)
2. [UI App: Windows download](https://nightly.link/data-catering/data-caterer/workflows/build/main/data-caterer-windows.zip)
   1. After downloading, go to 'Downloads' folder and 'Extract All' from data-caterer-windows
   2. Double-click 'DataCaterer-1.0.0' to install Data Caterer
   3. Click on 'More info' then at the bottom, click 'Run anyway'
   4. Go to '/Program Files/DataCaterer' folder and run DataCaterer application
   5. If your browser doesn't open, go to [http://localhost:9898](http://localhost:9898) in your preferred browser
3. [UI App: Linux download](https://nightly.link/data-catering/data-caterer/workflows/build/main/data-caterer-linux.zip)
4. Docker
   ```shell
   docker run -d -i -p 9898:9898 -e DEPLOY_MODE=standalone --name datacaterer datacatering/data-caterer-basic:0.11.11
   ```
   [Open localhost:9898](http://localhost:9898).

### Run Scala/Java examples

```shell
git clone git@github.com:data-catering/data-caterer-example.git
cd data-caterer-example && ./run.sh
#check results under docker/sample/report/index.html folder
```

## Integrations

### Supported data sources

Data Caterer supports the below data sources. [Check here for the full roadmap](#roadmap).

| Data Source Type | Data Source                        | Support |
|------------------|------------------------------------|---------|
| Cloud Storage    | AWS S3                             | ✅       |
| Cloud Storage    | Azure Blob Storage                 | ✅       |
| Cloud Storage    | GCP Cloud Storage                  | ✅       |
| Database         | Cassandra                          | ✅       |
| Database         | MySQL                              | ✅       |
| Database         | Postgres                           | ✅       |
| Database         | Elasticsearch                      | ❌       |
| Database         | MongoDB                            | ❌       |
| File             | CSV                                | ✅       |
| File             | Delta Lake                         | ✅       |
| File             | JSON                               | ✅       |
| File             | Iceberg                            | ✅       |
| File             | ORC                                | ✅       |
| File             | Parquet                            | ✅       |
| File             | Hudi                               | ❌       |
| HTTP             | REST API                           | ✅       |
| Messaging        | Kafka                              | ✅       |
| Messaging        | Solace                             | ✅       |
| Messaging        | ActiveMQ                           | ❌       |
| Messaging        | Pulsar                             | ❌       |
| Messaging        | RabbitMQ                           | ❌       |
| Metadata         | Data Contract CLI                  | ✅       |
| Metadata         | Great Expectations                 | ✅       |
| Metadata         | Marquez                            | ✅       |
| Metadata         | OpenAPI/Swagger                    | ✅       |
| Metadata         | OpenMetadata                       | ✅       |
| Metadata         | Open Data Contract Standard (ODCS) | ✅       |
| Metadata         | Amundsen                           | ❌       |
| Metadata         | Datahub                            | ❌       |
| Metadata         | Solace Event Portal                | ❌       |


## Sponsorship

Data Caterer is set up under a sponsorship model. If you require support or additional features from Data Caterer
as an enterprise, you are required to be a sponsor for the project.

[Find out more details here to help with sponsorship.](https://data.catering/sponsor)

## Contributing

[View details here about how you can contribute to the project.](misc/CONTRIBUTING.md)

## Additional Details

## Run Configurations

Different ways to run Data Caterer based on your use case:

![Types of run configurations](misc/design/high_level_flow-run-config.svg)

### Design

[Design motivations and details can be found here.](https://data.catering/setup/design)

### Roadmap

[Can check here for full list.](https://data.catering/use-case/roadmap/)
