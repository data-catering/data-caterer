---
description: "A data generation and testing tool that can automatically discover, generate and validate your data ecosystem"
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---

<h1 align="center" style="padding-top: 50px; padding-bottom: 50px">Data Caterer is a metadata-driven data generation and 
testing tool that aids in creating production-like data across both batch and event data systems. Run data validations 
to ensure your systems have ingested it as expected, then clean up the data afterwards.</h1>

<figure markdown>
  ![Data Caterer generate and validate data flows](diagrams/basic_data_caterer_flow_medium.gif)
</figure>

<h1 align="center">Simplify your data testing</h1>

<h2 align="center">Take away the pain and complexity of your data landscape and let Data Caterer handle it</h2>

<span class="center-content">
[Try now](get-started/docker.md){ .md-button .md-button--primary }
</span>

<h1 class="content-spaced" align="center">Data testing is difficult and fragmented</h1>

- Data being sent via messages, HTTP requests or files and getting stored in databases, file systems, etc.
- Maintaining and updating tests with the latest schemas and business definitions
- Different testing tools for services, jobs or data sources
- Complex relationships between datasets and fields
- Different scenarios, permutations, combinations and edge cases to cover

<h1 class="content-spaced" align="center">Current solutions only cover half the story</h1>

- Specific testing frameworks that support one or limited number of data sources or transport protocols
- Under utilizing metadata from data catalogs or metadata discovery services
- Testing teams having difficulties understanding when failures occur
- Integration tests relying on external teams/services
- Manually generating data, or worse, copying/masking production data into lower environments
- Observability pushes towards being reactive rather than proactive

<span class="center-content">
[Try now](get-started/docker.md){ .md-button .md-button--primary }
</span>

<h1 class="content-spaced" align="center">What you need is a reliable tool that can handle changes to your data landscape</h1> 

<figure markdown>
  ![High level overview of Data Caterer](diagrams/high_level_flow-high-level.svg#only-light)
  ![High level overview of Data Caterer](diagrams/high_level_flow-high-level-dark.svg#only-dark)
</figure>

With Data Caterer, you get:

- Ability to connect to any type of data source: files, SQL or no-SQL databases, messaging systems, HTTP
- Discover metadata from your existing infrastructure and services
- Gain confidence that bugs do not propagate to production
- Be proactive in ensuring changes do not affect other data producers or consumers
- Configurability to run the way you want

<span class="center-content">
[Try now](get-started/docker.md){ .md-button .md-button--primary }
</span>

## Tech Summary

Use the Java, Scala API, or YAML files to help with setup or customisation that are all run via a Docker image. Want to 
get into details? [Checkout the setup pages here](setup/index.md) to get code examples and guides that will take you 
through scenarios and data sources.

Main features include:

- :material-card-search: Metadata discovery
- :material-file: Batch and :material-circle-multiple: event data generation
- :material-vector-difference-ba: Maintain referential integrity across any dataset
- :material-projector-screen-variant-outline: Create custom data generation scenarios
- :material-delete-sweep: Clean up generated data
- :material-check: Validate data
- :material-test-tube: Suggest data validations

[Check other run configurations here.](setup/index.md#high-level-run-configurations)

## What is it

<div class="grid cards" markdown>

-   :material-tools: __Data generation and testing tool__

    ---

    Generate production like data to be consumed and validated.

-   :material-connection: __Designed for any data source__

    ---

    We aim to support pushing data to any data source, in any format.

-   :material-code-tags-check: __Low/no code solution__

    ---

    Can use the tool via either Scala, Java or YAML. Connect to data or metadata sources to generate data and validate.

-   :material-run-fast: __Developer productivity tool__

    ---

    If you are a new developer or seasoned veteran, cut down on your feedback loop when developing with data.

</div>

## What it is not

<div class="grid cards" markdown>

-   :material-brain: __Metadata storage/platform__

    ---

    You could store and use metadata within the data generation/validation tasks but is not the recommended approach.
    Rather, this metadata should be gathered from existing services who handle metadata on behalf of Data Caterer.

-   :fontawesome-solid-handshake: __Data contract__

    ---

    The focus of Data Caterer is on the data generation and testing, which can include details about how the data looks
    like and how it behaves. But it does not encompass all the additional metadata that comes with a data contract such
    as SLAs, security, etc.

-   :material-cloud-download-outline: __Metrics from load testing__

    ---

    Although millions of records can be generated, there are limited capabilities in terms of metric capturing.

</div>

<span class="center-content">
[Try now](get-started/docker.md){ .md-button .md-button--primary }
</span>

<h1 class="content-spaced" align="center">Data Catering vs Other tools vs In-house</h1>

<span class="center-content">

|                        | Data Catering                                                                  | Other tools                               | In-house                                    |
|------------------------|--------------------------------------------------------------------------------|-------------------------------------------|---------------------------------------------|
| <b>Data flow</b>       | Batch and events generation with validation                                    | Batch generation only or validation only  | Depends on architecture and design          |
| <b>Time to results</b> | 1 day                                                                          | 1+ month to integrate, deploy and onboard | 1+ month to build and deploy                |
| <b>Solution</b>        | Connect with your existing data ecosystem, automatic generation and validation | Manual UI data entry or via SDK           | Depends on engineer(s) building it          |

</span>
