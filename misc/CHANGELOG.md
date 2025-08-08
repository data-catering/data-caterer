# Changelog

## 0.16.5

- Allow for deeply nested SQL references in arrays or objects
- Ability to use `unwrapTopLevel` to allow for top-level JSON arrays

## 0.16.4

- Fix bug when using `omit` fields in SQL generated fields

## 0.16.3

- Fix bug when using foreign key relationships with nested fields beyond 3 levels
- Allow for referencing array fields in SQL generated fields

## 0.16.2

- Add in `referenceMode` to data generation config
  - `enableReferenceMode` to step and connection task builders and YAML
- Fix bug when using nested fields with foreign key relationships
- Change order of data generation precendence
  - Now order is `oneOf`, `sql`, `expression`, `regex`, `random`
  - Previous order was `regex`, `oneOf`, `expression`, `sql`, `random`
- Fix bug for deeply nested SQL fields not being applied correctly
- Add in `enableFastGeneration` for automatically applying optimizations for faster completion of data generation

## 0.16.1

- Fixed issue relating to matching tasks from YAML and tasks from metadata

## 0.16.0

- Add in JSON schema as metadata source
- Allow for include/exclude fields to be defined to filter fields to generate

## 0.15.4

- Fix bug with using `messageHeaders` from YAML not being parsed correctly

## 0.15.3

- Ensure correct field options are parsed from YAML to real-time data sources

## 0.15.2

- Various performance improvements
  - Don't call `df.rdd` when zipping with index in foreign key logic
  - Don't call `df.rdd` when checking for unique values
  - When passing metadata to nested fields, don't re-create dataframe
  - Use `unionByName` instead of checking if dataframe is empty then running `union`
  - Set `enableSinkMetadata` to false by default
  - New unique value checking logic using bloom filters
    - Add `uniqueBloomFilterNumItems` to generation config
    - Add `uniqueBloomFilterFalsePositiveProbability` to generation config
  - Update default Spark memory settings
- Update `netty` and `jsonsmart` libraries due to vulnerabilities
- Add `enableUniqueCheckOnlyInBatch` to Scala and Java API

## 0.15.1

- Ensure increment starting number uses `long` data type
- Ensure all rdds are cleared from memory after each batch
- Fix unique data logic
- Add in additional flag to enable/disable unique check only per batch
- Add additional tests

## 0.15.0

- Allow for empty sequences to be generated for per field counts
- Allow users to use field `__index_inc` for generating unique values
- Calculate number of records generated based on foreign key definitions
- Unpersist DataFrame after generating data to avoid OOM errors
- Update to use `jakarta.jms` v3.1.x
  - Use `sol-jms-jakarta` for JMS messaging to Solace
- Add in `rabbitmq` as a data source
- Add in `bigquery` as a data source
- Introduce `oneOfWeighted` data generation for weighted random selection from set of values. Can be used for fields or record count
- Update to use Java 17 across all modules and libraries
- Update to Gradle 8.12

## 0.14.7

- Fix bug when trying to use validations on sub-data source that gets generated from metadata
- Merge in data source options when multiple are defined via connection task builder
- Add in extra debug logs when saving real-time responses

## 0.14.6

- Fix bug relating to sending API calls to management server
- Filter out rows that cannot be transposed before sending alert to Slack

## 0.14.5

- Allow for `iceberg` data to be tracked for validation
- Changed order for `iceberg` connection details in Java to be `name`, `tableName` and `path`
- Add getting metadata source connection details when generated from UI
- When capturing relationship from UI, convert override options to step name
- Fix bug when metadata generated details try to match with user defined details fails when no step options match
- Replaced deprecated usage of `udf` function call with schema type with `selectExpr` with named struct

## 0.14.4

- Ensure step options are persisted when user defined steps come from YAML combined with generated from metadata steps
- Catch all exception when running plan, return error message and exit accordingly
  - Always show how long it took to run the plan

## 0.14.3

- Update Data Contract CLI to capture primary key
- Capture `map` data type when converting from YAML
- Ensure foreign key insert order is correct and pushes non-foreign key data sources to the end
- Update to use insta-integration@v2 github action
- Add `exponentialDistribution` and `normalDistribution` to data generator

## 0.14.0

- Login via UI to run plans
- Login via environment variables to run plans
- Ability to track plan usage and failure
  - Catch failures at any stage in the pipline
    - `parsePlan`
    - `prePlanProcessors`
    - `extractMetadata`
    - `generateData`
    - `validateData`
    - `deleteData`
    - `postPlanProcessors`
    - `planFinish`
- Ability to stop plan running based on quota for feature usage
- Add in pre-plan processing
- Add in helper methods for creating HTTP and message fields
  - `field.messageHeaders`
  - `field.messageHeader`
  - `field.messageBody`
  - `field.httpHeader`
  - `field.httpPathParam`
  - `field.httpQueryParam`
  - `field.httpUrl`
  - `field.httpMethod`
  - `field.httpBody`
- Add in helper YAML fields for creating HTTP and message fields
  - `messageBody`
  - `messageHeaders`
  - `httpBody`
    - `url`
    - `method`
    - `pathParam`
    - `queryParam`
  - `httpHeaders`
  - `httpUrl`
- Fix for automatic retrieval of metadata from Postgres and MySQL failing because of `column` renamed to `field`
- Move more constants to API layer instead of core
- Convert sample errors to string values instead of keeping as DataFrame to make API layer simpler
- HTTP calls now use Pekko for controlling rate of messages
  - DataFrame is collected before being sent to HTTP endpoints
  - More accurate timing of HTTP request and response
- Fix bug when field options defined as non-string get ignored by data generator
- Ensure all tests are run via JUnitRunner
- Reduce size of planFinish payload, only send generation and validation summary
- Use apiToken naming convention to keep consistency
- Don't save real-time responses if schema is empty

## 0.13.1

- Clear exception message on which validation failed to run when running list of validations
- Add test for all field validations being read from YAML
- Create ValidationBuilders for Java API
- Add in missing Matches List field validation

## 0.13.0

Following major changes to API were made:
- `generator` removed from `field`, `count` and `countPerField`, `generator.options` now moved to same level as name `options`
- Foreign key definition change to `source`, `generate` and `delete`
  - Each part of foreign key contains `dataSource`, `step` and `fields`
- Use `field` instead of `column`
  - Shortened version `col` is also changed to `field`
- `schema` removed, `fields` moves down one level
- Validation changes
  - Updates
    - `...Col` -> `...Field` (i.e. `isEqualCol` -> `isEqualField`)
    - `isNot...` -> `is...(negate)` (i.e. `isNotNull` -> `isNull(true)`)
    - `not...` -> `...(negate)` (i.e. `notStartsWith("a")` -> `startsWith("a", true)`)
    - `equalToOr...` -> `...(strictly)` (i.e. `equalToOrLessThan(10)` -> `lessThan(10, false)`)
    - `notIn("a", "b")` -> `in(List("a", "b"), true)`
    - `upstreamValidation...withValidation(...)` -> `upstreamValidation...validations(...)`
  - New
    - `matchesList(regexes, matchAll, negate)`
    - `hasTypes(types, negate)`
    - `distinctInSet(set, negate)`
    - `distinctContainsSet(set, negate)`
    - `distinctEqual(set, negate)`
    - `maxBetween(min, max, negate)`
    - `meanBetween(min, max, negate)`
    - `medianBetween(min, max, negate)`
    - `minBetween(min, max, negate)`
    - `stdDevBetween(min, max, negate)`
    - `sumBetween(min, max, negate)`
    - `lengthBetween(min, max, negate)`
    - `lengthEqual(value, negate)`
    - `isDecreasing(strictly)`
    - `isIncreasing(strictly)`
    - `isJsonParsable(negate)`
    - `matchJsonSchema(schema, negate)`
    - `matchDateTimeFormat(format, negate)`
    - `mostCommonValueInSet(values, negate)`
    - `uniqueValuesProportionBetween(min, max, negate)`
    - `quantileValuesBetween(quantileRanges, negate)`

