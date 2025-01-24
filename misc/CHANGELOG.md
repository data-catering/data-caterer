# Changelog

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

