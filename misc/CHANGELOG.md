# Changelog

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

