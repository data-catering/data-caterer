---
title: "Data Generation"
description: "Data Caterer can generate all kinds of data structures including simple fields, nested fields, reference other fields in calculating values. It can also connect to metadata sources such as OpenMetadata or OpenAPI/Swagger to retrieve schema information to generate data."
image: "https://data.catering/diagrams/logo/data_catering_logo.svg"
---
# Data Generators

## Data Types

Below is a list of all supported data types for generating data:

| Data Type                              | Options                              |
|----------------------------------------|--------------------------------------|
| string, StringType                     | [String options](#string)            |
| integer, IntegerType                   | [Integer options](#integerlongshort) |
| long, LongType                         | [Long options](#integerlongshort)    |
| short, ShortType                       | [Short options](#integerlongshort)   |
| decimal(precision, scale), DecimalType | [Decimal options](#decimal)          |
| double, DoubleType                     | [Double options](#doublefloat)       |
| float, FloatType                       | [Float options](#doublefloat)        |
| date, DateType                         | [Date options](#date)                |
| timestamp, TimestampType               | [Timestamp options](#timestamp)      |
| boolean, BooleanType                   |                                      |
| binary, BinaryType                     | [Binary options](#binary)            |
| byte, ByteType                         | [Byte options](#binary)              |
| array, ArrayType                       | [Array options](#array)              |
| struct, StructType                     |                                      |

## Options

### All data types

Some options are available to use for all types of data generators. Below is the list along with example and
descriptions:

| Option                | Default | Example                                                                                             | Description                                                                                                                                                                                                                                                                                     |
|-----------------------|---------|-----------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `enableEdgeCase`      | false   | `enableEdgeCase: "true"`                                                                            | Enable/disable generated data to contain edge cases based on the data type. For example, integer data type has edge cases of (Int.MaxValue, Int.MinValue and 0)                                                                                                                                 |
| `edgeCaseProbability` | 0.0     | `edgeCaseProb: "0.1"`                                                                               | Probability of generating a random edge case value if `enableEdgeCase` is true                                                                                                                                                                                                                  |
| `isUnique`            | false   | `isUnique: "true"`                                                                                  | Enable/disable generated data to be unique for that field. Errors will be thrown when it is unable to generate unique data                                                                                                                                                                      |
| `regex`               | <empty> | `regex: "ACC[0-9]{10}"`                                                                             | Regular expression to define pattern generated data should follow                                                                                                                                                                                                                               |
| `seed`                | <empty> | `seed: "1"`                                                                                         | Defines the random seed for generating data for that particular field. It will override any seed defined at a global level                                                                                                                                                                      |
| `sql`                 | <empty> | `sql: "CASE WHEN amount < 10 THEN true ELSE false END"`                                             | Define any SQL statement for generating that fields value. Computation occurs after all non-SQL fields are generated. This means any fields used in the SQL cannot be based on other SQL generated fields. Data type of generated value from SQL needs to match data type defined for the field. See [Advanced SQL Generation](#advanced-sql-generation) for more examples |
| `oneOf`               | <empty> | `oneOf: ["open", "closed", "suspended"]` or `oneOf: ["open->0.8", "closed->0.1", "suspended->0.1"]` | Field can only take one of the prescribed values. Chance of value being chosen is based on the weight assigned to it. Weight can be any double value.                                                                                                                                           |
| `omit`                | false   | `omit: "true"`                                                                                      | If true, field will not be included in final data generated. Useful for intermediate transformations that are not included in final outcome                                                                                                                                                     |

### String

| Option            | Default | Example                                                                                      | Description                                                                                                                                                                                                                    |
|-------------------|---------|----------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `minLen`          | 1       | `minLen: "2"`                                                                                | Ensures that all generated strings have at least length `minLen`                                                                                                                                                               |
| `maxLen`          | 10      | `maxLen: "15"`                                                                               | Ensures that all generated strings have at most length `maxLen`                                                                                                                                                                |
| `expression`      | <empty> | `expression: "#{Name.name}"`<br/>`expression:"#{Address.city}/#{Demographic.maritalStatus}"` | Will generate a string based on the faker expression provided. All possible faker expressions can be found [**here**](../../sample/datafaker/expressions.txt)<br/> Expression has to be in format `#{<faker expression name>}` |
| `enableNull`      | false   | `enableNull: "true"`                                                                         | Enable/disable null values being generated                                                                                                                                                                                     |
| `nullProbability` | 0.0     | `nullProb: "0.1"`                                                                            | Probability to generate null values if `enableNull` is true                                                                                                                                                                    |
| `uuid`            | <empty> | `uuid: "account_id"`                                                                         | Generate a UUID value. If value is non-empty, UUID value will be generated based off column value                                                                                                                              |

**Edge cases**: ("", "\n", "\r", "\t", " ", "\\u0000", "\\ufff", "İyi günler", "Спасибо", "Καλημέρα", "صباح الخير", "
Förlåt", "你好吗", "Nhà vệ sinh ở đâu", "こんにちは", "नमस्ते", "Բարեւ", "Здравейте")

#### Sample

[:material-run-fast: Scala Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/scala/io/github/datacatering/plan/DocumentationPlanRun.scala) | [:material-coffee: Java Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/java/io/github/datacatering/plan/DocumentationJavaPlanRun.java)

=== "Java"

    ```java
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field()
          .name("customer_name")
          .type(StringType.instance())
          .expression("#{Name.name}")
          .enableNull(true)
          .nullProbability(0.1)
          .minLength(4)
          .maxLength(20),
        field()
          .name("account_id")
          .type(StringType.instance())
          .regex("ACC[0-9]{10}")
          .isUnique(true)
          .enableEdgeCase(true)
          .edgeCaseProbability(0.05),
        field()
          .name("status")
          .type(StringType.instance())
          .oneOf("open", "closed", "suspended"),
        field()
          .name("priority")
          .type(StringType.instance())
          .oneOf("high->0.1", "medium->0.7", "low->0.2"),
        field()
          .name("user_uuid")
          .type(StringType.instance())
          .uuid("user_id"),
        field()
          .name("address")
          .type(StringType.instance())
          .expression("#{Address.city}/#{Demographic.maritalStatus}")
          .minLength(10)
          .maxLength(50),
        field()
          .name("calculated_field")
          .type(StringType.instance())
          .sql("CASE WHEN amount < 10 THEN 'small' ELSE 'large' END")
      );
    ```

=== "Scala"

    ```scala
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field
          .name("customer_name")
          .`type`(StringType)
          .expression("#{Name.name}")
          .enableNull(true)
          .nullProbability(0.1)
          .minLength(4)
          .maxLength(20),
        field
          .name("account_id")
          .`type`(StringType)
          .regex("ACC[0-9]{10}")
          .isUnique(true)
          .enableEdgeCase(true)
          .edgeCaseProbability(0.05),
        field
          .name("status")
          .`type`(StringType)
          .oneOf("open", "closed", "suspended"),
        field
          .name("priority")
          .`type`(StringType)
          .oneOf("high->0.1", "medium->0.7", "low->0.2"),
        field
          .name("user_uuid")
          .`type`(StringType)
          .uuid("user_id"),
        field
          .name("address")
          .`type`(StringType)
          .expression("#{Address.city}/#{Demographic.maritalStatus}")
          .minLength(10)
          .maxLength(50),
        field
          .name("calculated_field")
          .`type`(StringType)
          .sql("CASE WHEN amount < 10 THEN 'small' ELSE 'large' END")
      )
    ```

=== "YAML"

    ```yaml
    name: "csv_file"
    steps:
      - name: "transactions"
        type: "csv"
        options:
          path: "app/src/test/resources/sample/csv/transactions"
        fields:
          - name: "customer_name"
            type: "string"
            options:
              expression: "#{Name.name}"
              enableNull: true
              nullProb: 0.1
              minLen: 4
              maxLen: 20
          - name: "account_id"
            type: "string"
            options:
              regex: "ACC[0-9]{10}"
              isUnique: true
              enableEdgeCase: true
              edgeCaseProb: 0.05
          - name: "status"
            type: "string"
            options:
              oneOf: ["open", "closed", "suspended"]
          - name: "priority"
            type: "string"
            options:
              oneOf: ["high->0.1", "medium->0.7", "low->0.2"]
          - name: "user_uuid"
            type: "string"
            options:
              uuid: "user_id"
          - name: "address"
            type: "string"
            options:
              expression: "#{Address.city}/#{Demographic.maritalStatus}"
              minLen: 10
              maxLen: 50
          - name: "calculated_field"
            type: "string"
            options:
              sql: "CASE WHEN amount < 10 THEN 'small' ELSE 'large' END"
    ```

### Numeric

For all the numeric data types, there are 4 options to choose from: min, max and maxValue.
Generally speaking, you only need to define one of min or minValue, similarly with max or maxValue.  
The reason why there are 2 options for each is because of when metadata is automatically gathered, we gather the
statistics of the observed min and max values. Also, it will attempt to gather any restriction on the min or max value
as defined by the data source (i.e. max value as per database type).

#### Integer/Long/Short

| Option                  | Default     | Example                        | Description                                                                         |
|-------------------------|-------------|--------------------------------|-------------------------------------------------------------------------------------|
| `min`                   | 0           | `min: "2"`                     | Ensures that all generated values are greater than or equal to `min`                |
| `max`                   | 1000        | `max: "25"`                    | Ensures that all generated values are less than or equal to `max`                   |
| `stddev`                | 1.0         | `stddev: "2.0"`                | Standard deviation for normal distributed data                                      |
| `mean`                  | `max - min` | `mean: "5.0"`                  | Mean for normal distributed data                                                    |
| `distribution`          | <empty>     | `distribution: "exponential"`  | Type of distribution of the data. Either `exponential` or `normal`                  |
| `distributionRateParam` | <empty>     | `distributionRateParam: "1.0"` | If distribution is `exponential`, rate parameter to adjust exponential distribution |
| `incremental`           | 1           | `incremental: "1"`             | Values will be incremental. Define a start number to increment from                 |

**Edge cases Integer**: (2147483647, -2147483648, 0)  
**Edge cases Long**: (9223372036854775807, -9223372036854775808, 0)  
**Edge cases Short**: (32767, -32768, 0)

##### Sample

=== "Java"

    ```java
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field().name("year").type(IntegerType.instance()).min(2020).max(2023),
        field().name("customer_id").type(LongType.instance()).incremental(1000),
        field().name("customer_group").type(ShortType.instance()).oneOf("1", "2", "3"),
        field().name("transaction_amount").type(IntegerType.instance())
          .min(1).max(1000)
          .distribution("normal")
          .mean(100.0)
          .stddev(25.0),
        field().name("retry_count").type(IntegerType.instance())
          .min(0).max(10)
          .distribution("exponential")
          .distributionRateParam(2.0),
        field().name("unique_sequence").type(IntegerType.instance())
          .min(1).max(99999)
          .isUnique(true)
          .enableEdgeCase(true)
          .edgeCaseProbability(0.1),
        field().name("priority_level").type(ShortType.instance())
          .oneOf("high->1", "medium->2", "low->3")
          .regex("[1-3]"),
        field().name("calculated_score").type(IntegerType.instance())
          .sql("CASE WHEN transaction_amount > 500 THEN 100 ELSE 50 END")
      );
    ```

=== "Scala"

    ```scala
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field.name("year").`type`(IntegerType).min(2020).max(2023),
        field.name("customer_id").`type`(LongType).incremental(1000),
        field.name("customer_group").`type`(ShortType).oneOf("1", "2", "3"),
        field.name("transaction_amount").`type`(IntegerType)
          .min(1).max(1000)
          .distribution("normal")
          .mean(100.0)
          .stddev(25.0),
        field.name("retry_count").`type`(IntegerType)
          .min(0).max(10)
          .distribution("exponential")
          .distributionRateParam(2.0),
        field.name("unique_sequence").`type`(IntegerType)
          .min(1).max(99999)
          .isUnique(true)
          .enableEdgeCase(true)
          .edgeCaseProbability(0.1),
        field.name("priority_level").`type`(ShortType)
          .oneOf("high->1", "medium->2", "low->3")
          .regex("[1-3]"),
        field.name("calculated_score").`type`(IntegerType)
          .sql("CASE WHEN transaction_amount > 500 THEN 100 ELSE 50 END")
      )
    ```

=== "YAML"

    ```yaml
    name: "csv_file"
    steps:
      - name: "transactions"
        type: "csv"
        options:
          path: "app/src/test/resources/sample/csv/transactions"
        fields:
          - name: "year"
            type: "integer"
            options:
              min: 2020
              max: 2023
          - name: "customer_id"
            type: "long"
            options:
              incremental: 1000
          - name: "customer_group"
            type: "short"
            options:
              oneOf: ["1", "2", "3"]
          - name: "transaction_amount"
            type: "integer"
            options:
              min: 1
              max: 1000
              distribution: "normal"
              mean: 100.0
              stddev: 25.0
          - name: "retry_count"
            type: "integer"
            options:
              min: 0
              max: 10
              distribution: "exponential"
              distributionRateParam: 2.0
          - name: "unique_sequence"
            type: "integer"
            options:
              min: 1
              max: 99999
              isUnique: true
              enableEdgeCase: true
              edgeCaseProb: 0.1
          - name: "priority_level"
            type: "short"
            options:
              oneOf: ["high->1", "medium->2", "low->3"]
              regex: "[1-3]"
          - name: "calculated_score"
            type: "integer"
            options:
              sql: "CASE WHEN transaction_amount > 500 THEN 100 ELSE 50 END"
    ```

#### Decimal


| Option                  | Default     | Example                        | Description                                                                                             |
|-------------------------|-------------|--------------------------------|---------------------------------------------------------------------------------------------------------|
| `min`                   | 0           | `min: "2"`                     | Ensures that all generated values are greater than or equal to `min`                                    |
| `max`                   | 1000        | `max: "25"`                    | Ensures that all generated values are less than or equal to `max`                                       |
| `stddev`                | 1.0         | `stddev: "2.0"`                | Standard deviation for normal distributed data                                                          |
| `mean`                  | `max - min` | `mean: "5.0"`                  | Mean for normal distributed data                                                                        |
| `numericPrecision`      | 10          | `precision: "25"`              | The maximum number of digits                                                                            |
| `numericScale`          | 0           | `scale: "25"`                  | The number of digits on the right side of the decimal point (has to be less than or equal to precision) |
| `distribution`          | <empty>     | `distribution: "exponential"`  | Type of distribution of the data. Either `exponential` or `normal`                                      |
| `distributionRateParam` | <empty>     | `distributionRateParam: "1.0"` | If distribution is `exponential`, rate parameter to adjust exponential distribution                     |

**Edge cases Decimal**: (9223372036854775807, -9223372036854775808, 0)

##### Sample

=== "Java"

    ```java
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field().name("account_balance").type(DecimalType.instance())
          .numericPrecision(10).numericScale(2)
          .min(new BigDecimal("0.00")).max(new BigDecimal("99999.99")),
        field().name("interest_rate").type(DecimalType.instance())
          .numericPrecision(5).numericScale(4)
          .min(new BigDecimal("0.0001")).max(new BigDecimal("0.2500"))
          .distribution("normal")
          .mean(0.05)
          .stddev(0.02),
        field().name("commission_rate").type(DecimalType.instance())
          .numericPrecision(6).numericScale(3)
          .oneOf("0.025", "0.050", "0.075"),
        field().name("bonus_multiplier").type(DecimalType.instance())
          .numericPrecision(3).numericScale(1)
          .distribution("exponential")
          .distributionRateParam(1.5)
          .enableEdgeCase(true)
          .edgeCaseProbability(0.05),
        field().name("unique_transaction_fee").type(DecimalType.instance())
          .numericPrecision(8).numericScale(2)
          .min(new BigDecimal("1.00")).max(new BigDecimal("999.99"))
          .isUnique(true),
        field().name("calculated_total").type(DecimalType.instance())
          .numericPrecision(12).numericScale(2)
          .sql("CASE WHEN account_balance > 1000 THEN account_balance * 1.1 ELSE account_balance END")
      );
    ```

=== "Scala"

    ```scala
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field.name("account_balance").`type`(DecimalType)
          .numericPrecision(10).numericScale(2)
          .min(new java.math.BigDecimal("0.00")).max(new java.math.BigDecimal("99999.99")),
        field.name("interest_rate").`type`(DecimalType)
          .numericPrecision(5).numericScale(4)
          .min(new java.math.BigDecimal("0.0001")).max(new java.math.BigDecimal("0.2500"))
          .distribution("normal")
          .mean(0.05)
          .stddev(0.02),
        field.name("commission_rate").`type`(DecimalType)
          .numericPrecision(6).numericScale(3)
          .oneOf("0.025", "0.050", "0.075"),
        field.name("bonus_multiplier").`type`(DecimalType)
          .numericPrecision(3).numericScale(1)
          .distribution("exponential")
          .distributionRateParam(1.5)
          .enableEdgeCase(true)
          .edgeCaseProbability(0.05),
        field.name("unique_transaction_fee").`type`(DecimalType)
          .numericPrecision(8).numericScale(2)
          .min(new java.math.BigDecimal("1.00")).max(new java.math.BigDecimal("999.99"))
          .isUnique(true),
        field.name("calculated_total").`type`(DecimalType)
          .numericPrecision(12).numericScale(2)
          .sql("CASE WHEN account_balance > 1000 THEN account_balance * 1.1 ELSE account_balance END")
      )
    ```

=== "YAML"

    ```yaml
    name: "csv_file"
    steps:
      - name: "transactions"
        type: "csv"
        options:
          path: "app/src/test/resources/sample/csv/transactions"
        fields:
          - name: "account_balance"
            type: "decimal"
            options:
              precision: 10
              scale: 2
              min: "0.00"
              max: "99999.99"
          - name: "interest_rate"
            type: "decimal"
            options:
              precision: 5
              scale: 4
              min: "0.0001"
              max: "0.2500"
              distribution: "normal"
              mean: 0.05
              stddev: 0.02
          - name: "commission_rate"
            type: "decimal"
            options:
              precision: 6
              scale: 3
              oneOf: ["0.025", "0.050", "0.075"]
          - name: "bonus_multiplier"
            type: "decimal"
            options:
              precision: 3
              scale: 1
              distribution: "exponential"
              distributionRateParam: 1.5
              enableEdgeCase: true
              edgeCaseProb: 0.05
          - name: "unique_transaction_fee"
            type: "decimal"
            options:
              precision: 8
              scale: 2
              min: "1.00"
              max: "999.99"
              isUnique: true
          - name: "calculated_total"
            type: "decimal"
            options:
              precision: 12
              scale: 2
              sql: "CASE WHEN account_balance > 1000 THEN account_balance * 1.1 ELSE account_balance END"
    ```

#### Double/Float

| Option                  | Default     | Example                        | Description                                                                         |
|-------------------------|-------------|--------------------------------|-------------------------------------------------------------------------------------|
| `min`                   | 0.0         | `min: "2.1"`                   | Ensures that all generated values are greater than or equal to `min`                |
| `max`                   | 1000.0      | `max: "25.9"`                  | Ensures that all generated values are less than or equal to `max`                   |
| `round`                 | N/A         | `round: "2"`                   | Round to particular number of decimal places                                        |
| `stddev`                | 1.0         | `stddev: "2.0"`                | Standard deviation for normal distributed data                                      |
| `mean`                  | `max - min` | `mean: "5.0"`                  | Mean for normal distributed data                                                    |
| `round`                 | <empty>     | `round: "2"`                   | Number of decimal places to round to (round up)                                     |
| `distribution`          | <empty>     | `distribution: "exponential"`  | Type of distribution of the data. Either `exponential` or `normal`                  |
| `distributionRateParam` | <empty>     | `distributionRateParam: "1.0"` | If distribution is `exponential`, rate parameter to adjust exponential distribution |

**Edge cases Double**: (+infinity, 1.7976931348623157e+308, 4.9e-324, 0.0, -0.0, -1.7976931348623157e+308, -infinity,
NaN)  
**Edge cases Float**: (+infinity, 3.4028235e+38, 1.4e-45, 0.0, -0.0, -3.4028235e+38, -infinity, NaN)

##### Sample

=== "Java"

    ```java
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field().name("transaction_amount").type(DoubleType.instance())
          .min(1.0).max(10000.0)
          .round(2),
        field().name("processing_fee").type(FloatType.instance())
          .min(0.5f).max(99.99f)
          .round(2)
          .distribution("normal")
          .mean(5.0)
          .stddev(2.0),
        field().name("exchange_rate").type(DoubleType.instance())
          .min(0.1).max(5.0)
          .round(4)
          .distribution("exponential")
          .distributionRateParam(0.8),
        field().name("discount_percentage").type(FloatType.instance())
          .oneOf("0.05", "0.10", "0.15", "0.25"),
        field().name("unique_score").type(DoubleType.instance())
          .min(0.0).max(100.0)
          .round(3)
          .isUnique(true)
          .enableEdgeCase(true)
          .edgeCaseProbability(0.02),
        field().name("weighted_value").type(DoubleType.instance())
          .oneOf("low->10.5", "medium->25.75", "high->50.0")
          .round(2),
        field().name("calculated_ratio").type(FloatType.instance())
          .sql("CASE WHEN transaction_amount > 1000 THEN 1.5 ELSE 1.0 END")
          .round(1)
      );
    ```

=== "Scala"

    ```scala
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field.name("transaction_amount").`type`(DoubleType)
          .min(1.0).max(10000.0)
          .round(2),
        field.name("processing_fee").`type`(FloatType)
          .min(0.5f).max(99.99f)
          .round(2)
          .distribution("normal")
          .mean(5.0)
          .stddev(2.0),
        field.name("exchange_rate").`type`(DoubleType)
          .min(0.1).max(5.0)
          .round(4)
          .distribution("exponential")
          .distributionRateParam(0.8),
        field.name("discount_percentage").`type`(FloatType)
          .oneOf("0.05", "0.10", "0.15", "0.25"),
        field.name("unique_score").`type`(DoubleType)
          .min(0.0).max(100.0)
          .round(3)
          .isUnique(true)
          .enableEdgeCase(true)
          .edgeCaseProbability(0.02),
        field.name("weighted_value").`type`(DoubleType)
          .oneOf("low->10.5", "medium->25.75", "high->50.0")
          .round(2),
        field.name("calculated_ratio").`type`(FloatType)
          .sql("CASE WHEN transaction_amount > 1000 THEN 1.5 ELSE 1.0 END")
          .round(1)
      )
    ```

=== "YAML"

    ```yaml
    name: "csv_file"
    steps:
      - name: "transactions"
        type: "csv"
        options:
          path: "app/src/test/resources/sample/csv/transactions"
        fields:
          - name: "transaction_amount"
            type: "double"
            options:
              min: 1.0
              max: 10000.0
              round: 2
          - name: "processing_fee"
            type: "float"
            options:
              min: 0.5
              max: 99.99
              round: 2
              distribution: "normal"
              mean: 5.0
              stddev: 2.0
          - name: "exchange_rate"
            type: "double"
            options:
              min: 0.1
              max: 5.0
              round: 4
              distribution: "exponential"
              distributionRateParam: 0.8
          - name: "discount_percentage"
            type: "float"
            options:
              oneOf: ["0.05", "0.10", "0.15", "0.25"]
          - name: "unique_score"
            type: "double"
            options:
              min: 0.0
              max: 100.0
              round: 3
              isUnique: true
              enableEdgeCase: true
              edgeCaseProb: 0.02
          - name: "weighted_value"
            type: "double"
            options:
              oneOf: ["low->10.5", "medium->25.75", "high->50.0"]
              round: 2
          - name: "calculated_ratio"
            type: "float"
            options:
              sql: "CASE WHEN transaction_amount > 1000 THEN 1.5 ELSE 1.0 END"
              round: 1
    ```

## Step options

You can control behavior at the step level for generation.

### Enable/disable generation per step

Disable data generation for specific steps when you want to use them only for reference in foreign key relationships.

[:material-run-fast: Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/scala/io/github/datacatering/plan/FastGenerationAndReferencePlanRun.scala)

=== "Java"

    ```java
    csv("customer_accounts", "/opt/app/data/customer/account", Map.of("header", "true"))
      .enableDataGeneration(false);
    ```

=== "Scala"

    ```scala
    csv("customer_accounts", "/opt/app/data/customer/account", Map("header" -> "true"))
      .enableDataGeneration(false)
    ```

=== "YAML"

    In `plan.yaml`:
    
    ```yaml
    name: "my_plan"
    tasks:
      - name: "csv_accounts_task"
        dataSourceName: "csv_accounts"
        enabled: false  # disable generation for this task
    ```
    
    In task file:
    
    ```yaml
    name: "csv_accounts_task"
    steps:
      - name: "accounts"
        type: "csv"
        options:
          path: "/opt/app/data/customer/account"
          header: "true"
        enableDataGeneration: false
    ```

### Partitioning and throughput

Control data partitioning and parallelism for improved performance with large datasets.

[:material-run-fast: Scala Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/scala/io/github/datacatering/plan/FastGenerationAndReferencePlanRun.scala)

=== "Java"

    ```java
    csv("customer_accounts", "/opt/app/data/customer/account")
      .partitionBy("year", "account_id")
      .numPartitions(8);
    ```

=== "Scala"

    ```scala
    csv("customer_accounts", "/opt/app/data/customer/account")
      .partitionBy("year", "account_id")
      .numPartitions(8)
    ```

=== "YAML"

    ```yaml
    name: "csv_accounts_task" 
    steps:
      - name: "accounts"
        type: "csv"
        options:
          path: "/opt/app/data/customer/account"
          partitionBy: "year,account_id"
          partitions: 8
        fields:
          - name: "account_id"
          - name: "year"
            type: "integer"
    ```

### Single file output for file formats

When generating file-based data (CSV, JSON, Parquet, etc.), you can generate a single consolidated file instead of Spark's default partitioned output by specifying a file extension in the path.

By default, Spark writes data to a directory containing multiple part files (e.g., `part-00000-xxx.csv`, `part-00001-xxx.csv`). When you include a file extension in the path (e.g., `/path/to/output.csv`), Data Caterer will automatically:

1. Generate data using Spark to a temporary directory
2. Consolidate all part files into a single file with your specified name
3. Clean up the temporary directory

**Special handling for CSV with headers**: When using `header: "true"` with CSV format, the consolidation process ensures the header row appears only once at the top of the file, even when multiple partitions are generated.

=== "Java"

    ```java
    // Single CSV file with header
    csv("transactions", "/opt/app/data/transactions.csv")
      .option("header", "true")
      .fields(
        field().name("account_id"),
        field().name("amount").type(DoubleType.instance())
      );

    // Single JSON file
    json("events", "/opt/app/data/events.json")
      .fields(field().name("event_type"));

    // Single Parquet file
    parquet("data", "/opt/app/data/records.parquet")
      .fields(field().name("id"));

    // Directory with multiple part files (default Spark behavior)
    csv("bulk_data", "/opt/app/data/bulk_output")
      .fields(field().name("data"));
    ```

=== "Scala"

    ```scala
    // Single CSV file with header
    csv("transactions", "/opt/app/data/transactions.csv")
      .option("header", "true")
      .fields(
        field.name("account_id"),
        field.name("amount").`type`(DoubleType)
      )

    // Single JSON file
    json("events", "/opt/app/data/events.json")
      .fields(field.name("event_type"))

    // Single Parquet file
    parquet("data", "/opt/app/data/records.parquet")
      .fields(field.name("id"))

    // Directory with multiple part files (default Spark behavior)
    csv("bulk_data", "/opt/app/data/bulk_output")
      .fields(field.name("data"))
    ```

=== "YAML"

    ```yaml
    # Single CSV file with header
    name: "csv_single_file"
    steps:
      - name: "transactions"
        type: "csv"
        options:
          path: "/opt/app/data/transactions.csv"
          header: "true"
        fields:
          - name: "account_id"
          - name: "amount"
            type: "double"

    # Single JSON file
    ---
    name: "json_single_file"
    steps:
      - name: "events"
        type: "json"
        options:
          path: "/opt/app/data/events.json"
        fields:
          - name: "event_type"

    # Directory with multiple part files (default)
    ---
    name: "bulk_output"
    steps:
      - name: "bulk_data"
        type: "csv"
        options:
          path: "/opt/app/data/bulk_output"
        fields:
          - name: "data"
    ```

**Supported file formats**: `.csv`, `.json`, `.parquet`, `.orc`, `.xml`, `.txt`

**Performance note**: For large datasets, using directory output (without file extension) will be faster as it leverages Spark's parallel writing capabilities.

### Data source specific options

Data Caterer uses Apache Spark as its underlying data generation engine. This means you can use any Spark data source options in your step configuration to control how data is written.

!!! info "Spark Data Source Options"
    Step options can include any options supported by the corresponding Spark data source. These options are passed directly to Spark's DataFrameWriter.

    **Common data source option references:**

    - **CSV**: [Spark CSV Options](https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option)
    - **JSON**: [Spark JSON Options](https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option)
    - **Parquet**: [Spark Parquet Options](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option)
    - **ORC**: [Spark ORC Options](https://spark.apache.org/docs/latest/sql-data-sources-orc.html#configuration)
    - **JDBC**: [Spark JDBC Options](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option)
    - **Kafka**: [Spark Kafka Options](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#writing-data-to-kafka)
    - **Delta Lake**: [Delta Lake Options](https://docs.delta.io/latest/delta-batch.html#write-to-a-table)
    - **Iceberg**: [Iceberg Spark Options](https://iceberg.apache.org/docs/latest/spark-writes/)

Examples of commonly used Spark options:

=== "Java"

    ```java
    // CSV with custom delimiter and compression
    csv("data", "/opt/app/data/output.csv")
      .option("header", "true")
      .option("delimiter", "|")
      .option("compression", "gzip")
      .option("quote", "\"")
      .option("escape", "\\")
      .fields(field().name("col1"), field().name("col2"));

    // JSON with date format and pretty printing
    json("data", "/opt/app/data/output")
      .option("dateFormat", "yyyy-MM-dd")
      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
      .option("compression", "gzip")
      .fields(field().name("date").type(DateType.instance()));

    // Parquet with compression
    parquet("data", "/opt/app/data/output")
      .option("compression", "snappy")
      .option("parquet.block.size", "268435456")
      .fields(field().name("data"));

    // JDBC with batch size and isolation level
    postgres("transactions", "jdbc:postgresql://localhost:5432/db")
      .table("schema", "transactions")
      .option("batchsize", "10000")
      .option("isolationLevel", "READ_COMMITTED")
      .option("truncate", "true")
      .fields(field().name("id"));
    ```

=== "Scala"

    ```scala
    // CSV with custom delimiter and compression
    csv("data", "/opt/app/data/output.csv")
      .option("header", "true")
      .option("delimiter", "|")
      .option("compression", "gzip")
      .option("quote", "\"")
      .option("escape", "\\")
      .fields(field.name("col1"), field.name("col2"))

    // JSON with date format and pretty printing
    json("data", "/opt/app/data/output")
      .option("dateFormat", "yyyy-MM-dd")
      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
      .option("compression", "gzip")
      .fields(field.name("date").`type`(DateType))

    // Parquet with compression
    parquet("data", "/opt/app/data/output")
      .option("compression", "snappy")
      .option("parquet.block.size", "268435456")
      .fields(field.name("data"))

    // JDBC with batch size and isolation level
    postgres("transactions", "jdbc:postgresql://localhost:5432/db")
      .table("schema", "transactions")
      .option("batchsize", "10000")
      .option("isolationLevel", "READ_COMMITTED")
      .option("truncate", "true")
      .fields(field.name("id"))
    ```

=== "YAML"

    ```yaml
    # CSV with custom options
    name: "csv_custom"
    steps:
      - name: "data"
        type: "csv"
        options:
          path: "/opt/app/data/output.csv"
          header: "true"
          delimiter: "|"
          compression: "gzip"
          quote: "\""
          escape: "\\"
        fields:
          - name: "col1"
          - name: "col2"

    # JSON with date formatting
    ---
    name: "json_formatted"
    steps:
      - name: "data"
        type: "json"
        options:
          path: "/opt/app/data/output"
          dateFormat: "yyyy-MM-dd"
          timestampFormat: "yyyy-MM-dd'T'HH:mm:ss"
          compression: "gzip"
        fields:
          - name: "date"
            type: "date"

    # Parquet with compression
    ---
    name: "parquet_compressed"
    steps:
      - name: "data"
        type: "parquet"
        options:
          path: "/opt/app/data/output"
          compression: "snappy"
          parquet.block.size: "268435456"
        fields:
          - name: "data"

    # JDBC with custom options
    ---
    name: "jdbc_custom"
    steps:
      - name: "transactions"
        type: "postgres"
        options:
          url: "jdbc:postgresql://localhost:5432/db"
          dbtable: "schema.transactions"
          batchsize: "10000"
          isolationLevel: "READ_COMMITTED"
          truncate: "true"
        fields:
          - name: "id"
    ```

## Reference mode

Use existing data as reference instead of generating new data for a step. This is useful when you want to reference real data in foreign key relationships.

[:material-run-fast: Scala Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/scala/io/github/datacatering/plan/FastGenerationAndReferencePlanRun.scala) | [:material-coffee: Java Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/java/io/github/datacatering/plan/FastGenerationAndReferenceJavaPlanRun.java) | [:simple-yaml: YAML Example](https://github.com/data-catering/data-caterer/blob/main/example/docker/data/custom/task/file/json/reference-data-task.yaml)

=== "Java"

    ```java
    json("reference_json", "/opt/app/data/reference/json")
      .enableReferenceMode(true);
    ```

=== "Scala"

    ```scala
    json("reference_json", "/opt/app/data/reference/json")
      .enableReferenceMode(true)
    ```

=== "YAML"

    ```yaml
    name: "reference_json_task"
    steps:
      - name: "reference_data"
        type: "json"
        options:
          path: "/opt/app/data/reference/json"
          enableReferenceMode: true
          enableDataGeneration: false
    ```

## Key positions and clustering

For databases that support primary key positions and clustering order (like Cassandra), you can specify the order of primary keys and clustering keys.

[:material-run-fast: Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/scala/io/github/datacatering/plan/FastGenerationAndReferencePlanRun.scala)

=== "Java"

    ```java
    cassandra("customer_cassandra", "localhost:9042")
      .table("account", "accounts")
      .fields(
        field().name("id").primaryKey(true).primaryKeyPosition(1),
        field().name("event_time").clusteringPosition(1)
      );
    ```

=== "Scala"

    ```scala
    cassandra("customer_cassandra", "localhost:9042")
      .table("account", "accounts")
      .fields(
        field.name("id").primaryKey(true).primaryKeyPosition(1),
        field.name("event_time").clusteringPosition(1)
      )
    ```

=== "YAML"

    ```yaml
    name: "cassandra_accounts_task"
    steps:
      - name: "accounts"
        type: "cassandra"
        options:
          keyspace: "account"
          table: "accounts"
        fields:
          - name: "id"
            options:
              isPrimaryKey: true
              primaryKeyPosition: 1
          - name: "event_time"
            type: "timestamp"
            options:
              clusteringPosition: 1
    ```

## Advanced SQL Generation

Data Caterer supports complex SQL expressions for generating sophisticated data relationships and calculations. SQL expressions are evaluated after all non-SQL fields have been generated.

[:material-run-fast: Scala Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/scala/io/github/datacatering/plan/DocumentationPlanRun.scala) | [:material-coffee: Java Example](https://github.com/data-catering/data-caterer/blob/main/example/src/main/java/io/github/datacatering/plan/DocumentationJavaPlanRun.java)

### Array Operations and Aggregations

Extract data from arrays, perform aggregations, and create complex nested data structures.

=== "Java"

    ```java
    // Extract latest status from array of updates
    field().name("current_status")
        .sql("element_at(sort_array(update_history.status, false), 1)"),
    
    // Calculate year from date field    
    field().name("year").type(IntegerType.instance())
        .sql("YEAR(date)"),
        
    // Get first transaction date from sorted array
    field().name("first_transaction_date").type(DateType.instance())
        .sql("element_at(sort_array(transactions.transaction_date), 1)")
    ```

=== "Scala" 

    ```scala
    // Extract latest status from array of updates
    field.name("current_status")
      .sql("element_at(sort_array(update_history.status, false), 1)"),
    
    // Calculate year from date field
    field.name("year").`type`(IntegerType)
      .sql("YEAR(date)"),
      
    // Get first transaction date from sorted array  
    field.name("first_transaction_date").`type`(DateType)
      .sql("element_at(sort_array(transactions.transaction_date), 1)")
    ```

=== "YAML"

    ```yaml
    fields:
      - name: "current_status"
        options:
          sql: "element_at(sort_array(update_history.status, false), 1)"
      - name: "year" 
        type: "integer"
        options:
          sql: "YEAR(date)"
      - name: "first_transaction_date"
        type: "date"
        options:
          sql: "element_at(sort_array(transactions.transaction_date), 1)"
    ```

### Nested Field References and Calculations

Access nested fields and perform complex calculations across multiple data levels.

=== "Java"

    ```java
    // Reference nested field value
    field().name("customer_name")
        .sql("customer_details.name"),
        
    // Calculate balance with interest based on account type
    field().name("balance_with_interest").type(DoubleType.instance())
        .sql("CASE WHEN account_details.account_type = 'premium' THEN balance * 1.05 ELSE balance * 1.02 END"),
        
    // Aggregate transaction amounts
    field().name("total_transaction_amount").type(DoubleType.instance()) 
        .sql("aggregate(transactions.amount, 0.0, (acc, x) -> acc + x)")
    ```

=== "Scala"

    ```scala
    // Reference nested field value
    field.name("customer_name")
      .sql("customer_details.name"),
      
    // Calculate balance with interest based on account type  
    field.name("balance_with_interest").`type`(DoubleType)
      .sql("CASE WHEN account_details.account_type = 'premium' THEN balance * 1.05 ELSE balance * 1.02 END"),
      
    // Aggregate transaction amounts
    field.name("total_transaction_amount").`type`(DoubleType)
      .sql("aggregate(transactions.amount, 0.0, (acc, x) -> acc + x)")
    ```

=== "YAML"

    ```yaml
    fields:
      - name: "customer_name"
        options:
          sql: "customer_details.name"
      - name: "balance_with_interest"
        type: "double"
        options:
          sql: "CASE WHEN account_details.account_type = 'premium' THEN balance * 1.05 ELSE balance * 1.02 END"
      - name: "total_transaction_amount"
        type: "double" 
        options:
          sql: "aggregate(transactions.amount, 0.0, (acc, x) -> acc + x)"
    ```

### String Operations and Pattern Matching

Manipulate strings, extract patterns, and create formatted outputs.

=== "Java"

    ```java
    // Concatenate multiple fields with formatting
    field().name("account_display_name")
        .sql("CONCAT(customer_details.name, ' - ', account_type, ' (', account_id, ')')"),
        
    // Extract domain from email addresses
    field().name("email_domain")
        .sql("SUBSTRING_INDEX(customer_details.email, '@', -1)"),
        
    // Generate formatted account number
    field().name("formatted_account_number") 
        .sql("CONCAT('ACC-', LPAD(account_number, 8, '0'))")
    ```

=== "Scala"

    ```scala
    // Concatenate multiple fields with formatting  
    field.name("account_display_name")
      .sql("CONCAT(customer_details.name, ' - ', account_type, ' (', account_id, ')')"),
      
    // Extract domain from email addresses
    field.name("email_domain")
      .sql("SUBSTRING_INDEX(customer_details.email, '@', -1)"),
      
    // Generate formatted account number
    field.name("formatted_account_number")
      .sql("CONCAT('ACC-', LPAD(account_number, 8, '0'))")
    ```

=== "YAML"

    ```yaml
    fields:
      - name: "account_display_name"
        options:
          sql: "CONCAT(customer_details.name, ' - ', account_type, ' (', account_id, ')')"
      - name: "email_domain"
        options:
          sql: "SUBSTRING_INDEX(customer_details.email, '@', -1)"
      - name: "formatted_account_number"
        options:
          sql: "CONCAT('ACC-', LPAD(account_number, 8, '0'))"
    ```

### Date and Time Operations

Perform complex date calculations, extract date parts, and handle time zones.

=== "Java"

    ```java
    // Calculate age from birth date
    field().name("age").type(IntegerType.instance())
        .sql("DATEDIFF(CURRENT_DATE(), birth_date) / 365"),
        
    // Extract quarter from date
    field().name("quarter").type(IntegerType.instance())
        .sql("QUARTER(transaction_date)"),
        
    // Calculate business days between dates
    field().name("business_days_since_opening").type(IntegerType.instance())
        .sql("CASE WHEN DAYOFWEEK(open_date) IN (1, 7) THEN 0 ELSE DATEDIFF(CURRENT_DATE(), open_date) - (DATEDIFF(CURRENT_DATE(), open_date) / 7 * 2) END")
    ```

=== "Scala"

    ```scala
    // Calculate age from birth date
    field.name("age").`type`(IntegerType)
      .sql("DATEDIFF(CURRENT_DATE(), birth_date) / 365"),
      
    // Extract quarter from date
    field.name("quarter").`type`(IntegerType) 
      .sql("QUARTER(transaction_date)"),
      
    // Calculate business days between dates
    field.name("business_days_since_opening").`type`(IntegerType)
      .sql("CASE WHEN DAYOFWEEK(open_date) IN (1, 7) THEN 0 ELSE DATEDIFF(CURRENT_DATE(), open_date) - (DATEDIFF(CURRENT_DATE(), open_date) / 7 * 2) END")
    ```

=== "YAML"

    ```yaml
    fields:
      - name: "age"
        type: "integer"
        options:
          sql: "DATEDIFF(CURRENT_DATE(), birth_date) / 365"
      - name: "quarter"
        type: "integer"
        options:
          sql: "QUARTER(transaction_date)"
      - name: "business_days_since_opening"
        type: "integer"
        options:
          sql: "CASE WHEN DAYOFWEEK(open_date) IN (1, 7) THEN 0 ELSE DATEDIFF(CURRENT_DATE(), open_date) - (DATEDIFF(CURRENT_DATE(), open_date) / 7 * 2) END"
    ```

### Window Functions and Analytics

Use window functions for ranking, running totals, and analytical calculations.

=== "Java"

    ```java
    // Rank customers by balance within each region
    field().name("balance_rank").type(IntegerType.instance())
        .sql("ROW_NUMBER() OVER (PARTITION BY region ORDER BY balance DESC)"),
        
    // Calculate running total of transactions
    field().name("running_total").type(DoubleType.instance())
        .sql("SUM(transaction_amount) OVER (PARTITION BY account_id ORDER BY transaction_date ROWS UNBOUNDED PRECEDING)"),
        
    // Calculate percentage of total within group  
    field().name("balance_percentage").type(DoubleType.instance())
        .sql("(balance / SUM(balance) OVER (PARTITION BY account_type)) * 100")
    ```

=== "Scala"

    ```scala  
    // Rank customers by balance within each region
    field.name("balance_rank").`type`(IntegerType)
      .sql("ROW_NUMBER() OVER (PARTITION BY region ORDER BY balance DESC)"),
      
    // Calculate running total of transactions
    field.name("running_total").`type`(DoubleType)
      .sql("SUM(transaction_amount) OVER (PARTITION BY account_id ORDER BY transaction_date ROWS UNBOUNDED PRECEDING)"),
      
    // Calculate percentage of total within group
    field.name("balance_percentage").`type`(DoubleType) 
      .sql("(balance / SUM(balance) OVER (PARTITION BY account_type)) * 100")
    ```

=== "YAML"

    ```yaml
    fields:
      - name: "balance_rank"
        type: "integer"  
        options:
          sql: "ROW_NUMBER() OVER (PARTITION BY region ORDER BY balance DESC)"
      - name: "running_total"
        type: "double"
        options:
          sql: "SUM(transaction_amount) OVER (PARTITION BY account_id ORDER BY transaction_date ROWS UNBOUNDED PRECEDING)"
      - name: "balance_percentage"
        type: "double"
        options:
          sql: "(balance / SUM(balance) OVER (PARTITION BY account_type)) * 100"
    ```

### Date

| Option            | Default          | Example              | Description                                                          |
|-------------------|------------------|----------------------|----------------------------------------------------------------------|
| `min`             | now() - 365 days | `min: "2023-01-31"`  | Ensures that all generated values are greater than or equal to `min` |
| `max`             | now()            | `max: "2023-12-31"`  | Ensures that all generated values are less than or equal to `max`    |
| `enableNull`      | false            | `enableNull: "true"` | Enable/disable null values being generated                           |
| `nullProbability` | 0.0              | `nullProb: "0.1"`    | Probability to generate null values if `enableNull` is true          |

**Edge cases**: (0001-01-01, 1582-10-15, 1970-01-01, 9999-12-31)
([reference](https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/RandomDataGenerator.scala#L206))

#### Sample

=== "Java"

    ```java
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field().name("created_date").type(DateType.instance())
          .min(java.sql.Date.valueOf("2020-01-01"))
          .max(java.sql.Date.valueOf("2023-12-31")),
        field().name("birth_date").type(DateType.instance())
          .min(java.sql.Date.valueOf("1950-01-01"))
          .max(java.sql.Date.valueOf("2005-12-31"))
          .enableNull(true)
          .nullProbability(0.05),
        field().name("expiry_date").type(DateType.instance())
          .oneOf("2024-01-01", "2024-06-01", "2024-12-31"),
        field().name("random_date_with_edges").type(DateType.instance())
          .min(java.sql.Date.valueOf("2022-01-01"))
          .max(java.sql.Date.valueOf("2024-01-01"))
          .enableEdgeCase(true)
          .edgeCaseProbability(0.1),
        field().name("unique_event_date").type(DateType.instance())
          .min(java.sql.Date.valueOf("2023-01-01"))
          .max(java.sql.Date.valueOf("2023-12-31"))
          .isUnique(true),
        field().name("calculated_date").type(DateType.instance())
          .sql("CASE WHEN created_date < '2022-01-01' THEN '2022-01-01' ELSE created_date END")
      );
    ```

=== "Scala"

    ```scala
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field.name("created_date").`type`(DateType)
          .min(java.sql.Date.valueOf("2020-01-01"))
          .max(java.sql.Date.valueOf("2023-12-31")),
        field.name("birth_date").`type`(DateType)
          .min(java.sql.Date.valueOf("1950-01-01"))
          .max(java.sql.Date.valueOf("2005-12-31"))
          .enableNull(true)
          .nullProbability(0.05),
        field.name("expiry_date").`type`(DateType)
          .oneOf("2024-01-01", "2024-06-01", "2024-12-31"),
        field.name("random_date_with_edges").`type`(DateType)
          .min(java.sql.Date.valueOf("2022-01-01"))
          .max(java.sql.Date.valueOf("2024-01-01"))
          .enableEdgeCase(true)
          .edgeCaseProbability(0.1),
        field.name("unique_event_date").`type`(DateType)
          .min(java.sql.Date.valueOf("2023-01-01"))
          .max(java.sql.Date.valueOf("2023-12-31"))
          .isUnique(true),
        field.name("calculated_date").`type`(DateType)
          .sql("CASE WHEN created_date < '2022-01-01' THEN '2022-01-01' ELSE created_date END")
      )
    ```

=== "YAML"

    ```yaml
    name: "csv_file"
    steps:
      - name: "transactions"
        type: "csv"
        options:
          path: "app/src/test/resources/sample/csv/transactions"
        fields:
          - name: "created_date"
            type: "date"
            options:
              min: "2020-01-01"
              max: "2023-12-31"
          - name: "birth_date"
            type: "date"
            options:
              min: "1950-01-01"
              max: "2005-12-31"
              enableNull: true
              nullProb: 0.05
          - name: "expiry_date"
            type: "date"
            options:
              oneOf: ["2024-01-01", "2024-06-01", "2024-12-31"]
          - name: "random_date_with_edges"
            type: "date"
            options:
              min: "2022-01-01"
              max: "2024-01-01"
              enableEdgeCase: true
              edgeCaseProb: 0.1
          - name: "unique_event_date"
            type: "date"
            options:
              min: "2023-01-01"
              max: "2023-12-31"
              isUnique: true
          - name: "calculated_date"
            type: "date"
            options:
              sql: "CASE WHEN created_date < '2022-01-01' THEN '2022-01-01' ELSE created_date END"
    ```

### Timestamp

| Option            | Default          | Example                      | Description                                                          |
|-------------------|------------------|------------------------------|----------------------------------------------------------------------|
| `min`             | now() - 365 days | `min: "2023-01-31 23:10:10"` | Ensures that all generated values are greater than or equal to `min` |
| `max`             | now()            | `max: "2023-12-31 23:10:10"` | Ensures that all generated values are less than or equal to `max`    |
| `enableNull`      | false            | `enableNull: "true"`         | Enable/disable null values being generated                           |
| `nullProbability` | 0.0              | `nullProb: "0.1"`            | Probability to generate null values if `enableNull` is true          |

**Edge cases**: (0001-01-01 00:00:00, 1582-10-15 23:59:59, 1970-01-01 00:00:00, 9999-12-31 23:59:59)

#### Sample

=== "Java"

    ```java
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field().name("created_time").type(TimestampType.instance())
          .min(java.sql.Timestamp.valueOf("2020-01-01 00:00:00"))
          .max(java.sql.Timestamp.valueOf("2023-12-31 23:59:59")),
        field().name("last_login").type(TimestampType.instance())
          .min(java.sql.Timestamp.valueOf("2023-01-01 00:00:00"))
          .max(java.sql.Timestamp.valueOf("2023-12-31 23:59:59"))
          .enableNull(true)
          .nullProbability(0.2),
        field().name("scheduled_time").type(TimestampType.instance())
          .oneOf("2024-01-01 09:00:00", "2024-01-01 12:00:00", "2024-01-01 17:00:00"),
        field().name("event_timestamp").type(TimestampType.instance())
          .min(java.sql.Timestamp.valueOf("2023-06-01 00:00:00"))
          .max(java.sql.Timestamp.valueOf("2023-12-31 23:59:59"))
          .enableEdgeCase(true)
          .edgeCaseProbability(0.05),
        field().name("unique_activity_time").type(TimestampType.instance())
          .min(java.sql.Timestamp.valueOf("2023-01-01 00:00:00"))
          .max(java.sql.Timestamp.valueOf("2023-01-31 23:59:59"))
          .isUnique(true),
        field().name("calculated_timestamp").type(TimestampType.instance())
          .sql("CASE WHEN created_time < '2022-01-01 00:00:00' THEN '2022-01-01 00:00:00' ELSE created_time END")
      );
    ```

=== "Scala"

    ```scala
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field.name("created_time").`type`(TimestampType)
          .min(java.sql.Timestamp.valueOf("2020-01-01 00:00:00"))
          .max(java.sql.Timestamp.valueOf("2023-12-31 23:59:59")),
        field.name("last_login").`type`(TimestampType)
          .min(java.sql.Timestamp.valueOf("2023-01-01 00:00:00"))
          .max(java.sql.Timestamp.valueOf("2023-12-31 23:59:59"))
          .enableNull(true)
          .nullProbability(0.2),
        field.name("scheduled_time").`type`(TimestampType)
          .oneOf("2024-01-01 09:00:00", "2024-01-01 12:00:00", "2024-01-01 17:00:00"),
        field.name("event_timestamp").`type`(TimestampType)
          .min(java.sql.Timestamp.valueOf("2023-06-01 00:00:00"))
          .max(java.sql.Timestamp.valueOf("2023-12-31 23:59:59"))
          .enableEdgeCase(true)
          .edgeCaseProbability(0.05),
        field.name("unique_activity_time").`type`(TimestampType)
          .min(java.sql.Timestamp.valueOf("2023-01-01 00:00:00"))
          .max(java.sql.Timestamp.valueOf("2023-01-31 23:59:59"))
          .isUnique(true),
        field.name("calculated_timestamp").`type`(TimestampType)
          .sql("CASE WHEN created_time < '2022-01-01 00:00:00' THEN '2022-01-01 00:00:00' ELSE created_time END")
      )
    ```

=== "YAML"

    ```yaml
    name: "csv_file"
    steps:
      - name: "transactions"
        type: "csv"
        options:
          path: "app/src/test/resources/sample/csv/transactions"
        fields:
          - name: "created_time"
            type: "timestamp"
            options:
              min: "2020-01-01 00:00:00"
              max: "2023-12-31 23:59:59"
          - name: "last_login"
            type: "timestamp"
            options:
              min: "2023-01-01 00:00:00"
              max: "2023-12-31 23:59:59"
              enableNull: true
              nullProb: 0.2
          - name: "scheduled_time"
            type: "timestamp"
            options:
              oneOf: ["2024-01-01 09:00:00", "2024-01-01 12:00:00", "2024-01-01 17:00:00"]
          - name: "event_timestamp"
            type: "timestamp"
            options:
              min: "2023-06-01 00:00:00"
              max: "2023-12-31 23:59:59"
              enableEdgeCase: true
              edgeCaseProb: 0.05
          - name: "unique_activity_time"
            type: "timestamp"
            options:
              min: "2023-01-01 00:00:00"
              max: "2023-01-31 23:59:59"
              isUnique: true
          - name: "calculated_timestamp"
            type: "timestamp"
            options:
              sql: "CASE WHEN created_time < '2022-01-01 00:00:00' THEN '2022-01-01 00:00:00' ELSE created_time END"
    ```

### Binary

| Option            | Default | Example              | Description                                                             |
|-------------------|---------|----------------------|-------------------------------------------------------------------------|
| `minLen`          | 1       | `minLen: "2"`        | Ensures that all generated array of bytes have at least length `minLen` |
| `maxLen`          | 20      | `maxLen: "15"`       | Ensures that all generated array of bytes have at most length `maxLen`  |
| `enableNull`      | false   | `enableNull: "true"` | Enable/disable null values being generated                              |
| `nullProbability` | 0.0     | `nullProb: "0.1"`    | Probability to generate null values if `enableNull` is true             |

**Edge cases**: ("", "\n", "\r", "\t", " ", "\\u0000", "\\ufff", -128, 127)

#### Sample

=== "Java"

    ```java
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field().name("message_payload").type(BinaryType.instance())
          .minLength(10)
          .maxLength(100),
        field().name("encrypted_data").type(BinaryType.instance())
          .minLength(32)
          .maxLength(256)
          .enableNull(true)
          .nullProbability(0.1),
        field().name("signature").type(BinaryType.instance())
          .minLength(64)
          .maxLength(128)
          .enableEdgeCase(true)
          .edgeCaseProbability(0.05),
        field().name("unique_hash").type(BinaryType.instance())
          .minLength(32)
          .maxLength(32)
          .isUnique(true),
        field().name("calculated_checksum").type(BinaryType.instance())
          .sql("CASE WHEN LENGTH(message_payload) > 50 THEN UNHEX('DEADBEEF') ELSE UNHEX('CAFEBABE') END")
      );
    ```

=== "Scala"

    ```scala
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field.name("message_payload").`type`(BinaryType)
          .minLength(10)
          .maxLength(100),
        field.name("encrypted_data").`type`(BinaryType)
          .minLength(32)
          .maxLength(256)
          .enableNull(true)
          .nullProbability(0.1),
        field.name("signature").`type`(BinaryType)
          .minLength(64)
          .maxLength(128)
          .enableEdgeCase(true)
          .edgeCaseProbability(0.05),
        field.name("unique_hash").`type`(BinaryType)
          .minLength(32)
          .maxLength(32)
          .isUnique(true),
        field.name("calculated_checksum").`type`(BinaryType)
          .sql("CASE WHEN LENGTH(message_payload) > 50 THEN UNHEX('DEADBEEF') ELSE UNHEX('CAFEBABE') END")
      )
    ```

=== "YAML"

    ```yaml
    name: "csv_file"
    steps:
      - name: "transactions"
        type: "csv"
        options:
          path: "app/src/test/resources/sample/csv/transactions"
        fields:
          - name: "message_payload"
            type: "binary"
            options:
              minLen: 10
              maxLen: 100
          - name: "encrypted_data"
            type: "binary"
            options:
              minLen: 32
              maxLen: 256
              enableNull: true
              nullProb: 0.1
          - name: "signature"
            type: "binary"
            options:
              minLen: 64
              maxLen: 128
              enableEdgeCase: true
              edgeCaseProb: 0.05
          - name: "unique_hash"
            type: "binary"
            options:
              minLen: 32
              maxLen: 32
              isUnique: true
          - name: "calculated_checksum"
            type: "binary"
            options:
              sql: "CASE WHEN LENGTH(message_payload) > 50 THEN UNHEX('DEADBEEF') ELSE UNHEX('CAFEBABE') END"
    ```

### Array

| Option            | Default | Example               | Description                                                                                                              |
|-------------------|---------|-----------------------|--------------------------------------------------------------------------------------------------------------------------|
| `arrayMinLen`     | 0       | `arrayMinLen: "2"`    | Ensures that all generated arrays have at least length `arrayMinLen`                                                     |
| `arrayMaxLen`     | 5       | `arrayMaxLen: "15"`   | Ensures that all generated arrays have at most length `arrayMaxLen`                                                      |
| `arrayType`       | <empty> | `arrayType: "double"` | Inner data type of the array. Optional when using Java/Scala API. Allows for nested data types to be defined like struct |
| `enableNull`      | false   | `enableNull: "true"`  | Enable/disable null values being generated                                                                               |
| `nullProbability` | 0.0     | `nullProb: "0.1"`     | Probability to generate null values if `enableNull` is true                                                              |

#### Sample

=== "Java"

    ```java
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field().name("transaction_amounts").type(ArrayType.instance())
          .arrayType("double")
          .arrayMinLength(1)
          .arrayMaxLength(10),
        field().name("tags").type(ArrayType.instance())
          .arrayType("string")
          .arrayMinLength(0)
          .arrayMaxLength(5)
          .enableNull(true)
          .nullProbability(0.1),
        field().name("priority_scores").type(ArrayType.instance())
          .arrayType("integer")
          .arrayMinLength(3)
          .arrayMaxLength(3)
          .enableEdgeCase(true)
          .edgeCaseProbability(0.05),
        field().name("unique_identifiers").type(ArrayType.instance())
          .arrayType("string")
          .arrayMinLength(2)
          .arrayMaxLength(8)
          .isUnique(true),
        field().name("calculated_values").type(ArrayType.instance())
          .arrayType("double")
          .sql("CASE WHEN SIZE(transaction_amounts) > 3 THEN ARRAY(1.0, 2.0, 3.0) ELSE ARRAY(0.5, 1.0) END")
      );
    ```

=== "Scala"

    ```scala
    csv("transactions", "app/src/test/resources/sample/csv/transactions")
      .fields(
        field.name("transaction_amounts").`type`(ArrayType)
          .arrayType("double")
          .arrayMinLength(1)
          .arrayMaxLength(10),
        field.name("tags").`type`(ArrayType)
          .arrayType("string")
          .arrayMinLength(0)
          .arrayMaxLength(5)
          .enableNull(true)
          .nullProbability(0.1),
        field.name("priority_scores").`type`(ArrayType)
          .arrayType("integer")
          .arrayMinLength(3)
          .arrayMaxLength(3)
          .enableEdgeCase(true)
          .edgeCaseProbability(0.05),
        field.name("unique_identifiers").`type`(ArrayType)
          .arrayType("string")
          .arrayMinLength(2)
          .arrayMaxLength(8)
          .isUnique(true),
        field.name("calculated_values").`type`(ArrayType)
          .arrayType("double")
          .sql("CASE WHEN SIZE(transaction_amounts) > 3 THEN ARRAY(1.0, 2.0, 3.0) ELSE ARRAY(0.5, 1.0) END")
      )
    ```

=== "YAML"

    ```yaml
    name: "csv_file"
    steps:
      - name: "transactions"
        type: "csv"
        options:
          path: "app/src/test/resources/sample/csv/transactions"
        fields:
          - name: "transaction_amounts"
            type: "array<double>"
            options:
              arrayMinLen: 1
              arrayMaxLen: 10
          - name: "tags"
            type: "array<string>"
            options:
              arrayMinLen: 0
              arrayMaxLen: 5
              enableNull: true
              nullProb: 0.1
          - name: "priority_scores"
            type: "array<integer>"
            options:
              arrayMinLen: 3
              arrayMaxLen: 3
              enableEdgeCase: true
              edgeCaseProb: 0.05
          - name: "unique_identifiers"
            type: "array<string>"
            options:
              arrayMinLen: 2
              arrayMaxLen: 8
              isUnique: true
          - name: "calculated_values"
            type: "array<double>"
            options:
              sql: "CASE WHEN SIZE(transaction_amounts) > 3 THEN ARRAY(1.0, 2.0, 3.0) ELSE ARRAY(0.5, 1.0) END"
    ```
