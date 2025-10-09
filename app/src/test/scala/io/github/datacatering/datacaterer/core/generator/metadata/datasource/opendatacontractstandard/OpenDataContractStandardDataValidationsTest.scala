package io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard

import io.github.datacatering.datacaterer.core.generator.metadata.datasource.opendatacontractstandard.model.{DataQualityTypeEnum, LogicalTypeEnum, OpenDataContractStandardDataQualityV3, OpenDataContractStandardElementV3, OpenDataContractStandardSchemaV3}
import org.scalatest.funsuite.AnyFunSuite

class OpenDataContractStandardDataValidationsTest extends AnyFunSuite {

  test("Library null check validation") {
    val property = OpenDataContractStandardElementV3(
      name = "email",
      logicalType = LogicalTypeEnum.string,
      physicalType = "varchar(255)",
      quality = Some(Array(OpenDataContractStandardDataQualityV3(
        `type` = DataQualityTypeEnum.library,
        rule = Some("nullCheck"),
        description = Some("Email should not be null"),
        dimension = Some("completeness"),
        severity = Some("error")
      )))
    )

    val schema = OpenDataContractStandardSchemaV3(
      name = "users",
      properties = Some(Array(property))
    )

    val validations = OpenDataContractStandardDataValidations.getDataValidations(schema)

    assertResult(1)(validations.size)
    val validation = validations.head
    assert(validation.validation.description.contains("Email should not be null"))
  }

  test("Library unique check validation") {
    val property = OpenDataContractStandardElementV3(
      name = "user_id",
      logicalType = LogicalTypeEnum.integer,
      physicalType = "bigint",
      quality = Some(Array(OpenDataContractStandardDataQualityV3(
        `type` = DataQualityTypeEnum.library,
        rule = Some("uniqueCheck"),
        field = Some("user_id"),
        description = Some("User ID must be unique")
      )))
    )

    val schema = OpenDataContractStandardSchemaV3(
      name = "users",
      properties = Some(Array(property))
    )

    val validations = OpenDataContractStandardDataValidations.getDataValidations(schema)

    assertResult(1)(validations.size)
  }

  test("Library count check with between") {
    val schema = OpenDataContractStandardSchemaV3(
      name = "users",
      quality = Some(Array(OpenDataContractStandardDataQualityV3(
        `type` = DataQualityTypeEnum.library,
        rule = Some("countCheck"),
        description = Some("Row count should be between 100 and 1000"),
        mustBeBetween = Some(Array(100.0, 1000.0)),
        dimension = Some("completeness")
      )))
    )

    val validations = OpenDataContractStandardDataValidations.getDataValidations(schema)

    assertResult(1)(validations.size)
    val validation = validations.head
    assert(validation.validation.description.contains("Row count should be between 100 and 1000"))
  }

  test("SQL quality check") {
    val schema = OpenDataContractStandardSchemaV3(
      name = "orders",
      quality = Some(Array(OpenDataContractStandardDataQualityV3(
        `type` = DataQualityTypeEnum.sql,
        query = Some("SELECT COUNT(*) > 0 FROM orders WHERE status = 'pending'"),
        description = Some("Should have pending orders"),
        dimension = Some("consistency")
      )))
    )

    val validations = OpenDataContractStandardDataValidations.getDataValidations(schema)

    assertResult(1)(validations.size)
  }

  test("Between validation") {
    val property = OpenDataContractStandardElementV3(
      name = "age",
      logicalType = LogicalTypeEnum.integer,
      physicalType = "int",
      quality = Some(Array(OpenDataContractStandardDataQualityV3(
        `type` = DataQualityTypeEnum.library,
        rule = Some("betweenCheck"),
        field = Some("age"),
        mustBeBetween = Some(Array(18.0, 120.0)),
        description = Some("Age must be between 18 and 120")
      )))
    )

    val schema = OpenDataContractStandardSchemaV3(
      name = "users",
      properties = Some(Array(property))
    )

    val validations = OpenDataContractStandardDataValidations.getDataValidations(schema)

    assertResult(1)(validations.size)
  }

  test("Multiple validations") {
    val property1 = OpenDataContractStandardElementV3(
      name = "email",
      logicalType = LogicalTypeEnum.string,
      physicalType = "varchar(255)",
      quality = Some(Array(
        OpenDataContractStandardDataQualityV3(
          `type` = DataQualityTypeEnum.library,
          rule = Some("nullCheck"),
          description = Some("Email should not be null")
        ),
        OpenDataContractStandardDataQualityV3(
          `type` = DataQualityTypeEnum.library,
          rule = Some("matchesPattern"),
          mustBe = Some("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"),
          description = Some("Email must be valid format")
        )
      ))
    )

    val schema = OpenDataContractStandardSchemaV3(
      name = "users",
      properties = Some(Array(property1)),
      quality = Some(Array(OpenDataContractStandardDataQualityV3(
        `type` = DataQualityTypeEnum.library,
        rule = Some("countCheck"),
        mustBeGreaterThan = Some(0),
        description = Some("Table should have records")
      )))
    )

    val validations = OpenDataContractStandardDataValidations.getDataValidations(schema)

    assertResult(3)(validations.size) // 2 from property + 1 from schema
  }

  test("Warning threshold") {
    val property = OpenDataContractStandardElementV3(
      name = "optional_field",
      logicalType = LogicalTypeEnum.string,
      physicalType = "varchar(255)",
      quality = Some(Array(OpenDataContractStandardDataQualityV3(
        `type` = DataQualityTypeEnum.library,
        rule = Some("nullCheck"),
        description = Some("Optional field null check"),
        severity = Some("warning")
      )))
    )

    val schema = OpenDataContractStandardSchemaV3(
      name = "users",
      properties = Some(Array(property))
    )

    val validations = OpenDataContractStandardDataValidations.getDataValidations(schema)

    assertResult(1)(validations.size)
    val validation = validations.head
    assert(validation.validation.errorThreshold.isDefined, "Warning should have error threshold")
    assert(math.abs(validation.validation.errorThreshold.get - 0.05) < 0.001, "Warning threshold should be 5%")
  }
}
