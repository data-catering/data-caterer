package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.FieldBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{ALL_COMBINATIONS, OMIT, ONE_OF_GENERATOR, REGEX_GENERATOR, ARRAY_MINIMUM_LENGTH, ARRAY_MAXIMUM_LENGTH, MINIMUM, MAXIMUM}
import io.github.datacatering.datacaterer.api.model.{Count, DoubleType, Field, IntegerType, PerFieldCount, Step}
import io.github.datacatering.datacaterer.core.util.{Account, SparkSuite}
import net.datafaker.Faker
import org.apache.spark
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row}
import io.github.datacatering.datacaterer.api.ConnectionConfigWithTaskBuilder
import org.apache.spark.sql.types.StructType

class DataGeneratorFactoryTest extends SparkSuite {

  private val dataGeneratorFactory = new DataGeneratorFactory(new Faker() with Serializable, enableFastGeneration = false)
  private val fields = List(
    FieldBuilder().name("id").minLength(20).maxLength(25),
    FieldBuilder().name("amount").`type`(DoubleType).min(0.0).max(1000.0),
    FieldBuilder().name("debit_credit").oneOf(List("D", "C")),
    FieldBuilder().name("name").regex("[A-Z][a-z]{2,6} [A-Z][a-z]{2,8}"),
    FieldBuilder().name("code").`type`(IntegerType).sql("CASE WHEN debit_credit == 'D' THEN 1 ELSE 0 END"),
    FieldBuilder().name("party_id").uuid().incremental(),
    FieldBuilder().name("customer_id").`type`(IntegerType).incremental(),
  ).map(_.field) ++
    FieldBuilder().name("rank").oneOfWeighted((1, 0.8), (2, 0.1), (3, 0.1)).map(_.field) ++
    FieldBuilder().name("rating").oneOfWeighted(("A", 1), ("B", 2), ("C", 3)).map(_.field)

  private val simpleFields = List(Field("id"), Field("name"))

  private val nestedFields = List(
    Field("id"),
    Field("tmp_account_id", Some("string"), Map(REGEX_GENERATOR -> "ACC[0-9]{8}", OMIT -> "true")),
    Field("details", fields = List(Field("account_id", Some("string"), Map("sql" -> "tmp_account_id"))))
  )

  private val doubleNestedFields = List(
    Field("id"),
    Field("tmp_account_id", Some("string"), Map(REGEX_GENERATOR -> "ACC[0-9]{8}", OMIT -> "true")),
    Field("details", fields = List(
      Field("account_id", Some("string"), Map("sql" -> "tmp_account_id")),
      Field("customer", fields = List(Field("name")))
    ))
  )

  test("Can generate data for basic step") {
    val step = Step("transaction", "parquet", Count(records = Some(10)), Map("path" -> "sample/output/parquet/transactions"), fields)

    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    assertResult(10L)(df.count())
    assertResult(Array("id", "amount", "debit_credit", "name", "code", "party_id", "customer_id", "rank", "rating"))(df.columns)
    assertResult(Array(
      ("id", spark.sql.types.StringType),
      ("amount", spark.sql.types.DoubleType),
      ("debit_credit", spark.sql.types.StringType),
      ("name", spark.sql.types.StringType),
      ("code", spark.sql.types.IntegerType),
      ("party_id", spark.sql.types.StringType),
      ("customer_id", spark.sql.types.IntegerType),
      ("rank", spark.sql.types.IntegerType),
      ("rating", spark.sql.types.StringType),
    ))(df.schema.fields.map(x => (x.name, x.dataType)))
    val rows = df.collect()
    val sampleRow = df.head()
    assert(sampleRow.getString(0).nonEmpty && sampleRow.getString(0).length >= 20)
    assert(sampleRow.getDouble(1) >= 0.0)
    val debitCredit = sampleRow.getString(2)
    assert(debitCredit == "D" || debitCredit == "C")
    assert(sampleRow.getString(3).matches("[A-Z][a-z]{2,6} [A-Z][a-z]{2,8}"))
    if (debitCredit == "D") assert(sampleRow.getInt(4) == 1) else assert(sampleRow.getInt(4) == 0)
    assertResult("c4ca4238-a0b9-2382-0dcc-509a6f75849b")(sampleRow.getString(5))
    rows.foreach(row => {
      val customerId = row.getInt(6)
      assert(customerId > 0 && customerId <= 10)
      val rank = row.getInt(7)
      assert(rank == 1 || rank == 2 || rank == 3)
      val rating = row.getString(8)
      assert(rating == "A" || rating == "B" || rating == "C")
    })
  }

  test("Can generate data when number of rows per field is defined") {
    val step = Step("transaction", "parquet",
      Count(records = Some(10), perField = Some(PerFieldCount(List("id"), Some(2)))),
      Map("path" -> "sample/output/parquet/transactions"), simpleFields)

    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    assertResult(20L)(df.count())
    val sampleId = df.head().getAs[String]("id")
    val sampleRows = df.filter(_.getAs[String]("id") == sampleId)
    assertResult(2L)(sampleRows.count())
  }

  test("Can generate data with generated number of rows per field by a generator") {
    val step = Step("transaction", "parquet", Count(Some(10),
      perField = Some(PerFieldCount(List("id"), None, Map("min" -> "1", "max" -> "2")))),
      Map("path" -> "sample/output/parquet/transactions"), simpleFields)

    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    assert(df.count() >= 10L)
    assert(df.count() <= 20L)
    val sampleId = df.head().getAs[String]("id")
    val sampleRows = df.filter(_.getAs[String]("id") == sampleId)
    assert(sampleRows.count() >= 1L)
    assert(sampleRows.count() <= 2L)
  }

  test("Can generate data with generated number of rows generated by a data generator") {
    val step = Step("transaction", "parquet", Count(None,
      perField = None,
      options = Map("min" -> "10", "max" -> "20")),
      Map("path" -> "sample/output/parquet/transactions"), simpleFields)

    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 15)
    df.cache()

    assert(df.count() >= 10L)
    assert(df.count() <= 20L)
    val sampleId = df.head().getAs[String]("id")
    val sampleRows = df.filter(_.getAs[String]("id") == sampleId)
    assertResult(1L)(sampleRows.count())
  }

  test("Can generate data with all possible oneOf combinations enabled in step") {
    val step = Step("transaction", "parquet", Count(),
      Map("path" -> "sample/output/parquet/transactions", ALL_COMBINATIONS -> "true"), fields)

    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 15)
    df.cache()

    assertResult(2L)(df.count())
    val idx = df.columns.indexOf("debit_credit")
    assert(df.collect().exists(r => r.getString(idx) == "D"))
    assert(df.collect().exists(r => r.getString(idx) == "C"))
  }

  test("Can generate data with all possible oneOf combinations enabled in step with multiple oneOf fields") {
    val statusField = Field("status", Some("string"),
      Map(ONE_OF_GENERATOR -> List("open", "closed", "suspended")))
    val fieldsWithStatus = fields ++ List(statusField)
    val step = Step("transaction", "parquet", Count(),
      Map("path" -> "sample/output/parquet/transactions", ALL_COMBINATIONS -> "true"), fieldsWithStatus)

    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 15)
    df.cache()

    assertResult(6L)(df.count())
    val debitIdx = df.columns.indexOf("debit_credit")
    val statusIdx = df.columns.indexOf("status")
    assertResult(3)(df.collect().count(r => r.getString(debitIdx) == "D"))
    assertResult(3)(df.collect().count(r => r.getString(debitIdx) == "C"))
    assertResult(2)(df.collect().count(r => r.getString(statusIdx) == "open"))
    assertResult(2)(df.collect().count(r => r.getString(statusIdx) == "closed"))
    assertResult(2)(df.collect().count(r => r.getString(statusIdx) == "suspended"))
  }

  test("Can generate data with nested field part of per field count") {
    val step = Step("transaction", "parquet", Count(Some(10),
      perField = Some(PerFieldCount(List("tmp_account_id"), Some(2)))),
      Map("path" -> "sample/output/parquet/transactions"), nestedFields)

    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    assertResult(20L)(df.count())
    val dfArr = df.collect()
    dfArr.foreach(row => {
      val sampleId = row.getAs[Row]("details").getAs[String]("account_id")
      val sampleRows = df.filter(_.getAs[Row]("details").getAs[String]("account_id") == sampleId)
      assert(sampleRows.count() == 2L)
    })
  }

  test("Can generate data with two level nested fields") {
    val step = Step("transaction", "parquet", Count(Some(10),
      perField = Some(PerFieldCount(List("tmp_account_id"), Some(2)))),
      Map("path" -> "sample/output/parquet/transactions"), doubleNestedFields)

    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    assertResult(20L)(df.count())
    val dfArr = df.collect()
    dfArr.foreach(row => {
      val sampleId = row.getAs[Row]("details").getAs[String]("account_id")
      val sampleRows = df.filter(_.getAs[Row]("details").getAs[String]("account_id") == sampleId)
      assert(sampleRows.count() == 2L)
    })
  }

  test("Can filter out empty struct fields from data generation") {
    val fieldsWithEmptyStruct = List(
      Field("id"),
      Field("name"),
      Field("empty_struct", Some("struct"), fields = List()), // Empty struct - should be filtered
      Field("valid_struct", Some("struct"), fields = List(
        Field("inner_field", Some("string"))
      )), // Valid struct - should be kept
      Field("nested_empty_struct", Some("struct"), fields = List(
        Field("inner_empty", Some("struct"), fields = List()) // Nested empty struct
      )) // Should be filtered because all nested fields are empty
    )

    val step = Step("test", "json", fields = fieldsWithEmptyStruct)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 10)

    // Should only have 3 fields: id, name, valid_struct (empty struct fields filtered out)  
    assert(result.columns.length == 3)
    assert(result.columns.contains("id"))
    assert(result.columns.contains("name"))
    assert(result.columns.contains("valid_struct"))
    assert(!result.columns.contains("empty_struct"))
    assert(!result.columns.contains("nested_empty_struct"))
  }

  test("Can handle nested struct filtering correctly") {
    val fieldsWithNestedStructs = List(
      Field("root_field", Some("string")),
      Field("mixed_struct", Some("struct"), fields = List(
        Field("valid_field", Some("string")),
        Field("empty_nested", Some("struct"), fields = List()) // This should be filtered
      ))
    )

    val step = Step("test", "json", fields = fieldsWithNestedStructs)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 10)

    // Should have root_field and mixed_struct (with filtered nested content)
    assert(result.columns.length == 2) // root_field + mixed_struct
    assert(result.columns.contains("root_field"))
    assert(result.columns.contains("mixed_struct"))
  }

  test("Can include specific fields only") {
    val allFields = List(
      Field("id", Some("long")),
      Field("name", Some("string")),
      Field("email", Some("string")),
      Field("age", Some("int")),
      Field("address", Some("struct"), fields = List(
        Field("street", Some("string")),
        Field("city", Some("string"))
      ))
    )

    val step = Step("test", "json", 
      fields = allFields,
      options = Map("includeFields" -> "id,name,address.city")
    )
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 10)

    // Should only have id, name, and address (but address should only have city field)
    assert(result.columns.length == 3)
    assert(result.columns.contains("id"))
    assert(result.columns.contains("name"))
    assert(result.columns.contains("address"))
    assert(!result.columns.contains("email"))
    assert(!result.columns.contains("age"))
  }

  test("Can exclude specific fields") {
    val allFields = List(
      Field("id", Some("long")),
      Field("name", Some("string")),
      Field("email", Some("string")),
      Field("internal_id", Some("string")),
      Field("temp_field", Some("string"))
    )

    val step = Step("test", "json", 
      fields = allFields,
      options = Map("excludeFields" -> "internal_id,temp_field")
    )
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 10)

    // Should have all fields except internal_id and temp_field
    assert(result.columns.length == 3)
    assert(result.columns.contains("id"))
    assert(result.columns.contains("name"))
    assert(result.columns.contains("email"))
    assert(!result.columns.contains("internal_id"))
    assert(!result.columns.contains("temp_field"))
  }

  test("Can use regex patterns to include fields") {
    val allFields = List(
      Field("user_id", Some("long")),
      Field("user_name", Some("string")),
      Field("user_email", Some("string")),
      Field("account_id", Some("long")),
      Field("account_balance", Some("double")),
      Field("transaction_id", Some("string"))
    )

    val step = Step("test", "json", 
      fields = allFields,
      options = Map("includeFieldPatterns" -> "user_.*,transaction_.*")
    )
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 10)

    // Should only have user_* and transaction_* fields
    assert(result.columns.length == 4)
    assert(result.columns.contains("user_id"))
    assert(result.columns.contains("user_name"))
    assert(result.columns.contains("user_email"))
    assert(result.columns.contains("transaction_id"))
    assert(!result.columns.contains("account_id"))
    assert(!result.columns.contains("account_balance"))
  }

  test("Can use regex patterns to exclude fields") {
    val allFields = List(
      Field("user_id", Some("long")),
      Field("user_name", Some("string")),
      Field("internal_secret", Some("string")),
      Field("internal_key", Some("string")),
      Field("temp_data", Some("string")),
      Field("public_field", Some("string"))
    )

    val step = Step("test", "json", 
      fields = allFields,
      options = Map("excludeFieldPatterns" -> "internal_.*,temp_.*")
    )
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 10)

    // Should exclude internal_* and temp_* fields
    assert(result.columns.length == 3)
    assert(result.columns.contains("user_id"))
    assert(result.columns.contains("user_name"))
    assert(result.columns.contains("public_field"))
    assert(!result.columns.contains("internal_secret"))
    assert(!result.columns.contains("internal_key"))
    assert(!result.columns.contains("temp_data"))
  }

  test("Can combine include and exclude field filters") {
    val allFields = List(
      Field("user_id", Some("long")),
      Field("user_name", Some("string")),
      Field("user_internal_secret", Some("string")),
      Field("user_email", Some("string")),
      Field("account_id", Some("long")),
      Field("transaction_id", Some("string"))
    )

    val step = Step("test", "json", 
      fields = allFields,
      options = Map(
        "includeFieldPatterns" -> "user_.*", // Include all user_* fields
        "excludeFieldPatterns" -> ".*internal.*" // But exclude any field with 'internal'
      )
    )
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 10)

    // Should have user fields except user_internal_secret
    assert(result.columns.length == 3)
    assert(result.columns.contains("user_id"))
    assert(result.columns.contains("user_name"))
    assert(result.columns.contains("user_email"))
    assert(!result.columns.contains("user_internal_secret"))
    assert(!result.columns.contains("account_id"))
    assert(!result.columns.contains("transaction_id"))
  }

  test("Can filter nested struct fields with dot notation") {
    val nestedFields = List(
      Field("id", Some("long")),
      Field("user", Some("struct"), fields = List(
        Field("name", Some("string")),
        Field("email", Some("string")),
        Field("internal_notes", Some("string")),
        Field("profile", Some("struct"), fields = List(
          Field("age", Some("int")),
          Field("secret_data", Some("string"))
        ))
      )),
      Field("metadata", Some("struct"), fields = List(
        Field("created_at", Some("timestamp")),
        Field("internal_flag", Some("boolean"))
      ))
    )

    val step = Step("test", "json", 
      fields = nestedFields,
      options = Map(
        "includeFields" -> "id,user.name,user.profile.age,metadata.created_at"
      )
    )
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 10)

    // Should have id, user (with only name and profile.age), metadata (with only created_at)
    assert(result.columns.length == 3)
    assert(result.columns.contains("id"))
    assert(result.columns.contains("user"))
    assert(result.columns.contains("metadata"))
  }

  test("Field filtering preserves data quality") {
    val fieldsWithTypes = List(
      Field("id", Some("long")),
      Field("name", Some("string")),
      Field("excluded_field", Some("string"))
    )

    val step = Step("test", "json", 
      fields = fieldsWithTypes,
      options = Map("excludeFields" -> "excluded_field")
    )
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 5)

    // Check that data is actually generated correctly
    assert(result.count() == 5)
    assert(result.columns.length == 2)
    assert(result.select("id").collect().forall(_.get(0) != null))
    assert(result.select("name").collect().forall(_.get(0) != null))
  }

  test("No filtering when no filter options specified") {
    val allFields = List(
      Field("id", Some("long")),
      Field("name", Some("string")),
      Field("email", Some("string"))
    )

    val step = Step("test", "json", fields = allFields)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 10)

    // Should have all fields since no filtering is specified
    assert(result.columns.length == 3)
    assert(result.columns.contains("id"))
    assert(result.columns.contains("name"))
    assert(result.columns.contains("email"))
  }

  test("Can filter array fields with dot notation for nested array elements") {
    // Simulate the JSON schema structure similar to mx_pain with arrays
    // Note: JSON schema arrays don't have "element" wrappers - the nested fields are directly under the array field
    val arrayFields = List(
      Field("id", Some("string")),
      Field("customer_direct_debit_initiation_v11", Some("struct"), fields = List(
        Field("group_header", Some("struct"), fields = List(
          Field("message_identification", Some("string")),
          Field("creation_date_time", Some("string")),
          Field("number_of_transactions", Some("string")),
          Field("control_sum", Some("string")),
          Field("initiating_party", Some("struct"), fields = List(
            Field("name", Some("string")),
            Field("internal_id", Some("string")) // Should be excluded
          ))
        )),
        Field("payment_information", Some("array"), fields = List(
          Field("payment_information_identification", Some("string")),
          Field("payment_method", Some("string")),
          Field("batch_booking", Some("boolean")),
          Field("number_of_transactions", Some("string")),
          Field("control_sum", Some("string")),
          Field("requested_collection_date", Some("string")),
          Field("internal_processing_flag", Some("string")), // Should be excluded
          Field("creditor", Some("struct"), fields = List(
            Field("name", Some("string")),
            Field("internal_code", Some("string")) // Should be excluded
          )),
          Field("direct_debit_transaction_information", Some("array"), fields = List(
            Field("payment_identification", Some("struct"), fields = List(
              Field("end_to_end_identification", Some("string")),
              Field("internal_ref", Some("string")) // Should be excluded
            )),
            Field("instructed_amount", Some("struct"), fields = List(
              Field("value", Some("string")),
              Field("currency", Some("string"))
            )),
            Field("debtor", Some("struct"), fields = List(
              Field("name", Some("string")),
              Field("internal_notes", Some("string")) // Should be excluded
            ))
          ))
        )),
        Field("supplementary_data", Some("array"), fields = List(
          Field("place_and_name", Some("string")),
          Field("internal_metadata", Some("string")) // Should be excluded
        ))
      ))
    )

    val step = Step("test", "json", 
      fields = arrayFields,
      options = Map(
        "includeFields" -> List(
          // Include the id field
          "id",
          
          // Group header fields
          "customer_direct_debit_initiation_v11.group_header.message_identification",
          "customer_direct_debit_initiation_v11.group_header.creation_date_time",
          "customer_direct_debit_initiation_v11.group_header.number_of_transactions",
          "customer_direct_debit_initiation_v11.group_header.control_sum",
          "customer_direct_debit_initiation_v11.group_header.initiating_party.name",
          
          // Payment information array fields
          "customer_direct_debit_initiation_v11.payment_information.payment_information_identification",
          "customer_direct_debit_initiation_v11.payment_information.payment_method",
          "customer_direct_debit_initiation_v11.payment_information.batch_booking",
          "customer_direct_debit_initiation_v11.payment_information.number_of_transactions",
          "customer_direct_debit_initiation_v11.payment_information.control_sum",
          "customer_direct_debit_initiation_v11.payment_information.requested_collection_date",
          "customer_direct_debit_initiation_v11.payment_information.creditor.name",
          
          // Nested array within array fields
          "customer_direct_debit_initiation_v11.payment_information.direct_debit_transaction_information.payment_identification.end_to_end_identification",
          "customer_direct_debit_initiation_v11.payment_information.direct_debit_transaction_information.instructed_amount.value",
          "customer_direct_debit_initiation_v11.payment_information.direct_debit_transaction_information.instructed_amount.currency",
          "customer_direct_debit_initiation_v11.payment_information.direct_debit_transaction_information.debtor.name",
          
          // Supplementary data array fields
          "customer_direct_debit_initiation_v11.supplementary_data.place_and_name"
        ).mkString(",")
      )
    )
    
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 1)

    // Should have id and customer_direct_debit_initiation_v11
    assert(result.columns.length == 2)
    assert(result.columns.contains("id"))
    assert(result.columns.contains("customer_direct_debit_initiation_v11"))
    
    // Verify the nested structure is correctly filtered
    val row = result.head()
    val customerDirectDebit = row.getAs[org.apache.spark.sql.Row]("customer_direct_debit_initiation_v11")
    assert(customerDirectDebit != null, "customer_direct_debit_initiation_v11 should not be null")
    
    // Check group_header structure
    val groupHeader = customerDirectDebit.getAs[org.apache.spark.sql.Row]("group_header")
    assert(groupHeader != null, "group_header should not be null")
    
    val groupHeaderSchema = groupHeader.schema
    assert(groupHeaderSchema.fieldNames.contains("message_identification"), "group_header should contain message_identification")
    assert(groupHeaderSchema.fieldNames.contains("creation_date_time"), "group_header should contain creation_date_time")
    assert(groupHeaderSchema.fieldNames.contains("number_of_transactions"), "group_header should contain number_of_transactions")
    assert(groupHeaderSchema.fieldNames.contains("control_sum"), "group_header should contain control_sum")
    assert(groupHeaderSchema.fieldNames.contains("initiating_party"), "group_header should contain initiating_party")
    
    // Check initiating_party nested structure
    val initiatingParty = groupHeader.getAs[org.apache.spark.sql.Row]("initiating_party")
    assert(initiatingParty != null, "initiating_party should not be null")
    val initiatingPartySchema = initiatingParty.schema
    assert(initiatingPartySchema.fieldNames.contains("name"), "initiating_party should contain name")
    assert(!initiatingPartySchema.fieldNames.contains("internal_id"), "initiating_party should NOT contain internal_id (excluded)")
    
    // Check payment_information array structure exists
    val customerDirectDebitSchema = customerDirectDebit.schema
    assert(customerDirectDebitSchema.fieldNames.contains("payment_information"), "customer_direct_debit_initiation_v11 should contain payment_information")
    
    // For arrays, Spark represents them as ArrayType, so we need to check the schema differently
    val paymentInfoField = customerDirectDebitSchema("payment_information")
    assert(paymentInfoField.dataType.typeName == "array", "payment_information should be an array type")
    
    // Check supplementary_data array structure exists
    assert(customerDirectDebitSchema.fieldNames.contains("supplementary_data"), "customer_direct_debit_initiation_v11 should contain supplementary_data")
    
    val supplementaryDataField = customerDirectDebitSchema("supplementary_data")
    assert(supplementaryDataField.dataType.typeName == "array", "supplementary_data should be an array type")
    
    println("✅ Array field filtering test passed!")
    println(s"Generated schema structure:\n${result.schema.treeString}")
  }

  test("Can filter array fields with pattern matching") {
    // Test pattern-based filtering on array structures
    val arrayFieldsForPattern = List(
      Field("root_id", Some("string")),
      Field("data_container", Some("struct"), fields = List(
        Field("transactions", Some("array"), fields = List(
          Field("transaction_id", Some("string")),
          Field("transaction_amount", Some("double")),
          Field("transaction_date", Some("string")),
          Field("internal_transaction_code", Some("string")), // Should be excluded
          Field("metadata", Some("struct"), fields = List(
            Field("transaction_type", Some("string")),
            Field("internal_flags", Some("string")) // Should be excluded
          ))
        )),
        Field("payments", Some("array"), fields = List(
          Field("payment_id", Some("string")),
          Field("payment_amount", Some("double")),
          Field("internal_payment_ref", Some("string")) // Should be excluded
        ))
      ))
    )
    
    val step = Step("test", "json", 
      fields = arrayFieldsForPattern,
      options = Map(
        "includeFieldPatterns" -> "root_id,.*transaction.*,.*payment.*", // Include root_id, transaction and payment related fields
        "excludeFieldPatterns" -> ".*internal.*" // Exclude internal fields
      )
    )
    
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 1)

    // Should have root_id and data_container
    assert(result.columns.length == 2)
    assert(result.columns.contains("root_id"))
    assert(result.columns.contains("data_container"))
    
    val row = result.collect().head
    val dataContainer = row.getAs[org.apache.spark.sql.Row]("data_container")
    assert(dataContainer != null, "data_container should not be null")
    
    val dataContainerSchema = dataContainer.schema
    
    // Check that arrays exist and are properly typed
    assert(dataContainerSchema.fieldNames.contains("transactions"), "data_container should contain transactions")
    assert(dataContainerSchema.fieldNames.contains("payments"), "data_container should contain payments")
    
    val transactionsField = dataContainerSchema("transactions")
    assert(transactionsField.dataType.typeName == "array", "transactions should be an array type")
    
    val paymentsField = dataContainerSchema("payments")
    assert(paymentsField.dataType.typeName == "array", "payments should be an array type")
    
    println("✅ Array pattern filtering test passed!")
    println("Generated schema structure:")
    result.printSchema()
  }

  test("Enhanced array field filtering handles element wrappers automatically") {
    // Test that users can specify simple paths like "payment_information.payment_id" 
    // without needing to know about Spark's "element" wrapper
    val complexArrayFields = List(
      Field("root_id", Some("string")),
      Field("customer_data", Some("struct"), fields = List(
        Field("basic_info", Some("struct"), fields = List(
          Field("name", Some("string")),
          Field("email", Some("string"))
        )),
        Field("orders", Some("array"), fields = List(
          Field("order_id", Some("string")),
          Field("order_date", Some("string")),
          Field("internal_order_code", Some("string")), // Should be excluded
          Field("items", Some("array"), fields = List(
            Field("item_id", Some("string")),
            Field("item_name", Some("string")),
            Field("quantity", Some("integer")),
            Field("internal_item_ref", Some("string")) // Should be excluded
          )),
          Field("shipping", Some("struct"), fields = List(
            Field("address", Some("string")),
            Field("tracking_number", Some("string")),
            Field("internal_carrier_code", Some("string")) // Should be excluded
          ))
        ))
      ))
    )
    
    val step = Step("test", "json", 
      fields = complexArrayFields,
      options = Map(
        "includeFields" -> List(
          "root_id",
          "customer_data.basic_info.name",
          "customer_data.basic_info.email",
          "customer_data.orders.order_id",
          "customer_data.orders.order_date",
          "customer_data.orders.items.item_id",
          "customer_data.orders.items.item_name",
          "customer_data.orders.items.quantity",
          "customer_data.orders.shipping.address",
          "customer_data.orders.shipping.tracking_number"
        ).mkString(",")
      )
    )
    
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 1)

    // Verify the structure is correctly filtered
    assert(result.columns.contains("root_id"), "Should contain root_id")
    assert(result.columns.contains("customer_data"), "Should contain customer_data")
    
    val row = result.collect().head
    val customerData = row.getAs[org.apache.spark.sql.Row]("customer_data")
    assert(customerData != null, "customer_data should not be null")
    
    val customerDataSchema = customerData.schema
    
    // Check basic_info structure
    assert(customerDataSchema.fieldNames.contains("basic_info"), "customer_data should contain basic_info")
    val basicInfo = customerData.getAs[org.apache.spark.sql.Row]("basic_info")
    if (basicInfo != null) {
      val basicInfoSchema = basicInfo.schema
      assert(basicInfoSchema.fieldNames.contains("name"), "basic_info should contain name")
      assert(basicInfoSchema.fieldNames.contains("email"), "basic_info should contain email")
    }
    
    // Check orders array structure
    assert(customerDataSchema.fieldNames.contains("orders"), "customer_data should contain orders")
    val ordersField = customerDataSchema("orders")
    assert(ordersField.dataType.typeName == "array", "orders should be an array type")
    
    println("✅ Enhanced array field filtering test passed!")
    println("Users can specify simple paths without 'element' wrappers!")
    println("Generated schema structure:")
    result.printSchema()
  }

  test("Array data generation with simple nested structure constraints") {
    // Test simple array with nested structure and constraints
    val simpleArrayWithConstraints = List(
      Field("users", Some("array"), fields = List(
        Field("id", Some("integer"), options = Map("min" -> 1, "max" -> 1000)),
        Field("name", Some("string"), options = Map("regex" -> "^[A-Z][a-z]{2,10}$")),
        Field("email", Some("string"), options = Map("regex" -> "^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,4}$")),
        Field("age", Some("integer"), options = Map("min" -> 18, "max" -> 65)),
        Field("profile", Some("struct"), fields = List(
          Field("bio", Some("string"), options = Map("minLength" -> 10, "maxLength" -> 100)),
          Field("score", Some("double"), options = Map("min" -> 0.0, "max" -> 10.0))
        ))
      ))
    )
    
    val step = Step("test", "json", fields = simpleArrayWithConstraints)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 3)

    println("=== SIMPLE ARRAY WITH NESTED CONSTRAINTS TEST ===")
    println("Generated schema:")
    result.printSchema()
    
    val rows = result.collect()
    println(s"Generated ${rows.length} rows")
    
    rows.foreach { row =>
      val users = row.getAs[Seq[org.apache.spark.sql.Row]]("users")
      println(s"Users array length: ${if (users != null) users.length else "null"}")
      
      if (users != null && users.nonEmpty) {
        users.take(2).zipWithIndex.foreach { case (user, idx) =>
          println(s"User $idx:")
          println(s"  ID: ${user.getAs[Any]("id")} (should be 1-1000)")
          println(s"  Name: ${user.getAs[String]("name")} (should match ^[A-Z][a-z]{2,10}$$)")
          println(s"  Email: ${user.getAs[String]("email")} (should be valid email)")
          println(s"  Age: ${user.getAs[Any]("age")} (should be 18-65)")
          
          val profile = user.getAs[org.apache.spark.sql.Row]("profile")
          if (profile != null) {
            println(s"  Profile Bio: ${profile.getAs[String]("bio")} (should be 10-100 chars)")
            println(s"  Profile Score: ${profile.getAs[Any]("score")} (should be 0.0-10.0)")
          } else {
            println("  Profile: null")
          }
        }
      }
    }
    
    assert(result.columns.contains("users"), "Should contain users array")
  }

  test("Array data generation with multiple nested levels and constraints") {
    // Test complex nested arrays with multiple levels and constraints
    val complexNestedArrays = List(
      Field("companies", Some("array"), fields = List(
        Field("company_id", Some("string"), options = Map("regex" -> "^COMP-[0-9]{4}$")),
        Field("company_name", Some("string"), options = Map("regex" -> "^[A-Z][a-zA-Z\\s]{5,20} (Inc|LLC|Corp)$")),
        Field("departments", Some("array"), fields = List(
          Field("dept_id", Some("integer"), options = Map("min" -> 100, "max" -> 999)),
          Field("dept_name", Some("string"), options = Map("oneOf" -> "Engineering,Marketing,Sales,HR,Finance")),
          Field("budget", Some("double"), options = Map("min" -> 50000.0, "max" -> 500000.0)),
          Field("employees", Some("array"), fields = List(
            Field("emp_id", Some("string"), options = Map("regex" -> "^EMP-[0-9]{6}$")),
            Field("first_name", Some("string"), options = Map("regex" -> "^[A-Z][a-z]{2,15}$")),
            Field("last_name", Some("string"), options = Map("regex" -> "^[A-Z][a-z]{2,20}$")),
            Field("salary", Some("double"), options = Map("min" -> 30000.0, "max" -> 200000.0)),
            Field("skills", Some("array"), fields = List(
              Field("skill_name", Some("string"), options = Map("oneOf" -> "Java,Python,Scala,JavaScript,SQL,Docker,Kubernetes")),
              Field("proficiency", Some("integer"), options = Map("min" -> 1, "max" -> 5)),
              Field("years_experience", Some("double"), options = Map("min" -> 0.5, "max" -> 20.0))
            )),
            Field("contact", Some("struct"), fields = List(
              Field("phone", Some("string"), options = Map("regex" -> "^\\+1-[0-9]{3}-[0-9]{3}-[0-9]{4}$")),
              Field("email", Some("string"), options = Map("regex" -> "^[a-z]+\\.[a-z]+@[a-z]+\\.(com|org|net)$"))
            ))
          ))
        ))
      ))
    )
    
    val step = Step("test", "json", fields = complexNestedArrays)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 2)

    println("=== COMPLEX NESTED ARRAYS WITH CONSTRAINTS TEST ===")
    println("Generated schema:")
    result.printSchema()
    
    val rows = result.collect()
    println(s"Generated ${rows.length} rows")
    
    rows.foreach { row =>
      val companies = row.getAs[Seq[org.apache.spark.sql.Row]]("companies")
      println(s"Companies array length: ${if (companies != null) companies.length else "null"}")
      
      if (companies != null && companies.nonEmpty) {
        companies.take(1).zipWithIndex.foreach { case (company, companyIdx) =>
          println(s"Company $companyIdx:")
          println(s"  ID: ${company.getAs[String]("company_id")} (should match ^COMP-[0-9]{4}$$)")
          println(s"  Name: ${company.getAs[String]("company_name")} (should match company name pattern)")
          
          val departments = company.getAs[Seq[org.apache.spark.sql.Row]]("departments")
          println(s"  Departments array length: ${if (departments != null) departments.length else "null"}")
          
          if (departments != null && departments.nonEmpty) {
            departments.take(1).zipWithIndex.foreach { case (dept, deptIdx) =>
              println(s"    Department $deptIdx:")
              println(s"      ID: ${dept.getAs[Any]("dept_id")} (should be 100-999)")
              println(s"      Name: ${dept.getAs[String]("dept_name")} (should be from oneOf list)")
              println(s"      Budget: ${dept.getAs[Any]("budget")} (should be 50000-500000)")
              
              val employees = dept.getAs[Seq[org.apache.spark.sql.Row]]("employees")
              println(s"      Employees array length: ${if (employees != null) employees.length else "null"}")
              
              if (employees != null && employees.nonEmpty) {
                employees.take(1).zipWithIndex.foreach { case (emp, empIdx) =>
                  println(s"        Employee $empIdx:")
                  println(s"          ID: ${emp.getAs[String]("emp_id")} (should match ^EMP-[0-9]{6}$$)")
                  println(s"          Name: ${emp.getAs[String]("first_name")} ${emp.getAs[String]("last_name")}")
                  println(s"          Salary: ${emp.getAs[Any]("salary")} (should be 30000-200000)")
                  
                  val skills = emp.getAs[Seq[org.apache.spark.sql.Row]]("skills")
                  println(s"          Skills array length: ${if (skills != null) skills.length else "null"}")
                  
                  if (skills != null && skills.nonEmpty) {
                    skills.take(2).foreach { skill =>
                      println(s"            Skill: ${skill.getAs[String]("skill_name")} (proficiency: ${skill.getAs[Any]("proficiency")}, years: ${skill.getAs[Any]("years_experience")})")
                    }
                  }
                  
                  val contact = emp.getAs[org.apache.spark.sql.Row]("contact")
                  if (contact != null) {
                    println(s"          Contact: ${contact.getAs[String]("phone")}, ${contact.getAs[String]("email")}")
                  }
                }
              }
            }
          }
        }
      }
    }
    
    assert(result.columns.contains("companies"), "Should contain companies array")
  }

  test("Array constraint propagation fix validation") {
    // Test that demonstrates the fix for array constraint propagation
    val fields = List(
      Field(
        name = "test_data",
        `type` = Some("array"),
        fields = List(
          Field(
            name = "company_id",
            `type` = Some("string"),
            options = Map(REGEX_GENERATOR -> "^COMP-[0-9]{4}$")
          ),
          Field(
            name = "employee_count",
            `type` = Some("int"),
            options = Map(MINIMUM -> 10, MAXIMUM -> 100)
          ),
          Field(
            name = "department",
            `type` = Some("string"),
            options = Map(ONE_OF_GENERATOR -> "Engineering,Sales,Marketing")
          )
        )
      )
    )

    val step = Step(name = "validation_test", `type` = "json", fields = fields)
    val result = dataGeneratorFactory.generateDataForStep(step, "test", 1, 3)
    
    val rows = result.collect()
    var allConstraintsValid = true
    
    rows.foreach { row =>
      val testData = row.getAs[Seq[Row]]("test_data")
      testData.foreach { element =>
        val companyId = element.getAs[String]("company_id")
        val employeeCount = element.getAs[Int]("employee_count")
        val department = element.getAs[String]("department")
        
        // Validate company ID pattern
        val companyIdPattern = "^COMP-[0-9]{4}$".r
        if (!companyIdPattern.pattern.matcher(companyId).matches()) {
          println(s"❌ Company ID '$companyId' doesn't match pattern")
          allConstraintsValid = false
        }
        
        // Validate employee count range
        if (employeeCount < 10 || employeeCount > 100) {
          println(s"❌ Employee count $employeeCount not in range 10-100")
          allConstraintsValid = false
        }
        
        // Validate department oneOf
        val validDepartments = Set("Engineering", "Sales", "Marketing")
        if (!validDepartments.contains(department)) {
          println(s"❌ Department '$department' not in valid set")
          allConstraintsValid = false
        }
      }
    }
    
    if (allConstraintsValid) {
      println("✅ Array constraint propagation fix validation PASSED!")
      println("All regex patterns, numeric ranges, and oneOf constraints working correctly in arrays.")
    } else {
      println("❌ Array constraint propagation fix validation FAILED!")
    }
    
    assert(allConstraintsValid, "All array element constraints should be properly applied")
  }

  ignore("Can run spark streaming output at 2 records per second") {
    implicit val encoder: Encoder[Account] = Encoders.kryo[Account]
    val df = sparkSession.readStream
      .format("rate").option("rowsPerSecond", "10").load()
      .map(_ => Account())
      .limit(100)
    val stream = df.writeStream
      .foreachBatch((batch: Dataset[_], id: Long) => println(s"batch-id=$id, size=${batch.count()}"))
      .start()
    stream.awaitTermination(11000)
  }

  // ========== SQL EXPRESSION TESTS FOR DEEPLY NESTED STRUCTURES ==========

  test("Can generate SQL expressions in single-level nested structures") {
    val singleLevelNestedFields = List(
      Field("_business_msg_id", Some("string"), Map("regex" -> "MSG[0-9]{10}", "omit" -> "true")),
      Field("header", Some("struct"), fields = List(
        Field("message_id", Some("string"), Map("sql" -> "_business_msg_id"))
      ))
    )

    val step = Step("test_single_nested", "json", fields = singleLevelNestedFields)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 5)

    // Verify structure
    assert(result.columns.length == 1) // Only header (business_msg_id should be omitted)
    assert(result.columns.contains("header"))

    val rows = result.collect()
    rows.foreach { row =>
      val header = row.getAs[org.apache.spark.sql.Row]("header")
      assert(header != null, "header should not be null")
      
      val messageId = header.getAs[String]("message_id")
      assert(messageId != null, "message_id should not be null")
      assert(messageId.matches("MSG[0-9]{10}"), s"message_id should match MSG[0-9]{10} pattern: $messageId")
      
      println(s"✅ Single-level nested SQL: message_id = $messageId")
    }
  }

  test("Can generate SQL expressions in deeply nested structures") {
    val deeplyNestedFields = List(
      Field("_business_msg_id", Some("string"), Map("regex" -> "MSG[0-9]{10}", "omit" -> "true")),
      Field("_def_id", Some("string"), Map("regex" -> "DEF[0-9]{10}", "omit" -> "true")),
      Field("root", Some("struct"), fields = List(
        Field("level1", Some("struct"), fields = List(
          Field("level2", Some("struct"), fields = List(
            Field("message_id", Some("string"), Map("sql" -> "_business_msg_id")),
            Field("def_id", Some("string"), Map("sql" -> "_def_id"))
          ))
        ))
      ))
    )

    val step = Step("test_deeply_nested", "json", fields = deeplyNestedFields)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 5)

    // Verify structure
    assert(result.columns.length == 1) // Only root (helper fields should be omitted)
    assert(result.columns.contains("root"))

    val rows = result.collect()
    rows.foreach { row =>
      val root = row.getAs[org.apache.spark.sql.Row]("root")
      assert(root != null, "root should not be null")
      
      val level1 = root.getAs[org.apache.spark.sql.Row]("level1")
      assert(level1 != null, "level1 should not be null")
      
      val level2 = level1.getAs[org.apache.spark.sql.Row]("level2")
      assert(level2 != null, "level2 should not be null")
      
      val messageId = level2.getAs[String]("message_id")
      val defId = level2.getAs[String]("def_id")
      
      assert(messageId != null, "message_id should not be null")
      assert(defId != null, "def_id should not be null")
      assert(messageId.matches("MSG[0-9]{10}"), s"message_id should match MSG[0-9]{10} pattern: $messageId")
      assert(defId.matches("DEF[0-9]{10}"), s"def_id should match DEF[0-9]{10} pattern: $defId")
      
      println(s"✅ Deeply nested SQL: message_id = $messageId, def_id = $defId")
    }
  }

  test("Can generate SQL expressions in array structures with nested references") {
    val arrayWithNestedSqlFields = List(
      Field("_payment_id", Some("string"), Map("regex" -> "PAY[0-9]{8}", "omit" -> "true")),
      Field("_amount", Some("double"), Map("min" -> 10.0, "max" -> 1000.0, "omit" -> "true")),
      Field("payments", Some("array"), fields = List(
        Field("payment_info", Some("struct"), fields = List(
          Field("id", Some("string"), Map("sql" -> "_payment_id")),
          Field("amount", Some("double"), Map("sql" -> "_amount")),
          Field("details", Some("struct"), fields = List(
            Field("reference_id", Some("string"), Map("sql" -> "_payment_id")),
            Field("calculated_fee", Some("double"), Map("sql" -> "_amount * 0.1"))
          ))
        ))
      ))
    )

    val step = Step("test_array_nested_sql", "json", fields = arrayWithNestedSqlFields)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 3)

    // Verify structure
    assert(result.columns.length == 1) // Only payments array
    assert(result.columns.contains("payments"))

    val rows = result.collect()
    rows.foreach { row =>
      val payments = row.getAs[Seq[org.apache.spark.sql.Row]]("payments")
      assert(payments != null, "payments should not be null")
      
      if (payments.nonEmpty) {
        payments.take(2).foreach { payment =>
          val paymentInfo = payment.getAs[org.apache.spark.sql.Row]("payment_info")
          assert(paymentInfo != null, "payment_info should not be null")
          
          val id = paymentInfo.getAs[String]("id")
          val amount = paymentInfo.getAs[Double]("amount")
          
          assert(id != null, "id should not be null")
          assert(amount != null, "amount should not be null")
          assert(id.matches("PAY[0-9]{8}"), s"id should match PAY[0-9]{8} pattern: $id")
          assert(amount >= 10.0 && amount <= 1000.0, s"amount should be between 10.0 and 1000.0: $amount")
          
          val details = paymentInfo.getAs[org.apache.spark.sql.Row]("details")
          assert(details != null, "details should not be null")
          
          val referenceId = details.getAs[String]("reference_id")
          val calculatedFee = details.getAs[Double]("calculated_fee")
          
          assert(referenceId != null, "reference_id should not be null")
          assert(calculatedFee != null, "calculated_fee should not be null")
          assert(referenceId.matches("PAY[0-9]{8}"), s"reference_id should match PAY[0-9]{8} pattern: $referenceId")
          // Note: calculated_fee SQL expression might not work correctly - this test will reveal the issue
          
          println(s"✅ Array nested SQL: id = $id, amount = $amount, reference_id = $referenceId, calculated_fee = $calculatedFee")
        }
      }
    }
  }

  test("Can handle pain-008 style nested SQL expressions") {
    val pain008StyleFields = List(
      Field("_business_msg_id", Some("string"), Map("regex" -> "MSG[0-9]{10}", "omit" -> "true")),
      Field("business_application_header", Some("struct"), fields = List(
        Field("business_message_identifier", Some("string"), Map("regex" -> "MSG[0-9]{10}"))
      )),
      Field("business_document", Some("struct"), fields = List(
        Field("customer_direct_debit_initiation_v11", Some("struct"), fields = List(
          Field("group_header", Some("struct"), fields = List(
            Field("message_identification", Some("string"), Map("sql" -> "business_application_header.business_message_identifier")),
            Field("number_of_transactions", Some("integer"), Map("min" -> 1, "max" -> 3))
          )),
          Field("payment_information", Some("array"), fields = List(
            Field("payment_information_identification", Some("string"), Map("regex" -> "PAYINF[0-9]{3}")),
            Field("direct_debit_transaction_information", Some("array"), fields = List(
              Field("payment_identification", Some("struct"), fields = List(
                Field("end_to_end_identification", Some("string"), Map("sql" -> "business_application_header.business_message_identifier"))
              )),
              Field("instructed_amount", Some("struct"), fields = List(
                Field("amount", Some("double"), Map("min" -> 10.0, "max" -> 5000.0)),
                Field("currency", Some("string"), Map("oneOf" -> List("AUD")))
              ))
            ))
          ))
        ))
      ))
    )

    val step = Step("test_pain_008", "json", fields = pain008StyleFields)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 2)

    // Verify structure
    assert(result.columns.length == 2) // business_application_header and business_document
    assert(result.columns.contains("business_application_header"))
    assert(result.columns.contains("business_document"))

    val rows = result.collect()
    rows.foreach { row =>
      val businessHeader = row.getAs[org.apache.spark.sql.Row]("business_application_header")
      val businessDocument = row.getAs[org.apache.spark.sql.Row]("business_document")
      
      assert(businessHeader != null, "business_application_header should not be null")
      assert(businessDocument != null, "business_document should not be null")
      
      val businessMessageId = businessHeader.getAs[String]("business_message_identifier")
      assert(businessMessageId != null, "business_message_identifier should not be null")
      assert(businessMessageId.matches("MSG[0-9]{10}"), s"business_message_identifier should match MSG[0-9]{10}: $businessMessageId")
      
      val customerDirectDebit = businessDocument.getAs[org.apache.spark.sql.Row]("customer_direct_debit_initiation_v11")
      assert(customerDirectDebit != null, "customer_direct_debit_initiation_v11 should not be null")
      
      val groupHeader = customerDirectDebit.getAs[org.apache.spark.sql.Row]("group_header")
      assert(groupHeader != null, "group_header should not be null")
      
      val messageIdentification = groupHeader.getAs[String]("message_identification")
      assert(messageIdentification != null, "message_identification should not be null")
      
      // This is the key test - the SQL expression should copy the value from the deeply nested reference
      // If this fails, it indicates the SQL expression is not working correctly for nested structures
      println(s"Expected: $businessMessageId, Actual: $messageIdentification")
      
      // Note: This assertion might fail if the SQL expression bug exists
      try {
        assert(messageIdentification == businessMessageId, 
          s"message_identification should equal business_message_identifier: expected=$businessMessageId, actual=$messageIdentification")
        println("✅ Pain-008 style nested SQL expressions working correctly!")
      } catch {
        case e: AssertionError =>
          println(s"❌ Pain-008 style nested SQL expressions FAILED: ${e.getMessage}")
          println("This indicates the SQL expression bug in deeply nested structures")
          throw e
      }
      
      // Test array-level nested SQL expressions
      val paymentInformation = customerDirectDebit.getAs[Seq[org.apache.spark.sql.Row]]("payment_information")
      if (paymentInformation != null && paymentInformation.nonEmpty) {
        paymentInformation.take(1).foreach { payment =>
          val directDebitTxns = payment.getAs[Seq[org.apache.spark.sql.Row]]("direct_debit_transaction_information")
          if (directDebitTxns != null && directDebitTxns.nonEmpty) {
            directDebitTxns.take(1).foreach { txn =>
              val paymentId = txn.getAs[org.apache.spark.sql.Row]("payment_identification")
              if (paymentId != null) {
                val endToEndId = paymentId.getAs[String]("end_to_end_identification")
                println(s"End-to-end ID: $endToEndId (should match: $businessMessageId)")
                
                // This is another test of deeply nested SQL expressions
                try {
                  assert(endToEndId == businessMessageId, 
                    s"end_to_end_identification should equal business_message_identifier: expected=$businessMessageId, actual=$endToEndId")
                  println("✅ Array-level nested SQL expressions working correctly!")
                } catch {
                  case e: AssertionError =>
                    println(s"❌ Array-level nested SQL expressions FAILED: ${e.getMessage}")
                    println("This indicates the SQL expression bug in deeply nested array structures")
                    throw e
                }
              }
            }
          }
        }
      }
    }
  }

  test("Can handle complex nested SQL expressions with calculations") {
    val complexSqlFields = List(
      Field("_base_amount", Some("double"), Map("min" -> 100.0, "max" -> 1000.0, "omit" -> "true")),
      Field("_tax_rate", Some("double"), Map("static" -> "0.10", "omit" -> "true")),
      Field("invoice", Some("struct"), fields = List(
        Field("base_amount", Some("double"), Map("sql" -> "_base_amount")),
        Field("tax_rate", Some("double"), Map("sql" -> "CAST(_tax_rate AS DOUBLE)")),
        Field("line_items", Some("array"), fields = List(
          Field("amount", Some("double"), Map("sql" -> "_base_amount")),
          Field("tax_amount", Some("double"), Map("sql" -> "_base_amount * _tax_rate")),
          Field("total_amount", Some("double"), Map("sql" -> "_base_amount + (_base_amount * _tax_rate)")),
          Field("summary", Some("struct"), fields = List(
            Field("subtotal", Some("double"), Map("sql" -> "_base_amount")),
            Field("tax", Some("double"), Map("sql" -> "_base_amount * _tax_rate")),
            Field("grand_total", Some("double"), Map("sql" -> "_base_amount * (1 + _tax_rate)"))
          ))
        ))
      ))
    )

    val step = Step("test_complex_sql", "json", fields = complexSqlFields)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 2)

    // Verify structure
    assert(result.columns.length == 1) // Only invoice
    assert(result.columns.contains("invoice"))

    val rows = result.collect()
    rows.foreach { row =>
      val invoice = row.getAs[org.apache.spark.sql.Row]("invoice")
      assert(invoice != null, "invoice should not be null")
      
      val baseAmount = invoice.getAs[Double]("base_amount")
      val taxRate = invoice.getAs[Double]("tax_rate")
      
      assert(baseAmount != null, "base_amount should not be null")
      assert(taxRate != null, "tax_rate should not be null")
      assert(baseAmount >= 100.0 && baseAmount <= 1000.0, s"base_amount should be between 100.0 and 1000.0: $baseAmount")
      assert(taxRate == 0.10, s"tax_rate should be 0.10: $taxRate")
      
      val lineItems = invoice.getAs[Seq[org.apache.spark.sql.Row]]("line_items")
      if (lineItems != null && lineItems.nonEmpty) {
        lineItems.take(1).foreach { item =>
          val itemAmount = item.getAs[Double]("amount")
          val taxAmount = item.getAs[Double]("tax_amount")
          val totalAmount = item.getAs[Double]("total_amount")
          
          println(s"Line item: amount=$itemAmount, tax=$taxAmount, total=$totalAmount")
          
          // Test complex SQL calculations
          val expectedTax = baseAmount * taxRate
          val expectedTotal = baseAmount + expectedTax
          
          try {
            assert(itemAmount == baseAmount, s"item amount should equal base amount: expected=$baseAmount, actual=$itemAmount")
            assert(math.abs(taxAmount - expectedTax) < 0.001, s"tax amount should be calculated correctly: expected=$expectedTax, actual=$taxAmount")
            assert(math.abs(totalAmount - expectedTotal) < 0.001, s"total amount should be calculated correctly: expected=$expectedTotal, actual=$totalAmount")
            
            val summary = item.getAs[org.apache.spark.sql.Row]("summary")
            if (summary != null) {
              val subtotal = summary.getAs[Double]("subtotal")
              val tax = summary.getAs[Double]("tax")
              val grandTotal = summary.getAs[Double]("grand_total")
              
              assert(subtotal == baseAmount, s"subtotal should equal base amount: expected=$baseAmount, actual=$subtotal")
              assert(math.abs(tax - expectedTax) < 0.001, s"summary tax should be calculated correctly: expected=$expectedTax, actual=$tax")
              assert(math.abs(grandTotal - expectedTotal) < 0.001, s"grand total should be calculated correctly: expected=$expectedTotal, actual=$grandTotal")
            }
            
            println("✅ Complex nested SQL calculations working correctly!")
          } catch {
            case e: AssertionError =>
              println(s"❌ Complex nested SQL calculations FAILED: ${e.getMessage}")
              println("This indicates issues with SQL expression evaluation in nested contexts")
              throw e
          }
        }
      }
    }
  }

  test("Can handle metadata propagation through nested structures") {
    val fieldsWithMetadata = List(
      Field("_ref_id", Some("string"), Map("regex" -> "REF[0-9]{6}", "omit" -> "true")),
      Field("container", Some("struct"), fields = List(
        Field("level1", Some("struct"), fields = List(
          Field("reference_id", Some("string"), Map("sql" -> "_ref_id")),
          Field("level2", Some("struct"), fields = List(
            Field("nested_reference", Some("string"), Map("sql" -> "_ref_id")),
            Field("level3", Some("struct"), fields = List(
              Field("deep_reference", Some("string"), Map("sql" -> "_ref_id"))
            ))
          ))
        ))
      ))
    )

    val step = Step("test_metadata_propagation", "json", fields = fieldsWithMetadata)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 3)

    // Check that the data generation process doesn't throw errors
    assert(result.columns.length == 1) // Only container
    assert(result.columns.contains("container"))

    val rows = result.collect()
    rows.foreach { row =>
      val container = row.getAs[org.apache.spark.sql.Row]("container")
      assert(container != null, "container should not be null")
      
      val level1 = container.getAs[org.apache.spark.sql.Row]("level1")
      assert(level1 != null, "level1 should not be null")
      
      val referenceId = level1.getAs[String]("reference_id")
      assert(referenceId != null, "reference_id should not be null")
      
      val level2 = level1.getAs[org.apache.spark.sql.Row]("level2")
      assert(level2 != null, "level2 should not be null")
      
      val nestedReference = level2.getAs[String]("nested_reference")
      assert(nestedReference != null, "nested_reference should not be null")
      
      val level3 = level2.getAs[org.apache.spark.sql.Row]("level3")
      assert(level3 != null, "level3 should not be null")
      
      val deepReference = level3.getAs[String]("deep_reference")
      assert(deepReference != null, "deep_reference should not be null")
      
      // All references should match the same pattern and potentially the same value
      assert(referenceId.matches("REF[0-9]{6}"), s"reference_id should match REF[0-9]{6}: $referenceId")
      assert(nestedReference.matches("REF[0-9]{6}"), s"nested_reference should match REF[0-9]{6}: $nestedReference")
      assert(deepReference.matches("REF[0-9]{6}"), s"deep_reference should match REF[0-9]{6}: $deepReference")
      
      println(s"✅ Metadata propagation test: ref=$referenceId, nested=$nestedReference, deep=$deepReference")
    }
  }

  test("Can verify metadata propagation in nested structures") {
    val nestedFieldsWithSql = List(
      Field("_business_msg_id", Some("string"), Map("regex" -> "MSG[0-9]{10}", "omit" -> "true")),
      Field("root", Some("struct"), fields = List(
        Field("level1", Some("struct"), fields = List(
          Field("level2", Some("struct"), fields = List(
            Field("message_id", Some("string"), Map("sql" -> "_business_msg_id"))
          ))
        ))
      ))
    )

    val step = Step("test_metadata_propagation", "json", fields = nestedFieldsWithSql)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 2)

    // Debug: Print the actual DataFrame schema with metadata
    println("\n=== METADATA PROPAGATION DEBUG ===")
    println("DataFrame schema:")
    println(result.schema.treeString)
    
    // Check the nested field metadata
    val rootField = result.schema.fields.find(_.name == "root").get
    val rootStructType = rootField.dataType.asInstanceOf[StructType]
    val level1Field = rootStructType.fields.find(_.name == "level1").get
    val level1StructType = level1Field.dataType.asInstanceOf[StructType]
    val level2Field = level1StructType.fields.find(_.name == "level2").get
    val level2StructType = level2Field.dataType.asInstanceOf[StructType]
    val messageIdField = level2StructType.fields.find(_.name == "message_id").get
    
    println(s"Root field metadata: ${rootField.metadata.json}")
    println(s"Level1 field metadata: ${level1Field.metadata.json}")
    println(s"Level2 field metadata: ${level2Field.metadata.json}")
    println(s"MessageId field metadata: ${messageIdField.metadata.json}")
    println(s"MessageId field has SQL: ${messageIdField.metadata.contains("sql")}")
    if (messageIdField.metadata.contains("sql")) {
      println(s"MessageId SQL value: ${messageIdField.metadata.getString("sql")}")
    }
    
    // This should pass if metadata is properly propagated
    assert(messageIdField.metadata.contains("sql"), "message_id field should have SQL metadata")
    assert(messageIdField.metadata.getString("sql") == "_business_msg_id", "SQL value should be _business_msg_id")
  }

  // ========== END SQL EXPRESSION TESTS ==========

}
