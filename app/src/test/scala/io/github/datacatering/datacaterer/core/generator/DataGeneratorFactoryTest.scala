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
    // Weight fields are kept until SinkFactory
    assertResult(Array("id", "amount", "debit_credit", "name", "code", "party_id", "customer_id", "rank_weight", "rank", "rating_weight", "rating"))(df.columns)
    assertResult(Array(
      ("id", spark.sql.types.StringType),
      ("amount", spark.sql.types.DoubleType),
      ("debit_credit", spark.sql.types.StringType),
      ("name", spark.sql.types.StringType),
      ("code", spark.sql.types.IntegerType),
      ("party_id", spark.sql.types.StringType),
      ("customer_id", spark.sql.types.IntegerType),
      ("rank_weight", spark.sql.types.DoubleType),
      ("rank", spark.sql.types.IntegerType),
      ("rating_weight", spark.sql.types.DoubleType),
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
      val rankWeight = row.getDouble(7)
      assert(rankWeight >= 0.0 && rankWeight <= 1.0)
      val rank = row.getInt(8)
      assert(rank == 1 || rank == 2 || rank == 3)
      val ratingWeight = row.getDouble(9)
      assert(ratingWeight >= 0.0 && ratingWeight <= 1.0)
      val rating = row.getString(10)
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
    val customerDirectDebit = row.getAs[Row]("customer_direct_debit_initiation_v11")
    assert(customerDirectDebit != null, "customer_direct_debit_initiation_v11 should not be null")
    
    // Check group_header structure
    val groupHeader = customerDirectDebit.getAs[Row]("group_header")
    assert(groupHeader != null, "group_header should not be null")
    
    val groupHeaderSchema = groupHeader.schema
    assert(groupHeaderSchema.fieldNames.contains("message_identification"), "group_header should contain message_identification")
    assert(groupHeaderSchema.fieldNames.contains("creation_date_time"), "group_header should contain creation_date_time")
    assert(groupHeaderSchema.fieldNames.contains("number_of_transactions"), "group_header should contain number_of_transactions")
    assert(groupHeaderSchema.fieldNames.contains("control_sum"), "group_header should contain control_sum")
    assert(groupHeaderSchema.fieldNames.contains("initiating_party"), "group_header should contain initiating_party")
    
    // Check initiating_party nested structure
    val initiatingParty = groupHeader.getAs[Row]("initiating_party")
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
    val dataContainer = row.getAs[Row]("data_container")
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
    val customerData = row.getAs[Row]("customer_data")
    assert(customerData != null, "customer_data should not be null")
    
    val customerDataSchema = customerData.schema
    
    // Check basic_info structure
    assert(customerDataSchema.fieldNames.contains("basic_info"), "customer_data should contain basic_info")
    val basicInfo = customerData.getAs[Row]("basic_info")
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
      val users = row.getAs[Seq[Row]]("users")
      println(s"Users array length: ${if (users != null) users.length else "null"}")
      
      if (users != null && users.nonEmpty) {
        users.take(2).zipWithIndex.foreach { case (user, idx) =>
          println(s"User $idx:")
          println(s"  ID: ${user.getAs[Any]("id")} (should be 1-1000)")
          println(s"  Name: ${user.getAs[String]("name")} (should match ^[A-Z][a-z]{2,10}$$)")
          println(s"  Email: ${user.getAs[String]("email")} (should be valid email)")
          println(s"  Age: ${user.getAs[Any]("age")} (should be 18-65)")
          
          val profile = user.getAs[Row]("profile")
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
      val companies = row.getAs[Seq[Row]]("companies")
      println(s"Companies array length: ${if (companies != null) companies.length else "null"}")
      
      if (companies != null && companies.nonEmpty) {
        companies.take(1).zipWithIndex.foreach { case (company, companyIdx) =>
          println(s"Company $companyIdx:")
          println(s"  ID: ${company.getAs[String]("company_id")} (should match ^COMP-[0-9]{4}$$)")
          println(s"  Name: ${company.getAs[String]("company_name")} (should match company name pattern)")
          
          val departments = company.getAs[Seq[Row]]("departments")
          println(s"  Departments array length: ${if (departments != null) departments.length else "null"}")
          
          if (departments != null && departments.nonEmpty) {
            departments.take(1).zipWithIndex.foreach { case (dept, deptIdx) =>
              println(s"    Department $deptIdx:")
              println(s"      ID: ${dept.getAs[Any]("dept_id")} (should be 100-999)")
              println(s"      Name: ${dept.getAs[String]("dept_name")} (should be from oneOf list)")
              println(s"      Budget: ${dept.getAs[Any]("budget")} (should be 50000-500000)")
              
              val employees = dept.getAs[Seq[Row]]("employees")
              println(s"      Employees array length: ${if (employees != null) employees.length else "null"}")
              
              if (employees != null && employees.nonEmpty) {
                employees.take(1).zipWithIndex.foreach { case (emp, empIdx) =>
                  println(s"        Employee $empIdx:")
                  println(s"          ID: ${emp.getAs[String]("emp_id")} (should match ^EMP-[0-9]{6}$$)")
                  println(s"          Name: ${emp.getAs[String]("first_name")} ${emp.getAs[String]("last_name")}")
                  println(s"          Salary: ${emp.getAs[Any]("salary")} (should be 30000-200000)")
                  
                  val skills = emp.getAs[Seq[Row]]("skills")
                  println(s"          Skills array length: ${if (skills != null) skills.length else "null"}")
                  
                  if (skills != null && skills.nonEmpty) {
                    skills.take(2).foreach { skill =>
                      println(s"            Skill: ${skill.getAs[String]("skill_name")} (proficiency: ${skill.getAs[Any]("proficiency")}, years: ${skill.getAs[Any]("years_experience")})")
                    }
                  }
                  
                  val contact = emp.getAs[Row]("contact")
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

    // Verify structure - omitted fields are kept until SinkFactory
    assert(result.columns.contains("_business_msg_id"))
    assert(result.columns.contains("header"))

    val rows = result.collect()
    rows.foreach { row =>
      val header = row.getAs[Row]("header")
      assert(header != null, "header should not be null")
      
      val messageId = header.getAs[String]("message_id")
      assert(messageId != null, "message_id should not be null")
      assert(messageId.matches("MSG[0-9]{10}"), s"message_id should match MSG[0-9]{10} pattern: $messageId")
      
      // Verify SQL dependency is correct
      val businessMsgId = row.getAs[String]("_business_msg_id")
      assert(businessMsgId == messageId, "message_id should match _business_msg_id")
      
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

    // Verify structure - omitted fields are kept until SinkFactory
    assert(result.columns.contains("_business_msg_id"))
    assert(result.columns.contains("_def_id"))
    assert(result.columns.contains("root"))

    val rows = result.collect()
    rows.foreach { row =>
      val root = row.getAs[Row]("root")
      assert(root != null, "root should not be null")
      
      val level1 = root.getAs[Row]("level1")
      assert(level1 != null, "level1 should not be null")
      
      val level2 = level1.getAs[Row]("level2")
      assert(level2 != null, "level2 should not be null")
      
      val messageId = level2.getAs[String]("message_id")
      val defId = level2.getAs[String]("def_id")
      
      assert(messageId != null, "message_id should not be null")
      assert(defId != null, "def_id should not be null")
      assert(messageId.matches("MSG[0-9]{10}"), s"message_id should match MSG[0-9]{10} pattern: $messageId")
      assert(defId.matches("DEF[0-9]{10}"), s"def_id should match DEF[0-9]{10} pattern: $defId")
      
      // Verify SQL dependencies are correct
      val businessMsgId = row.getAs[String]("_business_msg_id")
      val defIdSource = row.getAs[String]("_def_id")
      assert(businessMsgId == messageId, "message_id should match _business_msg_id")
      assert(defIdSource == defId, "def_id should match _def_id")
      
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

    // Verify structure - omitted fields are kept until SinkFactory
    assert(result.columns.contains("_payment_id"))
    assert(result.columns.contains("_amount"))
    assert(result.columns.contains("payments"))

    val rows = result.collect()
    rows.foreach { row =>
      val payments = row.getAs[Seq[Row]]("payments")
      assert(payments != null, "payments should not be null")
      
      // Get source values for SQL dependencies
      val paymentId = row.getAs[String]("_payment_id")
      val amount = row.getAs[Double]("_amount")
      
      if (payments.nonEmpty) {
        payments.take(2).foreach { payment =>
          val paymentInfo = payment.getAs[Row]("payment_info")
          assert(paymentInfo != null, "payment_info should not be null")
          
          val id = paymentInfo.getAs[String]("id")
          val paymentAmount = paymentInfo.getAs[Double]("amount")
          
          assert(id != null, "id should not be null")
          assert(id.matches("PAY[0-9]{8}"), s"id should match PAY[0-9]{8} pattern: $id")
          assert(paymentAmount >= 10.0 && paymentAmount <= 1000.0, s"amount should be between 10.0 and 1000.0: $paymentAmount")
          
          // Verify SQL dependencies
          assert(id == paymentId, "payment_info.id should match _payment_id")
          assert(paymentAmount == amount, "payment_info.amount should match _amount")
          
          val details = paymentInfo.getAs[Row]("details")
          assert(details != null, "details should not be null")
          
          val referenceId = details.getAs[String]("reference_id")
          val calculatedFee = details.getAs[Double]("calculated_fee")
          
          assert(referenceId != null, "reference_id should not be null")
          assert(referenceId.matches("PAY[0-9]{8}"), s"reference_id should match PAY[0-9]{8} pattern: $referenceId")
          
          // Verify SQL dependencies
          assert(referenceId == paymentId, "details.reference_id should match _payment_id")
          assert(calculatedFee == amount * 0.1, "details.calculated_fee should be _amount * 0.1")
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

    // Verify structure - omitted fields are kept until SinkFactory
    assert(result.columns.contains("_business_msg_id"))
    assert(result.columns.contains("business_application_header"))
    assert(result.columns.contains("business_document"))

    val rows = result.collect()
    rows.foreach { row =>
      val businessMsgId = row.getAs[String]("_business_msg_id")
      val header = row.getAs[Row]("business_application_header")
      val document = row.getAs[Row]("business_document")
      
      assert(header != null, "business_application_header should not be null")
      assert(document != null, "business_document should not be null")
      
      val headerMsgId = header.getAs[String]("business_message_identifier")
      assert(headerMsgId.matches("MSG[0-9]{10}"), s"business_message_identifier should match MSG[0-9]{10} pattern: $headerMsgId")
      
      val directDebit = document.getAs[Row]("customer_direct_debit_initiation_v11")
      assert(directDebit != null, "customer_direct_debit_initiation_v11 should not be null")
      
      val groupHeader = directDebit.getAs[Row]("group_header")
      assert(groupHeader != null, "group_header should not be null")
      
      val messageId = groupHeader.getAs[String]("message_identification")
      assert(messageId != null, "message_identification should not be null")
      assert(messageId == headerMsgId, "message_identification should match business_message_identifier")
      
      val numTransactions = groupHeader.getAs[Int]("number_of_transactions")
      assert(numTransactions >= 1 && numTransactions <= 3, s"number_of_transactions should be between 1 and 3: $numTransactions")
      
      val paymentInfo = directDebit.getAs[Seq[Row]]("payment_information")
      if (paymentInfo != null && paymentInfo.nonEmpty) {
        paymentInfo.foreach { payment =>
          val paymentId = payment.getAs[String]("payment_information_identification")
          assert(paymentId.matches("PAYINF[0-9]{3}"), s"payment_information_identification should match PAYINF[0-9]{3} pattern: $paymentId")
          
          val transactions = payment.getAs[Seq[Row]]("direct_debit_transaction_information")
          if (transactions != null && transactions.nonEmpty) {
            transactions.foreach { transaction =>
              val paymentIdent = transaction.getAs[Row]("payment_identification")
              assert(paymentIdent != null, "payment_identification should not be null")
              
              val endToEndId = paymentIdent.getAs[String]("end_to_end_identification")
              assert(endToEndId == headerMsgId, "end_to_end_identification should match business_message_identifier")
              
              val amount = transaction.getAs[Row]("instructed_amount")
              assert(amount != null, "instructed_amount should not be null")
              
              val amountValue = amount.getAs[Double]("amount")
              assert(amountValue >= 10.0 && amountValue <= 5000.0, s"amount should be between 10.0 and 5000.0: $amountValue")
              
              val currency = amount.getAs[String]("currency")
              assert(currency == "AUD", s"currency should be AUD: $currency")
            }
          }
        }
      }
    }
  }

  test("Can handle complex nested SQL expressions with calculations") {
    val complexNestedSqlFields = List(
      Field("_base_amount", Some("double"), Map("min" -> 100.0, "max" -> 1000.0, "omit" -> "true")),
      Field("_tax_rate", Some("double"), Map("min" -> 0.1, "max" -> 0.3, "omit" -> "true")),
      Field("invoice", Some("struct"), fields = List(
        Field("details", Some("struct"), fields = List(
          Field("base_amount", Some("double"), Map("sql" -> "_base_amount")),
          Field("tax_rate", Some("double"), Map("sql" -> "_tax_rate")),
          Field("tax_amount", Some("double"), Map("sql" -> "_base_amount * _tax_rate")),
          Field("total_amount", Some("double"), Map("sql" -> "_base_amount + (_base_amount * _tax_rate)"))
        ))
      ))
    )

    val step = Step("test_complex_nested_sql", "json", fields = complexNestedSqlFields)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 3)

    // Verify structure - omitted fields are kept until SinkFactory
    assert(result.columns.contains("_base_amount"))
    assert(result.columns.contains("_tax_rate"))
    assert(result.columns.contains("invoice"))

    val rows = result.collect()
    rows.foreach { row =>
      val baseAmount = row.getAs[Double]("_base_amount")
      val taxRate = row.getAs[Double]("_tax_rate")
      assert(baseAmount >= 100.0 && baseAmount <= 1000.0, s"base_amount should be between 100.0 and 1000.0: $baseAmount")
      assert(taxRate >= 0.1 && taxRate <= 0.3, s"tax_rate should be between 0.1 and 0.3: $taxRate")
      
      val invoice = row.getAs[Row]("invoice")
      assert(invoice != null, "invoice should not be null")
      
      val details = invoice.getAs[Row]("details")
      assert(details != null, "details should not be null")
      
      val detailsBaseAmount = details.getAs[Double]("base_amount")
      val detailsTaxRate = details.getAs[Double]("tax_rate")
      val taxAmount = details.getAs[Double]("tax_amount")
      val totalAmount = details.getAs[Double]("total_amount")
      
      // Verify SQL dependencies
      assert(detailsBaseAmount == baseAmount, "details.base_amount should match _base_amount")
      assert(detailsTaxRate == taxRate, "details.tax_rate should match _tax_rate")
      assert(taxAmount == baseAmount * taxRate, "details.tax_amount should be _base_amount * _tax_rate")
      assert(totalAmount == baseAmount + (baseAmount * taxRate), "details.total_amount should be _base_amount + (_base_amount * _tax_rate)")
    }
  }

  test("Can handle metadata propagation through nested structures") {
    val nestedMetadataFields = List(
      Field("_ref_id", Some("string"), Map("regex" -> "REF[0-9]{10}", "omit" -> "true")),
      Field("container", Some("struct"), fields = List(
        Field("level1", Some("struct"), fields = List(
          Field("level2", Some("struct"), fields = List(
            Field("reference", Some("string"), Map("sql" -> "_ref_id"))
          ))
        ))
      ))
    )

    val step = Step("test_metadata_propagation", "json", fields = nestedMetadataFields)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 1)

    // Verify structure - omitted fields are kept until SinkFactory
    assert(result.columns.contains("_ref_id"))
    assert(result.columns.contains("container"))

    val rows = result.collect()
    rows.foreach { row =>
      val refId = row.getAs[String]("_ref_id")
      val container = row.getAs[Row]("container")
      assert(container != null, "container should not be null")
      
      val level1 = container.getAs[Row]("level1")
      assert(level1 != null, "level1 should not be null")
      
      val level2 = level1.getAs[Row]("level2")
      assert(level2 != null, "level2 should not be null")
      
      val reference = level2.getAs[String]("reference")
      assert(reference != null, "reference should not be null")
      assert(reference == refId, "reference should match _ref_id")
    }
  }

  test("Can verify metadata propagation in nested structures") {
    val nestedMetadataFields = List(
      Field("_business_msg_id", Some("string"), Map("regex" -> "MSG[0-9]{10}", "omit" -> "true")),
      Field("root", Some("struct"), fields = List(
        Field("level1", Some("struct"), fields = List(
          Field("level2", Some("struct"), fields = List(
            Field("message_id", Some("string"), Map("sql" -> "_business_msg_id"))
          ))
        ))
      ))
    )

    val step = Step("test_metadata_propagation", "json", fields = nestedMetadataFields)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 1)

    // Verify structure - omitted fields are kept until SinkFactory
    assert(result.columns.contains("_business_msg_id"))
    assert(result.columns.contains("root"))

    // Print metadata for debugging
    val rootField = result.schema("root")
    val level1Field = rootField.dataType.asInstanceOf[StructType]("level1")
    val level2Field = level1Field.dataType.asInstanceOf[StructType]("level2")
    val messageIdField = level2Field.dataType.asInstanceOf[StructType]("message_id")

    // Verify SQL dependencies
    val rows = result.collect()
    rows.foreach { row =>
      val businessMsgId = row.getAs[String]("_business_msg_id")
      val root = row.getAs[Row]("root")
      assert(root != null, "root should not be null")
      
      val level1 = root.getAs[Row]("level1")
      assert(level1 != null, "level1 should not be null")
      
      val level2 = level1.getAs[Row]("level2")
      assert(level2 != null, "level2 should not be null")
      
      val messageId = level2.getAs[String]("message_id")
      assert(messageId != null, "message_id should not be null")
      assert(messageId == businessMsgId, "message_id should match _business_msg_id")
    }
  }

  // ========== END SQL EXPRESSION TESTS ==========

  // ========== NESTED ARRAY SQL REFERENCE TESTS ==========

  test("Can handle SQL references within same array level") {
    val sameArrayLevelFields = List(
      Field("transactions", Some("array"), fields = List(
        Field("amount", Some("double"), Map("min" -> 10.0, "max" -> 1000.0)),
        Field("fee", Some("double"), Map("sql" -> "transactions.amount * 0.05")),
        Field("net_amount", Some("double"), Map("sql" -> "transactions.amount - transactions.fee")),
        Field("transaction_type", Some("string"), Map("sql" -> "CASE WHEN transactions.amount > 500 THEN 'LARGE' ELSE 'SMALL' END"))
      ))
    )

    val step = Step("test_same_array_level", "json", fields = sameArrayLevelFields)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 3)

    assert(result.columns.contains("transactions"))

    val rows = result.collect()
    rows.foreach { row =>
      val transactions = row.getAs[Seq[Row]]("transactions")
      assert(transactions != null, "transactions should not be null")
      
      if (transactions.nonEmpty) {
        transactions.foreach { transaction =>
          val amount = transaction.getAs[Double]("amount")
          val fee = transaction.getAs[Double]("fee")
          val netAmount = transaction.getAs[Double]("net_amount")
          val transactionType = transaction.getAs[String]("transaction_type")
          
          assert(amount >= 10.0 && amount <= 1000.0, s"amount should be between 10.0 and 1000.0: $amount")
          assert(fee == amount * 0.05, s"fee should be amount * 0.05: $fee")
          assert(netAmount == amount - fee, s"net_amount should be amount - fee: $netAmount")
          assert(
            (amount > 500 && transactionType == "LARGE") || (amount <= 500 && transactionType == "SMALL"),
            s"transaction_type should be 'LARGE' for amount > 500, 'SMALL' otherwise: $transactionType"
          )
        }
      }
    }
  }

  test("Can handle SQL references to nested arrays from outer arrays") {
    val outerToNestedArrayFields = List(
      Field("orders", Some("array"), fields = List(
        Field("order_id", Some("string"), Map("regex" -> "ORD[0-9]{8}")),
        Field("total_amount", Some("double"), Map("min" -> 100.0, "max" -> 2000.0)),
        Field("items", Some("array"), fields = List(
          Field("item_id", Some("string"), Map("regex" -> "ITEM[0-9]{8}")),
          Field("price", Some("double"), Map("min" -> 10.0, "max" -> 200.0)),
          Field("parent_order_id", Some("string"), Map("sql" -> "orders.order_id")),
          Field("percentage_of_total", Some("double"), Map("sql" -> "orders.items.price / orders.total_amount * 100"))
        ))
      ))
    )

    val step = Step("test_outer_to_nested_array", "json", fields = outerToNestedArrayFields)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 2)

    assert(result.columns.contains("orders"))

    val rows = result.collect()
    rows.foreach { row =>
      val orders = row.getAs[Seq[Row]]("orders")
      assert(orders != null, "orders should not be null")
      
      if (orders.nonEmpty) {
        orders.foreach { order =>
          val orderId = order.getAs[String]("order_id")
          val totalAmount = order.getAs[Double]("total_amount")
          val items = order.getAs[Seq[Row]]("items")
          
          assert(orderId.matches("ORD[0-9]{8}"), s"order_id should match ORD[0-9]{8} pattern: $orderId")
          assert(totalAmount >= 100.0 && totalAmount <= 2000.0, s"total_amount should be between 100.0 and 2000.0: $totalAmount")
          
          if (items != null && items.nonEmpty) {
            items.foreach { item =>
              val itemId = item.getAs[String]("item_id")
              val price = item.getAs[Double]("price")
              val parentOrderId = item.getAs[String]("parent_order_id")
              val percentageOfTotal = item.getAs[Double]("percentage_of_total")
              
              assert(itemId.matches("ITEM[0-9]{8}"), s"item_id should match ITEM[0-9]{8} pattern: $itemId")
              assert(price >= 10.0 && price <= 200.0, s"price should be between 10.0 and 200.0: $price")
              assert(parentOrderId == orderId, s"parent_order_id should match order_id: $parentOrderId")
              assert(percentageOfTotal == (price / totalAmount * 100), s"percentage_of_total should be price / total_amount * 100: $percentageOfTotal")
            }
          }
        }
      }
    }
  }

  test("Can handle SQL references to parent struct fields from within arrays") {
    val parentStructToArrayFields = List(
      Field("customer_id", Some("string"), Map("regex" -> "CUST[0-9]{8}")),
      Field("customer_name", Some("string"), Map("expression" -> "#{Name.name}")),
      Field("account_balance", Some("double"), Map("min" -> 1000.0, "max" -> 10000.0)),
      Field("transactions", Some("array"), fields = List(
        Field("transaction_id", Some("string"), Map("regex" -> "TXN[0-9]{10}")),
        Field("amount", Some("double"), Map("min" -> 10.0, "max" -> 500.0)),
        Field("owner_id", Some("string"), Map("sql" -> "customer_id")),
        Field("owner_name", Some("string"), Map("sql" -> "customer_name")),
        Field("balance_after_transaction", Some("double"), Map("sql" -> "account_balance - transactions.amount")),
        Field("is_large_for_balance", Some("boolean"), Map("sql" -> "transactions.amount > (account_balance * 0.1)"))
      ))
    )

    val step = Step("test_parent_struct_to_array", "json", fields = parentStructToArrayFields)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 2)

    assert(result.columns.contains("customer_id"))
    assert(result.columns.contains("customer_name"))
    assert(result.columns.contains("account_balance"))
    assert(result.columns.contains("transactions"))

    val rows = result.collect()
    rows.foreach { row =>
      val customerId = row.getAs[String]("customer_id")
      val customerName = row.getAs[String]("customer_name")
      val accountBalance = row.getAs[Double]("account_balance")
      val transactions = row.getAs[Seq[Row]]("transactions")
      
      assert(customerId.matches("CUST[0-9]{8}"), s"customer_id should match CUST[0-9]{8} pattern: $customerId")
      assert(customerName != null && customerName.nonEmpty, "customer_name should not be null or empty")
      assert(accountBalance >= 1000.0 && accountBalance <= 10000.0, s"account_balance should be between 1000.0 and 10000.0: $accountBalance")
      assert(transactions != null, "transactions should not be null")
      
      if (transactions.nonEmpty) {
        transactions.foreach { transaction =>
          val transactionId = transaction.getAs[String]("transaction_id")
          val amount = transaction.getAs[Double]("amount")
          val ownerId = transaction.getAs[String]("owner_id")
          val ownerName = transaction.getAs[String]("owner_name")
          val balanceAfterTransaction = transaction.getAs[Double]("balance_after_transaction")
          val isLargeForBalance = transaction.getAs[Boolean]("is_large_for_balance")
          
          assert(transactionId.matches("TXN[0-9]{10}"), s"transaction_id should match TXN[0-9]{10} pattern: $transactionId")
          assert(amount >= 10.0 && amount <= 500.0, s"amount should be between 10.0 and 500.0: $amount")
          assert(ownerId == customerId, s"owner_id should match customer_id: $ownerId")
          assert(ownerName == customerName, s"owner_name should match customer_name: $ownerName")
          assert(balanceAfterTransaction == accountBalance - amount, s"balance_after_transaction should be account_balance - amount: $balanceAfterTransaction")
          assert(isLargeForBalance == (amount > (accountBalance * 0.1)), s"is_large_for_balance should be amount > (account_balance * 0.1): $isLargeForBalance")
        }
      }
    }
  }

  test("Can handle SQL references across different array levels") {
    val crossArrayLevelFields = List(
      Field("customers", Some("array"), fields = List(
        Field("customer_id", Some("string"), Map("regex" -> "CUST[0-9]{8}")),
        Field("customer_name", Some("string"), Map("expression" -> "#{Name.name}")),
        Field("orders", Some("array"), fields = List(
          Field("order_id", Some("string"), Map("regex" -> "ORD[0-9]{8}")),
          Field("order_total", Some("double"), Map("min" -> 100.0, "max" -> 1000.0)),
          Field("customer_ref", Some("string"), Map("sql" -> "customers.customer_id")),
          Field("customer_name_ref", Some("string"), Map("sql" -> "customers.customer_name")),
          Field("line_items", Some("array"), fields = List(
            Field("item_id", Some("string"), Map("regex" -> "ITEM[0-9]{8}")),
            Field("item_price", Some("double"), Map("min" -> 10.0, "max" -> 200.0)),
            Field("grandparent_customer_id", Some("string"), Map("sql" -> "customers.customer_id")),
            Field("parent_order_id", Some("string"), Map("sql" -> "customers.orders.order_id")),
            Field("percentage_of_order", Some("double"), Map("sql" -> "customers.orders.line_items.item_price / customers.orders.order_total * 100"))
          ))
        ))
      ))
    )

    val step = Step("test_cross_array_level", "json", fields = crossArrayLevelFields)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 1)

    assert(result.columns.contains("customers"))

    val rows = result.collect()
    rows.foreach { row =>
      val customers = row.getAs[Seq[Row]]("customers")
      assert(customers != null, "customers should not be null")
      
      if (customers.nonEmpty) {
        customers.foreach { customer =>
          val customerId = customer.getAs[String]("customer_id")
          val customerName = customer.getAs[String]("customer_name")
          val orders = customer.getAs[Seq[Row]]("orders")
          
          assert(customerId.matches("CUST[0-9]{8}"), s"customer_id should match CUST[0-9]{8} pattern: $customerId")
          assert(customerName != null && customerName.nonEmpty, "customer_name should not be null or empty")
          assert(orders != null, "orders should not be null")
          
          if (orders.nonEmpty) {
            orders.foreach { order =>
              val orderId = order.getAs[String]("order_id")
              val orderTotal = order.getAs[Double]("order_total")
              val customerRef = order.getAs[String]("customer_ref")
              val customerNameRef = order.getAs[String]("customer_name_ref")
              val lineItems = order.getAs[Seq[Row]]("line_items")
              
              assert(orderId.matches("ORD[0-9]{8}"), s"order_id should match ORD[0-9]{8} pattern: $orderId")
              assert(orderTotal >= 100.0 && orderTotal <= 1000.0, s"order_total should be between 100.0 and 1000.0: $orderTotal")
              assert(customerRef == customerId, s"customer_ref should match customer_id: $customerRef")
              assert(customerNameRef == customerName, s"customer_name_ref should match customer_name: $customerNameRef")
              assert(lineItems != null, "line_items should not be null")
              
              if (lineItems.nonEmpty) {
                lineItems.foreach { lineItem =>
                  val itemId = lineItem.getAs[String]("item_id")
                  val itemPrice = lineItem.getAs[Double]("item_price")
                  val grandparentCustomerId = lineItem.getAs[String]("grandparent_customer_id")
                  val parentOrderId = lineItem.getAs[String]("parent_order_id")
                  val percentageOfOrder = lineItem.getAs[Double]("percentage_of_order")
                  
                  assert(itemId.matches("ITEM[0-9]{8}"), s"item_id should match ITEM[0-9]{8} pattern: $itemId")
                  assert(itemPrice >= 10.0 && itemPrice <= 200.0, s"item_price should be between 10.0 and 200.0: $itemPrice")
                  assert(grandparentCustomerId == customerId, s"grandparent_customer_id should match customer_id: $grandparentCustomerId")
                  assert(parentOrderId == orderId, s"parent_order_id should match order_id: $parentOrderId")
                  assert(percentageOfOrder == (itemPrice / orderTotal * 100), s"percentage_of_order should be item_price / order_total * 100: $percentageOfOrder")
                }
              }
            }
          }
        }
      }
    }
  }

  test("Can handle SQL references that skip intermediate array levels (pain-008 style)") {
    val skipIntermediateArrayFields = List(
      Field("business_application_header", Some("struct"), fields = List(
        Field("business_message_identifier", Some("string"), Map("regex" -> "MSG[0-9]{10}"))
      )),
      Field("business_document", Some("struct"), fields = List(
        Field("customer_direct_debit_initiation_v11", Some("struct"), fields = List(
          Field("payment_information", Some("array"), fields = List(
            Field("payment_information_identification", Some("string"), Map("regex" -> "PAYINF[0-9]{3}")),
            Field("debtor_agent", Some("struct"), fields = List(
              Field("financial_institution_identification", Some("struct"), fields = List(
                Field("bicfi", Some("string"), Map("regex" -> "CTBAAU2S[0-9]{3}"))
              ))
            )),
            Field("direct_debit_transaction_information", Some("array"), fields = List(
              Field("payment_identification", Some("struct"), fields = List(
                Field("end_to_end_identification", Some("string"), Map("regex" -> "E2E[0-9]{10}"))
              )),
              Field("debtor_account", Some("struct"), fields = List(
                Field("identification", Some("struct"), fields = List(
                  Field("other", Some("struct"), fields = List(
                    // This should work: references bicfi field that's in the same payment_information array
                    // but skips the direct_debit_transaction_information level
                    Field("identification", Some("string"), Map("sql" -> "CASE WHEN business_document.customer_direct_debit_initiation_v11.payment_information.debtor_agent.financial_institution_identification.bicfi = 'CTBAAU2S123' THEN '06223622993847' ELSE '12345612345678' END"))
                  ))
                ))
              ))
            ))
          ))
        ))
      ))
    )

    val step = Step("test_skip_intermediate_array", "json", fields = skipIntermediateArrayFields)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 2)

    assert(result.columns.contains("business_application_header"))
    assert(result.columns.contains("business_document"))

    val rows = result.collect()
    rows.foreach { row =>
      val header = row.getAs[Row]("business_application_header")
      val document = row.getAs[Row]("business_document")
      
      assert(header != null, "business_application_header should not be null")
      assert(document != null, "business_document should not be null")
      
      val headerMsgId = header.getAs[String]("business_message_identifier")
      assert(headerMsgId.matches("MSG[0-9]{10}"), s"business_message_identifier should match MSG[0-9]{10} pattern: $headerMsgId")
      
      val directDebit = document.getAs[Row]("customer_direct_debit_initiation_v11")
      assert(directDebit != null, "customer_direct_debit_initiation_v11 should not be null")
      
      val paymentInfo = directDebit.getAs[Seq[Row]]("payment_information")
      assert(paymentInfo != null, "payment_information should not be null")
      
      if (paymentInfo.nonEmpty) {
        paymentInfo.foreach { payment =>
          val paymentId = payment.getAs[String]("payment_information_identification")
          assert(paymentId.matches("PAYINF[0-9]{3}"), s"payment_information_identification should match PAYINF[0-9]{3} pattern: $paymentId")
          
          val debtorAgent = payment.getAs[Row]("debtor_agent")
          assert(debtorAgent != null, "debtor_agent should not be null")
          
          val financialInstitution = debtorAgent.getAs[Row]("financial_institution_identification")
          assert(financialInstitution != null, "financial_institution_identification should not be null")
          
          val bicfi = financialInstitution.getAs[String]("bicfi")
          assert(bicfi.matches("CTBAAU2S[0-9]{3}"), s"bicfi should match CTBAAU2S[0-9]{3} pattern: $bicfi")
          
          val transactions = payment.getAs[Seq[Row]]("direct_debit_transaction_information")
          assert(transactions != null, "direct_debit_transaction_information should not be null")
          
          if (transactions.nonEmpty) {
            transactions.foreach { transaction =>
              val paymentIdent = transaction.getAs[Row]("payment_identification")
              assert(paymentIdent != null, "payment_identification should not be null")
              
              val endToEndId = paymentIdent.getAs[String]("end_to_end_identification")
              assert(endToEndId.matches("E2E[0-9]{10}"), s"end_to_end_identification should match E2E[0-9]{10} pattern: $endToEndId")
              
              val debtorAccount = transaction.getAs[Row]("debtor_account")
              assert(debtorAccount != null, "debtor_account should not be null")
              
              val identification = debtorAccount.getAs[Row]("identification")
              assert(identification != null, "identification should not be null")
              
              val other = identification.getAs[Row]("other")
              assert(other != null, "other should not be null")
              
              val accountIdentification = other.getAs[String]("identification")
              assert(accountIdentification != null, "account identification should not be null")
              
              // This is the key test - the account identification should be based on the bicfi field
              // from the same payment_information array, but the SQL reference skips the 
              // direct_debit_transaction_information level
              if (bicfi == "CTBAAU2S123") {
                assert(accountIdentification == "06223622993847", s"account identification should be '06223622993847' for bicfi 'CTBAAU2S123', but was: $accountIdentification")
              } else {
                assert(accountIdentification == "12345612345678", s"account identification should be '12345612345678' for bicfi other than 'CTBAAU2S123', but was: $accountIdentification")
              }
            }
          }
        }
      }
    }
  }

  test("Can handle complex nested array SQL with multiple references") {
    val complexNestedArrayFields = List(
      Field("global_id", Some("string"), Map("regex" -> "GLOBAL[0-9]{8}")),
      Field("root_amount", Some("double"), Map("min" -> 10000.0, "max" -> 50000.0)),
      Field("organizations", Some("array"), fields = List(
        Field("org_id", Some("string"), Map("regex" -> "ORG[0-9]{8}")),
        Field("org_name", Some("string"), Map("expression" -> "#{Company.name}")),
        Field("org_budget", Some("double"), Map("min" -> 5000.0, "max" -> 20000.0)),
        Field("departments", Some("array"), fields = List(
          Field("dept_id", Some("string"), Map("regex" -> "DEPT[0-9]{8}")),
          Field("dept_name", Some("string"), Map("expression" -> "#{Commerce.department}")),
          Field("dept_budget", Some("double"), Map("min" -> 1000.0, "max" -> 5000.0)),
          Field("parent_org_id", Some("string"), Map("sql" -> "organizations.org_id")),
          Field("parent_org_name", Some("string"), Map("sql" -> "organizations.org_name")),
          Field("budget_percentage", Some("double"), Map("sql" -> "organizations.departments.dept_budget / organizations.org_budget * 100")),
          Field("employees", Some("array"), fields = List(
            Field("emp_id", Some("string"), Map("regex" -> "EMP[0-9]{8}")),
            Field("emp_name", Some("string"), Map("expression" -> "#{Name.name}")),
            Field("salary", Some("double"), Map("min" -> 50000.0, "max" -> 150000.0)),
            Field("global_ref", Some("string"), Map("sql" -> "global_id")),
            Field("org_ref", Some("string"), Map("sql" -> "organizations.org_id")),
            Field("dept_ref", Some("string"), Map("sql" -> "organizations.departments.dept_id")),
            Field("salary_vs_root_amount", Some("double"), Map("sql" -> "organizations.departments.employees.salary / root_amount * 100")),
            Field("salary_vs_org_budget", Some("double"), Map("sql" -> "organizations.departments.employees.salary / organizations.org_budget * 100")),
            Field("salary_vs_dept_budget", Some("double"), Map("sql" -> "organizations.departments.employees.salary / organizations.departments.dept_budget * 100"))
          ))
        ))
      ))
    )

    val step = Step("test_complex_nested_array", "json", fields = complexNestedArrayFields)
    val result = dataGeneratorFactory.generateDataForStep(step, "json", 0, 1)

    assert(result.columns.contains("global_id"))
    assert(result.columns.contains("root_amount"))
    assert(result.columns.contains("organizations"))

    val rows = result.collect()
    rows.foreach { row =>
      val globalId = row.getAs[String]("global_id")
      val rootAmount = row.getAs[Double]("root_amount")
      val organizations = row.getAs[Seq[Row]]("organizations")
      
      assert(globalId.matches("GLOBAL[0-9]{8}"), s"global_id should match GLOBAL[0-9]{8} pattern: $globalId")
      assert(rootAmount >= 10000.0 && rootAmount <= 50000.0, s"root_amount should be between 10000.0 and 50000.0: $rootAmount")
      assert(organizations != null, "organizations should not be null")
      
      if (organizations.nonEmpty) {
        organizations.foreach { org =>
          val orgId = org.getAs[String]("org_id")
          val orgName = org.getAs[String]("org_name")
          val orgBudget = org.getAs[Double]("org_budget")
          val departments = org.getAs[Seq[Row]]("departments")
          
          assert(orgId.matches("ORG[0-9]{8}"), s"org_id should match ORG[0-9]{8} pattern: $orgId")
          assert(orgName != null && orgName.nonEmpty, "org_name should not be null or empty")
          assert(orgBudget >= 5000.0 && orgBudget <= 20000.0, s"org_budget should be between 5000.0 and 20000.0: $orgBudget")
          assert(departments != null, "departments should not be null")
          
          if (departments.nonEmpty) {
            departments.foreach { dept =>
              val deptId = dept.getAs[String]("dept_id")
              val deptName = dept.getAs[String]("dept_name")
              val deptBudget = dept.getAs[Double]("dept_budget")
              val parentOrgId = dept.getAs[String]("parent_org_id")
              val parentOrgName = dept.getAs[String]("parent_org_name")
              val budgetPercentage = dept.getAs[Double]("budget_percentage")
              val employees = dept.getAs[Seq[Row]]("employees")
              
              assert(deptId.matches("DEPT[0-9]{8}"), s"dept_id should match DEPT[0-9]{8} pattern: $deptId")
              assert(deptName != null && deptName.nonEmpty, "dept_name should not be null or empty")
              assert(deptBudget >= 1000.0 && deptBudget <= 5000.0, s"dept_budget should be between 1000.0 and 5000.0: $deptBudget")
              assert(parentOrgId == orgId, s"parent_org_id should match org_id: $parentOrgId")
              assert(parentOrgName == orgName, s"parent_org_name should match org_name: $parentOrgName")
              assert(budgetPercentage == (deptBudget / orgBudget * 100), s"budget_percentage should be dept_budget / org_budget * 100: $budgetPercentage")
              assert(employees != null, "employees should not be null")
              
              if (employees.nonEmpty) {
                employees.foreach { emp =>
                  val empId = emp.getAs[String]("emp_id")
                  val empName = emp.getAs[String]("emp_name")
                  val salary = emp.getAs[Double]("salary")
                  val globalRef = emp.getAs[String]("global_ref")
                  val orgRef = emp.getAs[String]("org_ref")
                  val deptRef = emp.getAs[String]("dept_ref")
                  val salaryVsRootAmount = emp.getAs[Double]("salary_vs_root_amount")
                  val salaryVsOrgBudget = emp.getAs[Double]("salary_vs_org_budget")
                  val salaryVsDeptBudget = emp.getAs[Double]("salary_vs_dept_budget")
                  
                  assert(empId.matches("EMP[0-9]{8}"), s"emp_id should match EMP[0-9]{8} pattern: $empId")
                  assert(empName != null && empName.nonEmpty, "emp_name should not be null or empty")
                  assert(salary >= 50000.0 && salary <= 150000.0, s"salary should be between 50000.0 and 150000.0: $salary")
                  assert(globalRef == globalId, s"global_ref should match global_id: $globalRef")
                  assert(orgRef == orgId, s"org_ref should match org_id: $orgRef")
                  assert(deptRef == deptId, s"dept_ref should match dept_id: $deptRef")
                  assert(salaryVsRootAmount == (salary / rootAmount * 100), s"salary_vs_root_amount should be salary / root_amount * 100: $salaryVsRootAmount")
                  assert(salaryVsOrgBudget == (salary / orgBudget * 100), s"salary_vs_org_budget should be salary / org_budget * 100: $salaryVsOrgBudget")
                  assert(salaryVsDeptBudget == (salary / deptBudget * 100), s"salary_vs_dept_budget should be salary / dept_budget * 100: $salaryVsDeptBudget")
                }
              }
            }
          }
        }
      }
    }
  }

  // ========== END NESTED ARRAY SQL REFERENCE TESTS ==========

  // ========== SQL AGGREGATES FOR DATA GENERATION TESTS ==========
  
  test("Can use SQL aggregates for realistic data generation - Running totals") {
    val runningTotalFields = List(
      Field("order_id", Some("string"), Map("regex" -> "ORD[0-9]{6}")),
      Field("customer_id", Some("string"), Map("regex" -> "CUST[0-9]{4}")),
      Field("item_amount", Some("double"), Map("min" -> 10.0, "max" -> 1000.0)),
      Field("running_total", Some("double"), Map("sql" -> "SUM(item_amount) OVER (PARTITION BY customer_id ORDER BY order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")),
      Field("order_rank", Some("int"), Map("sql" -> "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY item_amount DESC)")),
      Field("avg_customer_order", Some("double"), Map("sql" -> "AVG(item_amount) OVER (PARTITION BY customer_id)"))
    )
    
    val step = Step("orders", "parquet", Count(records = Some(10)), Map("path" -> "sample/output/parquet/orders"), runningTotalFields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()
    
    assertResult(10L)(df.count())
    val rows = df.collect()
    
    // Verify running totals are calculated correctly
    val customerGroups = rows.groupBy(_.getAs[String]("customer_id"))
    customerGroups.foreach { case (customerId, customerRows) =>
      val sortedRows = customerRows.sortBy(_.getAs[String]("order_id"))
      var runningSum = 0.0
      
      sortedRows.foreach { row =>
        val itemAmount = row.getAs[Double]("item_amount")
        runningSum += itemAmount
        val calculatedRunningTotal = row.getAs[Double]("running_total")
        
        assert(math.abs(calculatedRunningTotal - runningSum) < 0.001, 
          s"Running total mismatch for customer $customerId: expected $runningSum, got $calculatedRunningTotal")
      }
    }
    
    df.unpersist()
  }
  
  test("Can use SQL aggregates for data distribution - Percentiles and quartiles") {
    val distributionFields = List(
      Field("product_id", Some("string"), Map("regex" -> "PROD[0-9]{4}")),
      Field("sales_amount", Some("double"), Map("min" -> 1000.0, "max" -> 50000.0)),
      Field("sales_percentile", Some("double"), Map("sql" -> "PERCENT_RANK() OVER (ORDER BY sales_amount)")),
      Field("sales_quartile", Some("int"), Map("sql" -> "NTILE(4) OVER (ORDER BY sales_amount)")),
      // Use supported PERCENT_RANK instead of unavailable PERCENTILE_CONT
      Field("sales_category", Some("string"), Map("sql" -> "CASE WHEN PERCENT_RANK() OVER (ORDER BY sales_amount) > 0.75 THEN 'HIGH' WHEN PERCENT_RANK() OVER (ORDER BY sales_amount) > 0.5 THEN 'MEDIUM' ELSE 'LOW' END"))
    )
    
    val step = Step("sales", "parquet", Count(records = Some(20)), Map("path" -> "sample/output/parquet/sales"), distributionFields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 20)
    df.cache()
    
    assertResult(20L)(df.count())
    val rows = df.collect()
    
    // Verify percentiles are between 0 and 1
    rows.foreach { row =>
      val percentile = row.getAs[Double]("sales_percentile")
      assert(percentile >= 0.0 && percentile <= 1.0, s"Percentile should be between 0 and 1: $percentile")
    }
    
    // Verify quartiles are 1, 2, 3, or 4
    rows.foreach { row =>
      val quartile = row.getAs[Int]("sales_quartile")
      assert(quartile >= 1 && quartile <= 4, s"Quartile should be between 1 and 4: $quartile")
    }
    
    // Verify categories are correctly assigned
    val categories = rows.map(_.getAs[String]("sales_category")).distinct
    assert(categories.toSet.subsetOf(Set("HIGH", "MEDIUM", "LOW")), 
      s"Categories should be HIGH, MEDIUM, or LOW: ${categories.mkString(", ")}")
    
    df.unpersist()
  }
  
  test("Can use SQL aggregates for time series data generation") {
    val timeSeriesFields = List(
      Field("timestamp", Some("string"), Map("sql" -> "DATE_ADD('2023-01-01', CAST(__index_inc AS INT))")),
      Field("sensor_value", Some("double"), Map("min" -> 20.0, "max" -> 80.0)),
      Field("moving_avg_3", Some("double"), Map("sql" -> "AVG(sensor_value) OVER (ORDER BY timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)")),
      Field("moving_avg_7", Some("double"), Map("sql" -> "AVG(sensor_value) OVER (ORDER BY timestamp ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)")),
      Field("daily_diff", Some("double"), Map("sql" -> "sensor_value - LAG(sensor_value, 1, sensor_value) OVER (ORDER BY timestamp)")),
      Field("trend_direction", Some("string"), Map("sql" -> "CASE WHEN sensor_value > LAG(sensor_value, 1, sensor_value) OVER (ORDER BY timestamp) THEN 'UP' WHEN sensor_value < LAG(sensor_value, 1, sensor_value) OVER (ORDER BY timestamp) THEN 'DOWN' ELSE 'FLAT' END"))
    )
    
    val step = Step("timeseries", "parquet", Count(records = Some(15)), Map("path" -> "sample/output/parquet/timeseries"), timeSeriesFields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 15)
    df.cache()
    
    assertResult(15L)(df.count())
    val rows = df.collect()
    
    // Verify moving averages are calculated correctly
    val sortedRows = rows.sortBy(_.getAs[String]("timestamp"))
    sortedRows.zipWithIndex.foreach { case (row, index) =>
      val movingAvg3 = row.getAs[Double]("moving_avg_3")
      val movingAvg7 = row.getAs[Double]("moving_avg_7")
      
      assert(movingAvg3 >= 20.0 && movingAvg3 <= 80.0, s"Moving average 3 should be within sensor value range: $movingAvg3")
      assert(movingAvg7 >= 20.0 && movingAvg7 <= 80.0, s"Moving average 7 should be within sensor value range: $movingAvg7")
    }
    
    // Verify trend directions are valid
    val trendDirections = rows.map(_.getAs[String]("trend_direction")).distinct
    assert(trendDirections.toSet.subsetOf(Set("UP", "DOWN", "FLAT")), 
      s"Trend directions should be UP, DOWN, or FLAT: ${trendDirections.mkString(", ")}")
    
    df.unpersist()
  }
  
  test("Can use SQL aggregates for complex business logic - Order fulfillment") {
    val orderFulfillmentFields = List(
      Field("order_id", Some("string"), Map("regex" -> "ORD[0-9]{6}")),
      Field("customer_segment", Some("string"), Map("oneOf" -> Array("PREMIUM", "STANDARD", "BASIC"))),
      Field("order_amount", Some("double"), Map("min" -> 100.0, "max" -> 5000.0)),
      Field("base_shipping_days", Some("int"), Map("min" -> 3, "max" -> 10)),
      Field("customer_lifetime_value", Some("double"), Map("sql" -> "CASE WHEN customer_segment = 'PREMIUM' THEN 10000 + RAND() * 40000 WHEN customer_segment = 'STANDARD' THEN 5000 + RAND() * 20000 ELSE 1000 + RAND() * 10000 END")),
      Field("priority_score", Some("double"), Map("sql" -> "PERCENT_RANK() OVER (ORDER BY customer_lifetime_value DESC) * 0.4 + PERCENT_RANK() OVER (ORDER BY order_amount DESC) * 0.6")),
      Field("expedited_shipping", Some("boolean"), Map("sql" -> "priority_score > 0.75")),
      Field("estimated_delivery_days", Some("int"), Map("sql" -> "CASE WHEN expedited_shipping THEN GREATEST(1, base_shipping_days - 2) ELSE base_shipping_days END")),
      Field("shipping_cost", Some("double"), Map("sql" -> "CASE WHEN expedited_shipping THEN order_amount * 0.05 + 25 ELSE order_amount * 0.02 + 10 END"))
    )
    
    val step = Step("fulfillment", "parquet", Count(records = Some(50)), Map("path" -> "sample/output/parquet/fulfillment"), orderFulfillmentFields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 50)
    df.cache()
    
    assertResult(50L)(df.count())
    val rows = df.collect()
    
    // Verify business logic is applied correctly
    rows.foreach { row =>
      val customerSegment = row.getAs[String]("customer_segment")
      val customerLifetimeValue = row.getAs[Double]("customer_lifetime_value")
      val priorityScore = row.getAs[Double]("priority_score")
      val expeditedShipping = row.getAs[Boolean]("expedited_shipping")
      val estimatedDeliveryDays = row.getAs[Int]("estimated_delivery_days")
      val baseShippingDays = row.getAs[Int]("base_shipping_days")
      val orderAmount = row.getAs[Double]("order_amount")
      val shippingCost = row.getAs[Double]("shipping_cost")
      
      // Verify customer lifetime value ranges based on segment
      customerSegment match {
        case "PREMIUM" => assert(customerLifetimeValue >= 10000 && customerLifetimeValue <= 50000, 
          s"Premium customer lifetime value should be between 10000 and 50000: $customerLifetimeValue")
        case "STANDARD" => assert(customerLifetimeValue >= 5000 && customerLifetimeValue <= 25000, 
          s"Standard customer lifetime value should be between 5000 and 25000: $customerLifetimeValue")
        case "BASIC" => assert(customerLifetimeValue >= 1000 && customerLifetimeValue <= 11000, 
          s"Basic customer lifetime value should be between 1000 and 11000: $customerLifetimeValue")
      }
      
      // Verify priority score is between 0 and 1
      assert(priorityScore >= 0.0 && priorityScore <= 1.0, s"Priority score should be between 0 and 1: $priorityScore")
      
      // Verify expedited shipping logic
      if (expeditedShipping) {
        assert(priorityScore > 0.75, s"Expedited shipping should only be true for priority score > 0.75: $priorityScore")
        assert(estimatedDeliveryDays >= 1 && estimatedDeliveryDays <= baseShippingDays - 2, 
          s"Expedited delivery should be faster: estimated=$estimatedDeliveryDays, base=$baseShippingDays")
      } else {
        assert(estimatedDeliveryDays == baseShippingDays, 
          s"Regular shipping should match base shipping days: estimated=$estimatedDeliveryDays, base=$baseShippingDays")
      }
      
      // Verify shipping cost calculation
      val expectedShippingCost = if (expeditedShipping) orderAmount * 0.05 + 25 else orderAmount * 0.02 + 10
      assert(math.abs(shippingCost - expectedShippingCost) < 0.001, 
        s"Shipping cost calculation mismatch: expected $expectedShippingCost, got $shippingCost")
    }
    
    df.unpersist()
  }
  
  test("Can use SQL aggregates for financial data generation - Portfolio analysis") {
    val portfolioFields = List(
      Field("security_id", Some("string"), Map("regex" -> "SEC[0-9]{4}")),
      Field("sector", Some("string"), Map("oneOf" -> Array("TECH", "FINANCE", "HEALTHCARE", "ENERGY", "CONSUMER"))),
      Field("market_cap", Some("double"), Map("min" -> 1000000000.0, "max" -> 1000000000000.0)), // 1B to 1T
      Field("daily_return", Some("double"), Map("min" -> -0.15, "max" -> 0.15)), // -15% to +15%
      Field("sector_avg_return", Some("double"), Map("sql" -> "AVG(daily_return) OVER (PARTITION BY sector)")),
      Field("sector_volatility", Some("double"), Map("sql" -> "STDDEV(daily_return) OVER (PARTITION BY sector)")),
      Field("relative_performance", Some("double"), Map("sql" -> "daily_return - sector_avg_return")),
      Field("market_cap_percentile", Some("double"), Map("sql" -> "PERCENT_RANK() OVER (ORDER BY market_cap)")),
      Field("risk_category", Some("string"), Map("sql" -> "CASE WHEN sector_volatility > 0.08 THEN 'HIGH_RISK' WHEN sector_volatility > 0.05 THEN 'MEDIUM_RISK' ELSE 'LOW_RISK' END")),
      Field("investment_grade", Some("string"), Map("sql" -> "CASE WHEN market_cap_percentile > 0.8 AND relative_performance > 0 THEN 'A' WHEN market_cap_percentile > 0.6 THEN 'B' WHEN market_cap_percentile > 0.4 THEN 'C' ELSE 'D' END"))
    )
    
    val step = Step("portfolio", "parquet", Count(records = Some(100)), Map("path" -> "sample/output/parquet/portfolio"), portfolioFields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 100)
    df.cache()
    
    assertResult(100L)(df.count())
    val rows = df.collect()
    
    // Verify sector-based calculations
    val sectorGroups = rows.groupBy(_.getAs[String]("sector"))
    sectorGroups.foreach { case (sector, sectorRows) =>
      val sectorReturns = sectorRows.map(_.getAs[Double]("daily_return"))
      val expectedSectorAvg = sectorReturns.sum / sectorReturns.length
      
      sectorRows.foreach { row =>
        val sectorAvgReturn = row.getAs[Double]("sector_avg_return")
        val relativePerformance = row.getAs[Double]("relative_performance")
        val dailyReturn = row.getAs[Double]("daily_return")
        
        assert(math.abs(sectorAvgReturn - expectedSectorAvg) < 0.001, 
          s"Sector average return mismatch for $sector: expected $expectedSectorAvg, got $sectorAvgReturn")
        assert(math.abs(relativePerformance - (dailyReturn - sectorAvgReturn)) < 0.001, 
          s"Relative performance calculation error: expected ${dailyReturn - sectorAvgReturn}, got $relativePerformance")
      }
    }
    
    // Verify risk categories and investment grades are valid
    val riskCategories = rows.map(_.getAs[String]("risk_category")).distinct
    assert(riskCategories.toSet.subsetOf(Set("HIGH_RISK", "MEDIUM_RISK", "LOW_RISK")), 
      s"Risk categories should be HIGH_RISK, MEDIUM_RISK, or LOW_RISK: ${riskCategories.mkString(", ")}")
    
    val investmentGrades = rows.map(_.getAs[String]("investment_grade")).distinct
    assert(investmentGrades.toSet.subsetOf(Set("A", "B", "C", "D")), 
      s"Investment grades should be A, B, C, or D: ${investmentGrades.mkString(", ")}")
    
    df.unpersist()
  }
  
  test("Can use SQL aggregates for hierarchical data generation - Organizational structure") {
    val orgStructureFields = List(
      Field("employee_id", Some("string"), Map("regex" -> "EMP[0-9]{4}")),
      Field("department", Some("string"), Map("oneOf" -> Array("SALES", "ENGINEERING", "MARKETING", "FINANCE", "HR"))),
      Field("level", Some("int"), Map("min" -> 1, "max" -> 5)),
      Field("salary", Some("double"), Map("min" -> 50000.0, "max" -> 200000.0)),
      Field("dept_avg_salary", Some("double"), Map("sql" -> "AVG(salary) OVER (PARTITION BY department)")),
      Field("dept_salary_rank", Some("int"), Map("sql" -> "RANK() OVER (PARTITION BY department ORDER BY salary DESC)")),
      Field("company_salary_percentile", Some("double"), Map("sql" -> "PERCENT_RANK() OVER (ORDER BY salary)")),
      Field("salary_vs_dept_avg", Some("double"), Map("sql" -> "salary - dept_avg_salary")),
      Field("is_top_performer", Some("boolean"), Map("sql" -> "dept_salary_rank <= 3")),
      Field("compensation_band", Some("string"), Map("sql" -> "CASE WHEN company_salary_percentile > 0.9 THEN 'EXECUTIVE' WHEN company_salary_percentile > 0.75 THEN 'SENIOR' WHEN company_salary_percentile > 0.5 THEN 'MID' ELSE 'JUNIOR' END"))
    )
    
    val step = Step("organization", "parquet", Count(records = Some(50)), Map("path" -> "sample/output/parquet/organization"), orgStructureFields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 50)
    df.cache()
    
    assertResult(50L)(df.count())
    val rows = df.collect()
    
    // Verify department-based calculations
    val deptGroups = rows.groupBy(_.getAs[String]("department"))
    deptGroups.foreach { case (department, deptRows) =>
      val deptSalaries = deptRows.map(_.getAs[Double]("salary"))
      val expectedDeptAvg = deptSalaries.sum / deptSalaries.length
      
      deptRows.foreach { row =>
        val deptAvgSalary = row.getAs[Double]("dept_avg_salary")
        val salaryVsDeptAvg = row.getAs[Double]("salary_vs_dept_avg")
        val salary = row.getAs[Double]("salary")
        
        assert(math.abs(deptAvgSalary - expectedDeptAvg) < 0.001, 
          s"Department average salary mismatch for $department: expected $expectedDeptAvg, got $deptAvgSalary")
        assert(math.abs(salaryVsDeptAvg - (salary - deptAvgSalary)) < 0.001, 
          s"Salary vs department average calculation error: expected ${salary - deptAvgSalary}, got $salaryVsDeptAvg")
      }
      
      // Verify top performers (should be at most 3 per department)
      val topPerformers = deptRows.filter(_.getAs[Boolean]("is_top_performer"))
      assert(topPerformers.length <= 3, 
        s"Department $department should have at most 3 top performers, got ${topPerformers.length}")
    }
    
    // Verify compensation bands are valid
    val compensationBands = rows.map(_.getAs[String]("compensation_band")).distinct
    assert(compensationBands.toSet.subsetOf(Set("EXECUTIVE", "SENIOR", "MID", "JUNIOR")), 
      s"Compensation bands should be EXECUTIVE, SENIOR, MID, or JUNIOR: ${compensationBands.mkString(", ")}")
    
    df.unpersist()
  }

  // ========== END SQL AGGREGATES TESTS ==========

}
