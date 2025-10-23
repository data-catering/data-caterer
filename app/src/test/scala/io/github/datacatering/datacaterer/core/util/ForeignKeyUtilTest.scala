package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants.FOREIGN_KEY_DELIMITER
import io.github.datacatering.datacaterer.api.model.{ForeignKey, ForeignKeyRelation, Plan, SinkOptions, TaskSummary}
import io.github.datacatering.datacaterer.core.exception.MissingDataSourceFromForeignKeyException
import io.github.datacatering.datacaterer.core.model.{ForeignKeyRelationship, ForeignKeyWithGenerateAndDelete}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.Date
import java.time.LocalDate

class ForeignKeyUtilTest extends SparkSuite {

  test("When no foreign keys defined, return back same dataframes") {
    val sinkOptions = SinkOptions(None, None, List())
    val plan = Plan("no foreign keys", "simple plan", List(), Some(sinkOptions))
    val dfMap = List("name" -> sparkSession.emptyDataFrame)

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, dfMap)

    assertResult(result)(dfMap)
  }

  test("Can get insert order") {
    val foreignKeys = List(
      "orders" -> List("customers"),
      "order_items" -> List("orders", "products"),
      "reviews" -> List("products", "customers")
    )
    val result = ForeignKeyUtil.getInsertOrder(foreignKeys)

    result should contain theSameElementsInOrderAs List("order_items", "reviews", "orders", "products", "customers")
  }

  test("Can get insert order with multiple foreign keys") {
    val foreignKeys = List(
      "products" -> List("customers", "prices", "orders"),
      "customers" -> List("addresses")
    )
    val result = ForeignKeyUtil.getInsertOrder(foreignKeys)

    result should contain theSameElementsInOrderAs List("products", "customers", "prices", "orders", "addresses")
  }

  test("Can get insert order when multiple generations are defined") {
    val foreignKeys = List(
      "products" -> List("customers", "prices", "orders"),
    )
    val result = ForeignKeyUtil.getInsertOrder(foreignKeys)

    result should contain theSameElementsInOrderAs List("products", "customers", "prices", "orders")
  }

  test("Can link foreign keys between data sets") {
    val sinkOptions = SinkOptions(None, None,
      List(ForeignKey(ForeignKeyRelation("postgres", "account", List("account_id")),
        List(ForeignKeyRelation("postgres", "transaction", List("account_id"))), List()))
    )
    val plan = Plan("foreign keys", "simple plan", List(), Some(sinkOptions))
    val accountsList = List(
      Account("acc1", "peter", Date.valueOf(LocalDate.now())),
      Account("acc2", "john", Date.valueOf(LocalDate.now())),
      Account("acc3", "jack", Date.valueOf(LocalDate.now()))
    )
    val transactionList = List(
      Transaction("some_acc9", "rand1", "id123", Date.valueOf(LocalDate.now()), 10.0),
      Transaction("some_acc9", "rand2", "id124", Date.valueOf(LocalDate.now()), 23.9),
      Transaction("some_acc10", "rand3", "id125", Date.valueOf(LocalDate.now()), 85.1),
    )
    val dfMap = List(
      "postgres.account" -> sparkSession.createDataFrame(accountsList),
      "postgres.transaction" -> sparkSession.createDataFrame(transactionList)
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, dfMap)
    val txn = result.filter(f => f._1.equalsIgnoreCase("postgres.transaction")).head._2
    val resTxnRows = txn.collect()
    resTxnRows.foreach(r => {
      r.getString(0) == "acc1" || r.getString(0) == "acc2" || r.getString(0) == "acc3"
    })
  }

  test("Can link foreign keys between data sets with multiple fields") {
    val sinkOptions = SinkOptions(None, None,
      List(ForeignKey(ForeignKeyRelation("postgres", "account", List("account_id", "name")),
        List(ForeignKeyRelation("postgres", "transaction", List("account_id", "name"))), List()))
    )
    val plan = Plan("foreign keys", "simple plan", List(TaskSummary("my_task", "postgres")), Some(sinkOptions))
    val accountsList = List(
      Account("acc1", "peter", Date.valueOf(LocalDate.now())),
      Account("acc2", "john", Date.valueOf(LocalDate.now())),
      Account("acc3", "jack", Date.valueOf(LocalDate.now()))
    )
    val transactionList = List(
      Transaction("some_acc9", "rand1", "id123", Date.valueOf(LocalDate.now()), 10.0),
      Transaction("some_acc9", "rand1", "id124", Date.valueOf(LocalDate.now()), 12.0),
      Transaction("some_acc9", "rand2", "id125", Date.valueOf(LocalDate.now()), 23.9),
      Transaction("some_acc10", "rand3", "id126", Date.valueOf(LocalDate.now()), 85.1),
    )
    val dfMap = List(
      "postgres.account" -> sparkSession.createDataFrame(accountsList),
      "postgres.transaction" -> sparkSession.createDataFrame(transactionList)
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, dfMap)
    val txn = result.filter(f => f._1.equalsIgnoreCase("postgres.transaction")).head._2
    val resTxnRows = txn.collect()
    val acc1 = resTxnRows.find(_.getString(0).equalsIgnoreCase("acc1"))
    assert(acc1.isDefined)
    assert(acc1.get.getString(1).equalsIgnoreCase("peter"))
    val acc2 = resTxnRows.find(_.getString(0).equalsIgnoreCase("acc2"))
    assert(acc2.isDefined)
    assert(acc2.get.getString(1).equalsIgnoreCase("john"))
    val acc3 = resTxnRows.find(_.getString(0).equalsIgnoreCase("acc3"))
    assert(acc3.isDefined)
    assert(acc3.get.getString(1).equalsIgnoreCase("jack"))
    val acc1Count = resTxnRows.count(_.getString(0).equalsIgnoreCase("acc1"))
    val acc2Count = resTxnRows.count(_.getString(0).equalsIgnoreCase("acc2"))
    val acc3Count = resTxnRows.count(_.getString(0).equalsIgnoreCase("acc3"))
    assert(acc1Count == 2 || acc2Count == 2 || acc3Count == 2)
  }

  test("Can link foreign keys between data sets with multiple records per field") {
    val sinkOptions = SinkOptions(None, None,
      List(ForeignKey(ForeignKeyRelation("postgres", "account", List("account_id")),
        List(ForeignKeyRelation("postgres", "transaction", List("account_id"))), List()))
    )
    val plan = Plan("foreign keys", "simple plan", List(TaskSummary("my_task", "postgres")), Some(sinkOptions))
    val accountsList = List(
      Account("acc1", "peter", Date.valueOf(LocalDate.now())),
      Account("acc2", "john", Date.valueOf(LocalDate.now())),
      Account("acc3", "jack", Date.valueOf(LocalDate.now()))
    )
    val transactionList = List(
      Transaction("some_acc9", "rand1", "id123", Date.valueOf(LocalDate.now()), 10.0),
      Transaction("some_acc9", "rand1", "id124", Date.valueOf(LocalDate.now()), 12.0),
      Transaction("some_acc9", "rand2", "id125", Date.valueOf(LocalDate.now()), 23.9),
      Transaction("some_acc10", "rand3", "id126", Date.valueOf(LocalDate.now()), 85.1),
      Transaction("some_acc10", "rand3", "id127", Date.valueOf(LocalDate.now()), 72.1),
      Transaction("some_acc11", "rand3", "id128", Date.valueOf(LocalDate.now()), 5.9)
    )
    val dfMap = List(
      "postgres.account" -> sparkSession.createDataFrame(accountsList),
      "postgres.transaction" -> sparkSession.createDataFrame(transactionList)
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, dfMap)
    val txn = result.filter(f => f._1.equalsIgnoreCase("postgres.transaction")).head._2
    txn.show(false)
    val resTxnRows = txn.collect()
    val acc1 = resTxnRows.find(_.getString(0).equalsIgnoreCase("acc1"))
    assert(acc1.isDefined)
    val acc2 = resTxnRows.find(_.getString(0).equalsIgnoreCase("acc2"))
    assert(acc2.isDefined)
    val acc3 = resTxnRows.find(_.getString(0).equalsIgnoreCase("acc3"))
    assert(acc3.isDefined)
    val acc1Count = resTxnRows.count(_.getString(0).equalsIgnoreCase("acc1"))
    val acc2Count = resTxnRows.count(_.getString(0).equalsIgnoreCase("acc2"))
    val acc3Count = resTxnRows.count(_.getString(0).equalsIgnoreCase("acc3"))
    assert(acc1Count == 3 || acc2Count == 3 || acc3Count == 3)
    assert(acc1Count == 2 || acc2Count == 2 || acc3Count == 2)
    assert(acc1Count == 1 || acc2Count == 1 || acc3Count == 1)
  }

  test("Can get delete order based on foreign keys defined") {
    val foreignKeys = List(
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" ->
        List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id", s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id")
    )
    val deleteOrder = ForeignKeyUtil.getDeleteOrder(foreignKeys)
    assert(deleteOrder ==
      List(
        s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id",
        s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id",
        s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id"
      )
    )
  }

  test("Can get delete order based on nested foreign keys") {
    val foreignKeys = List(
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id"),
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id"),
    )
    val deleteOrder = ForeignKeyUtil.getDeleteOrder(foreignKeys)
    val expected = List(
      s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id"
    )
    assertResult(expected)(deleteOrder)

    val foreignKeys1 = List(
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id"),
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id"),
    )
    val deleteOrder1 = ForeignKeyUtil.getDeleteOrder(foreignKeys1)
    assertResult(expected)(deleteOrder1)

    val foreignKeys2 = List(
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id"),
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id"),
      s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}customer${FOREIGN_KEY_DELIMITER}account_id"),
    )
    val deleteOrder2 = ForeignKeyUtil.getDeleteOrder(foreignKeys2)
    val expected2 = List(s"postgres${FOREIGN_KEY_DELIMITER}customer${FOREIGN_KEY_DELIMITER}account_id") ++ expected
    assertResult(expected2)(deleteOrder2)
  }

  test("Can generate correct values when per field count is defined over multiple fields that are also defined as foreign keys") {
    val foreignKeys = List(
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" ->
        List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id", s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id")
    )
    val deleteOrder = ForeignKeyUtil.getDeleteOrder(foreignKeys)
    assert(deleteOrder == List(
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id")
    )
  }

  test("Can generate correct values when primary keys are defined over multiple fields that are also defined as foreign keys") {
    val foreignKeys = List(
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" ->
        List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id", s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id")
    )
    val deleteOrder = ForeignKeyUtil.getDeleteOrder(foreignKeys)
    assert(deleteOrder == List(
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id")
    )
  }

  test("Can update foreign keys with updated names from metadata") {
    implicit val encoder = Encoders.kryo[ForeignKeyRelationship]
    val generatedForeignKeys = List(sparkSession.createDataset(Seq(ForeignKeyRelationship(
      ForeignKeyRelation("my_postgres", "public.account", List("account_id")),
      ForeignKeyRelation("my_postgres", "public.orders", List("customer_id")),
    ))))
    val optPlanRun = Some(new ForeignKeyPlanRun())
    val stepNameMapping = Map(
      s"my_csv${FOREIGN_KEY_DELIMITER}random_step" -> s"my_csv${FOREIGN_KEY_DELIMITER}public.accounts"
    )

    val result = ForeignKeyUtil.getAllForeignKeyRelationships(generatedForeignKeys, optPlanRun, stepNameMapping)

    assertResult(3)(result.size)
    assert(result.contains(
      ForeignKey(
        ForeignKeyRelation("my_csv", "public.accounts", List("id")),
        List(ForeignKeyRelation("my_postgres", "public.accounts", List("account_id"))),
        List()
      )
    ))
    assert(result.contains(
      ForeignKey(
        ForeignKeyRelation("my_json", "json_step", List("id")),
        List(ForeignKeyRelation("my_postgres", "public.orders", List("customer_id"))),
        List()
      )
    ))
    assert(result.contains(
      ForeignKey(
        ForeignKeyRelation("my_postgres", "public.account", List("account_id")),
        List(ForeignKeyRelation("my_postgres", "public.orders", List("customer_id"))),
        List()
      )
    ))
  }

  test("Can link foreign keys with nested fields") {
    import org.apache.spark.sql.types._

    val sinkOptions = SinkOptions(None, None,
      List(ForeignKey(ForeignKeyRelation("reference", "people", List("name", "email")),
        List(ForeignKeyRelation("target", "users", List("profile.name", "profile.email"))), List()))
    )
    val plan = Plan("nested foreign keys", "nested plan", List(
      TaskSummary("ref_task", "reference"), 
      TaskSummary("target_task", "target")
    ), Some(sinkOptions))

    // Create reference data with name-email pairs
    val referenceData = Seq(
      ("John Doe", "john.doe@example.com"),
      ("Jane Smith", "jane.smith@example.com"),
      ("Bob Johnson", "bob.johnson@example.com")
    )
    val referenceDf = sparkSession.createDataFrame(referenceData).toDF("name", "email")

    // Create target data with nested structure using explicit schema
    val targetSchema = StructType(Array(
      StructField("id", StringType, nullable = false),
      StructField("profile", StructType(Array(
        StructField("name", StringType, nullable = true),
        StructField("email", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true)
      )), nullable = true)
    ))

    import org.apache.spark.sql.Row
    val targetRows = Seq(
      Row("user1", Row("unknown_name", "unknown@email.com", 25)),
      Row("user2", Row("unknown_name2", "unknown2@email.com", 30)),
      Row("user3", Row("unknown_name3", "unknown3@email.com", 35))
    )
    val targetDf = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(targetRows), targetSchema)

    val dfMap = List(
      "reference.people" -> referenceDf,
      "target.users" -> targetDf
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, dfMap)
    val updatedTargetDf = result.filter(f => f._1.equalsIgnoreCase("target.users")).head._2
    val resultRows = updatedTargetDf.collect()

    // Verify that name-email combinations come from reference data
    resultRows.foreach { row =>
      val profileStruct = row.getAs[org.apache.spark.sql.Row]("profile")
      val name = profileStruct.getAs[String]("name")
      val email = profileStruct.getAs[String]("email")
      val age = profileStruct.getAs[Int]("age")

      // Check that name-email combination exists in reference data
      val isValidCombination = referenceData.exists { case (refName, refEmail) =>
        refName == name && refEmail == email
      }
      assert(isValidCombination, s"Name-email combination ($name, $email) should exist in reference data")
      
      // Age should remain unchanged (not part of foreign key)
      assert(List(25, 30, 35).contains(age), s"Age should remain unchanged, got: $age")
    }

    // Verify that only original schema fields are present (no flattened fields)
    val finalColumnNames = updatedTargetDf.columns.toSet
    val expectedColumnNames = Set("id", "profile")
    assert(finalColumnNames == expectedColumnNames, 
      s"Final columns should only contain original fields. Expected: $expectedColumnNames, Got: $finalColumnNames")
  }

  test("Can link foreign keys with single nested field") {
    import org.apache.spark.sql.types._

    val sinkOptions = SinkOptions(None, None,
      List(ForeignKey(ForeignKeyRelation("reference", "companies", List("company_name")),
        List(ForeignKeyRelation("target", "employees", List("employment.company"))), List()))
    )
    val plan = Plan("single nested foreign key", "single nested plan", List(
      TaskSummary("ref_task", "reference"), 
      TaskSummary("target_task", "target")
    ), Some(sinkOptions))

    // Create reference data
    val referenceData = Seq("ACME Corp", "TechStart Inc", "DataCorp Ltd")
    val referenceDf = sparkSession.createDataFrame(referenceData.map(Tuple1(_))).toDF("company_name")

    // Create target data with nested structure using explicit schema
    val targetSchema = StructType(Array(
      StructField("id", StringType, nullable = false),
      StructField("employment", StructType(Array(
        StructField("company", StringType, nullable = true),
        StructField("role", StringType, nullable = true),
        StructField("salary", IntegerType, nullable = true)
      )), nullable = true)
    ))

    import org.apache.spark.sql.Row
    val targetRows = Seq(
      Row("emp1", Row("unknown_company", "developer", 50000)),
      Row("emp2", Row("unknown_company2", "manager", 75000))
    )
    val targetDf = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(targetRows), targetSchema)

    val dfMap = List(
      "reference.companies" -> referenceDf,
      "target.employees" -> targetDf
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, dfMap)
    val updatedTargetDf = result.filter(f => f._1.equalsIgnoreCase("target.employees")).head._2
    val resultRows = updatedTargetDf.collect()

    // Verify that company names come from reference data
    resultRows.foreach { row =>
      val employmentStruct = row.getAs[org.apache.spark.sql.Row]("employment")
      val company = employmentStruct.getAs[String]("company")
      
      assert(referenceData.contains(company), s"Company name '$company' should exist in reference data: ${referenceData.mkString(", ")}")
    }
  }

  test("Can link foreign keys with mixed flat and nested fields") {
    import org.apache.spark.sql.types._

    val sinkOptions = SinkOptions(None, None,
      List(ForeignKey(ForeignKeyRelation("reference", "customers", List("customer_id", "name", "email")),
        List(ForeignKeyRelation("target", "orders", List("customer_id", "shipping.name", "shipping.email"))), List()))
    )
    val plan = Plan("mixed foreign keys", "mixed plan", List(
      TaskSummary("ref_task", "reference"), 
      TaskSummary("target_task", "target")
    ), Some(sinkOptions))

    // Create reference data
    val referenceData = Seq(
      ("CUST001", "Alice Johnson", "alice.johnson@example.com"),
      ("CUST002", "Bob Wilson", "bob.wilson@example.com"),
      ("CUST003", "Carol Davis", "carol.davis@example.com")
    )
    val referenceDf = sparkSession.createDataFrame(referenceData).toDF("customer_id", "name", "email")

    // Create target data with mixed flat and nested structure using explicit schema
    val targetSchema = StructType(Array(
      StructField("order_id", StringType, nullable = false),
      StructField("customer_id", StringType, nullable = true),
      StructField("shipping", StructType(Array(
        StructField("name", StringType, nullable = true),
        StructField("email", StringType, nullable = true),
        StructField("address", StringType, nullable = true)
      )), nullable = true)
    ))

    import org.apache.spark.sql.Row
    val targetRows = Seq(
      Row("ORD001", "unknown_cust", Row("unknown_name", "unknown@email.com", "123 Main St")),
      Row("ORD002", "unknown_cust2", Row("unknown_name2", "unknown2@email.com", "456 Oak Ave"))
    )
    val targetDf = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(targetRows), targetSchema)

    val dfMap = List(
      "reference.customers" -> referenceDf,
      "target.orders" -> targetDf
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, dfMap)
    val updatedTargetDf = result.filter(f => f._1.equalsIgnoreCase("target.orders")).head._2
    val resultRows = updatedTargetDf.collect()

    // Verify that customer_id, name, and email all come from the same reference row
    resultRows.foreach { row =>
      val customerId = row.getAs[String]("customer_id")
      val shippingStruct = row.getAs[org.apache.spark.sql.Row]("shipping")
      val shippingName = shippingStruct.getAs[String]("name")
      val shippingEmail = shippingStruct.getAs[String]("email")
      val address = shippingStruct.getAs[String]("address")

      // Find the matching reference record
      val matchingRef = referenceData.find(_._1 == customerId)
      assert(matchingRef.isDefined, s"Customer ID '$customerId' should exist in reference data")

      val (_, refName, refEmail) = matchingRef.get
      assert(shippingName == refName, s"Shipping name '$shippingName' should match reference name '$refName'")
      assert(shippingEmail == refEmail, s"Shipping email '$shippingEmail' should match reference email '$refEmail'")
      
      // Address should remain unchanged (not part of foreign key)
      assert(List("123 Main St", "456 Oak Ave").contains(address), s"Address should remain unchanged, got: $address")
    }
  }

  test("Can handle nested fields with array types") {
    val nestedStruct = StructType(Array(
      StructField("account_id", StringType),
      StructField("details", StructType(Array(
        StructField("name", StringType),
        StructField("transactions", ArrayType(StructType(Array(
          StructField("txn_id", StringType),
          StructField("amount", StringType)
        ))))
      )))
    ))
    val fields = Array(StructField("customer", nestedStruct))

    assert(ForeignKeyUtil.hasDfContainField("customer.account_id", fields))
    assert(ForeignKeyUtil.hasDfContainField("customer.details.name", fields))
    assert(ForeignKeyUtil.hasDfContainField("customer.details.transactions.txn_id", fields))
    assert(!ForeignKeyUtil.hasDfContainField("customer.details.invalid_field", fields))
  }

  test("hasDfContainField should handle deeply nested structures") {
    val deepNestedStruct = StructType(Array(
      StructField("level1", StructType(Array(
        StructField("level2", StructType(Array(
          StructField("level3", StructType(Array(
            StructField("target_field", StringType)
          )))
        )))
      )))
    ))
    val fields = Array(StructField("root", deepNestedStruct))

    assert(ForeignKeyUtil.hasDfContainField("root.level1.level2.level3.target_field", fields))
    assert(!ForeignKeyUtil.hasDfContainField("root.level1.level2.level3.missing_field", fields))
    assert(!ForeignKeyUtil.hasDfContainField("root.level1.level2.missing_level", fields))
  }

  test("Can link foreign keys with nested field names") {
    val nestedStruct = StructType(Array(StructField("account_id", StringType)))
    val nestedInArray = ArrayType(nestedStruct)
    val fields = Array(StructField("my_json", nestedStruct), StructField("my_array", nestedInArray))

    assert(ForeignKeyUtil.hasDfContainField("my_array.account_id", fields))
    assert(ForeignKeyUtil.hasDfContainField("my_json.account_id", fields))
    assert(!ForeignKeyUtil.hasDfContainField("my_json.name", fields))
    assert(!ForeignKeyUtil.hasDfContainField("my_array.name", fields))
  }

  test("getDataFramesWithForeignKeys should return back list of dataframes in correct order when foreign keys are defined") {
    val sinkOptions = SinkOptions(None, None,
      List(
        ForeignKey(
          ForeignKeyRelation("sourceDf", "sourceDataSource", List("value")),
          List(ForeignKeyRelation("targetDf", "targetDataSource", List("value"))),
          List()
        )
      ))
    val plan = Plan("foreign keys", "simple plan", List(
      TaskSummary("my_task", "sourceDf"),
      TaskSummary("my_target_task", "targetDf"),
      TaskSummary("my_other_task", "otherDf")
    ), Some(sinkOptions))
    val generatedDataForeachTask = List(
      ("otherDf.otherDataSource", sparkSession.createDataFrame(Seq((1, "f"), (2, "g"))).toDF("id", "value")),
      ("sourceDf.sourceDataSource", sparkSession.createDataFrame(Seq((1, "a"), (2, "b"))).toDF("id", "value")),
      ("targetDf.targetDataSource", sparkSession.createDataFrame(Seq((1, "x"), (2, "y"))).toDF("id", "value")),
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, generatedDataForeachTask)
    val resultDfNames = result.map(_._1)
    val expectedDfNamesOrder = List("sourceDf.sourceDataSource", "targetDf.targetDataSource", "otherDf.otherDataSource")

    resultDfNames should contain theSameElementsInOrderAs expectedDfNamesOrder
  }

  test("getDataFramesWithForeignKeys should return back list of dataframes in correct order when multiple generations are defined") {
    val sinkOptions = SinkOptions(None, None,
      List(
        ForeignKey(
          ForeignKeyRelation("sourceDf", "sourceDataSource", List("value")),
          List(
            ForeignKeyRelation("targetDf1", "targetDataSource1", List("value")),
            ForeignKeyRelation("targetDf2", "targetDataSource2", List("value")),
            ForeignKeyRelation("targetDf3", "targetDataSource3", List("value")),
          ),
          List()
        )
      ))
    val plan = Plan("foreign keys", "simple plan", List(
      TaskSummary("my_task", "sourceDf"),
      TaskSummary("my_target3_task", "targetDf3"),
      TaskSummary("my_target1_task", "targetDf1"),
      TaskSummary("my_target2_task", "targetDf2"),
    ), Some(sinkOptions))
    val generatedDataForeachTask = List(
      ("targetDf3.targetDataSource3", sparkSession.createDataFrame(Seq((1, "x"), (2, "y"))).toDF("id", "value")),
      ("targetDf1.targetDataSource1", sparkSession.createDataFrame(Seq((1, "c"), (2, "d"))).toDF("id", "value")),
      ("sourceDf.sourceDataSource", sparkSession.createDataFrame(Seq((1, "a"), (2, "b"))).toDF("id", "value")),
      ("targetDf2.targetDataSource2", sparkSession.createDataFrame(Seq((1, "f"), (2, "g"))).toDF("id", "value")),
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, generatedDataForeachTask)
    val resultDfNames = result.map(_._1)
    val expectedDfNamesOrder = List("sourceDf.sourceDataSource", "targetDf1.targetDataSource1", "targetDf2.targetDataSource2", "targetDf3.targetDataSource3")

    resultDfNames should contain theSameElementsInOrderAs expectedDfNamesOrder
  }

  test("getDataFramesWithForeignKeys should throw MissingDataSourceFromForeignKeyException if source dataframe is missing") {
    val sinkOptions = SinkOptions(None, None,
      List(
        ForeignKey(
          ForeignKeyRelation("sourceDf", "sourceDataSource", List("value")),
          List(ForeignKeyRelation("targetDf", "targetDataSource", List("value"))),
          List()
        )
      ))
    val plan = Plan("foreign keys", "simple plan", List(TaskSummary("my_task", "sourceDf"), TaskSummary("my_target_task", "targetDf")), Some(sinkOptions))
    val generatedDataForeachTask = List(
      ("targetDf.targetDataSource", sparkSession.createDataFrame(Seq((1, "x"), (2, "y"))).toDF("id", "value"))
    )

    assertThrows[MissingDataSourceFromForeignKeyException] {
      ForeignKeyUtil.getDataFramesWithForeignKeys(plan, generatedDataForeachTask)
    }
  }

  test("isValidForeignKeyRelation should return true for valid foreign key relation") {
    val generatedDataForeachTask = Map(
      "sourceDf.sourceDataSource" -> sparkSession.createDataFrame(Seq((1, "a"), (2, "b"))).toDF("id", "value"),
      "targetDf.targetDataSource" -> sparkSession.createDataFrame(Seq((1, "a"), (2, "b"))).toDF("id", "value"),
    )
    val enabledSources = List("sourceDf", "targetDf")
    val fkr = ForeignKeyWithGenerateAndDelete(
      ForeignKeyRelation("sourceDf", "sourceDataSource", List("value")),
      List(ForeignKeyRelation("targetDf", "targetDataSource", List("value"))),
      List()
    )

    val result = ForeignKeyUtil.isValidForeignKeyRelation(generatedDataForeachTask, enabledSources, fkr)
    result shouldBe true
  }

  test("isValidForeignKeyRelation should return false if main foreign key source is not enabled") {
    val generatedDataForeachTask = Map(
      "sourceDf.sourceDataSource" -> sparkSession.createDataFrame(Seq((1, "a"), (2, "b"))).toDF("id", "value")
    )
    val enabledSources = List("targetDataSource")
    val fkr = ForeignKeyWithGenerateAndDelete(
      ForeignKeyRelation("sourceDf", "sourceDataSource", List("value")),
      List(ForeignKeyRelation("targetDf", "targetDataSource", List("value"))),
      List()
    )

    val result = ForeignKeyUtil.isValidForeignKeyRelation(generatedDataForeachTask, enabledSources, fkr)
    result shouldBe false
  }

  class ForeignKeyPlanRun extends PlanRun {
    val myPlan = plan.addForeignKeyRelationship(
      foreignField("my_csv", "random_step", "id"),
      foreignField("my_postgres", "public.accounts", "account_id")
    ).addForeignKeyRelationship(
      foreignField("my_json", "json_step", "id"),
      foreignField("my_postgres", "public.orders", "customer_id")
    )

    execute(plan = myPlan)
  }
}

case class Account(account_id: String = "acc123", name: String = "peter", open_date: Date = Date.valueOf("2023-01-31"), age: Int = 10, debitCredit: String = "D")

case class Transaction(account_id: String, name: String, transaction_id: String, created_date: Date, amount: Double, links: List[String] = List())
