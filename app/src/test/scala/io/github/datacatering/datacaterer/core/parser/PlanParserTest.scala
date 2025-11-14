package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.api.model.Constants.YAML_REAL_TIME_BODY_FIELD
import io.github.datacatering.datacaterer.api.model.{Count, Field, ForeignKeyRelation, Step, Task}
import io.github.datacatering.datacaterer.core.util.SparkSuite

class PlanParserTest extends SparkSuite {

  private val basePath = getClass.getResource("/sample").getPath

  test("Can parse plan in YAML file") {
    val result = PlanParser.parsePlan(s"$basePath/plan/account-create-plan-test.yaml")

    assert(result.name.nonEmpty)
    assert(result.description.nonEmpty)
    assertResult(4)(result.tasks.size)
    assertResult(1)(result.validations.size)
    assert(result.sinkOptions.isDefined)
    assertResult(1)(result.sinkOptions.get.foreignKeys.size)
    assertResult(ForeignKeyRelation("solace", "jms_account", List("account_id")))(result.sinkOptions.get.foreignKeys.head.source)
    assertResult(List(ForeignKeyRelation("json", "file_account", List("account_id"))))(result.sinkOptions.get.foreignKeys.head.generate)
  }

  test("Can parse task in YAML file") {
    val result = PlanParser.parseTasks(s"$basePath/task")

    assert(result.length > 0)
  }

  test("Can parse plan in YAML file with foreign key") {
    val result = PlanParser.parsePlan(s"$basePath/plan/large-plan.yaml")

    assert(result.sinkOptions.isDefined)
    assertResult(1)(result.sinkOptions.get.foreignKeys.size)
    assertResult(ForeignKeyRelation("json", "file_account", List("account_id")))(result.sinkOptions.get.foreignKeys.head.source)
    assertResult(1)(result.sinkOptions.get.foreignKeys.head.generate.size)
    assertResult(ForeignKeyRelation("csv", "transaction", List("account_id")))(result.sinkOptions.get.foreignKeys.head.generate.head)
  }

  test("Can convert task into specific fields from YAML task") {
    val task = Task("task-name", List(Step(
      "my-step",
      "json",
      Count(),
      Map(),
      List(
        Field("account_id_uuid", Some("string"), Map("uuid" -> "")),
        Field("account_id_uuid_inc", Some("string"), Map("uuid" -> "", "incremental" -> "1")),
        Field("account_id_inc", Some("int"), Map("incremental" -> "5")),
        Field(YAML_REAL_TIME_BODY_FIELD, Some("struct"), fields = List(
          Field("other_acc_id", Some("string"), Map("uuid" -> "", "incremental" -> "10"))
        )),
      )
    )))

    val result = PlanParser.convertToSpecificFields(task)

    assertResult(1)(result.steps.size)
    assertResult(5)(result.steps.head.fields.size)
    assertResult(Field("account_id_uuid", Some("string"), Map("sql" -> "UUID()", "uuid" -> "")))(result.steps.head.fields.head)
    assertResult(Field("account_id_uuid_inc", Some("string"), Map("sql" ->
      """CONCAT(
        |SUBSTR(MD5(CAST(1 + __index_inc AS STRING)), 1, 8), '-',
        |SUBSTR(MD5(CAST(1 + __index_inc AS STRING)), 9, 4), '-',
        |SUBSTR(MD5(CAST(1 + __index_inc AS STRING)), 13, 4), '-',
        |SUBSTR(MD5(CAST(1 + __index_inc AS STRING)), 17, 4), '-',
        |SUBSTR(MD5(CAST(1 + __index_inc AS STRING)), 21, 12)
        |)""".stripMargin, "uuid" -> "", "incremental" -> "1")))(result.steps.head.fields(1))
    assertResult(Field("account_id_inc", Some("integer"), Map("incremental" -> "5")))(result.steps.head.fields(2))
    assertResult(Field("value", Some("string"), Map("sql" -> "TO_JSON(body)")))(result.steps.head.fields(3))
    assertResult(Field("body", Some("string"), fields = List(Field(
      "other_acc_id", Some("string"), Map("sql" ->
        """CONCAT(
          |SUBSTR(MD5(CAST(10 + __index_inc AS STRING)), 1, 8), '-',
          |SUBSTR(MD5(CAST(10 + __index_inc AS STRING)), 9, 4), '-',
          |SUBSTR(MD5(CAST(10 + __index_inc AS STRING)), 13, 4), '-',
          |SUBSTR(MD5(CAST(10 + __index_inc AS STRING)), 17, 4), '-',
          |SUBSTR(MD5(CAST(10 + __index_inc AS STRING)), 21, 12)
          |)""".stripMargin, "uuid" -> "", "incremental" -> "10")
    ))))(result.steps.head.fields(4))
  }

  test("Can parse plan with inline task definitions") {
    val yamlContent =
      """name: "test_inline_plan"
        |description: "Test plan with inline tasks"
        |tasks:
        |  - name: json_task
        |    enabled: true
        |    connection:
        |      type: json
        |      options:
        |        path: "/tmp/output/users"
        |    steps:
        |      - name: users
        |        type: json
        |        count:
        |          records: 100
        |        fields:
        |          - name: user_id
        |            type: string
        |          - name: name
        |            type: string
        |""".stripMargin

    val parsedPlan = PlanParser.parseTaskFromContent(yamlContent, withFieldConversion = false)
    // For this test, we're parsing as a Task to verify the structure
    // In reality, it would be parsed as a Plan with inline tasks

    // Parse as plan instead
    import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
    val plan = ObjectMapperUtil.yamlObjectMapper.readValue(yamlContent, classOf[io.github.datacatering.datacaterer.api.model.Plan])

    assertResult("test_inline_plan")(plan.name)
    assertResult("Test plan with inline tasks")(plan.description)
    assertResult(1)(plan.tasks.size)
    assertResult("json_task")(plan.tasks.head.name)
    assert(plan.tasks.head.steps.isDefined)
    assertResult(1)(plan.tasks.head.steps.get.size)
    assertResult("users")(plan.tasks.head.steps.get.head.name)
    assert(plan.tasks.head.connection.isDefined)
  }

  test("Can extract inline tasks from plan") {
    val yamlContent =
      """name: "test_inline_plan"
        |description: "Test plan with inline tasks"
        |tasks:
        |  - name: json_task
        |    enabled: true
        |    connection:
        |      type: json
        |      options:
        |        path: "/tmp/output/users"
        |    steps:
        |      - name: users
        |        type: json
        |        count:
        |          records: 100
        |        fields:
        |          - name: user_id
        |            type: string
        |          - name: name
        |            type: string
        |""".stripMargin

    import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
    val plan = ObjectMapperUtil.yamlObjectMapper.readValue(yamlContent, classOf[io.github.datacatering.datacaterer.api.model.Plan])

    // Use reflection to access the private extractInlineTasksFromPlan method
    val extractMethod = PlanParser.getClass.getDeclaredMethod("extractInlineTasksFromPlan", classOf[io.github.datacatering.datacaterer.api.model.Plan], classOf[org.apache.spark.sql.SparkSession])
    extractMethod.setAccessible(true)
    val inlineTasks = extractMethod.invoke(PlanParser, plan, sparkSession).asInstanceOf[Array[Task]]

    assertResult(1)(inlineTasks.length)
    assertResult("json_task")(inlineTasks.head.name)
    assertResult(1)(inlineTasks.head.steps.size)
    assertResult("users")(inlineTasks.head.steps.head.name)
    // Connection is on TaskSummary, not on the extracted Task object
  }

  // ==================== Enhanced Foreign Key Configuration Tests ====================

  test("Can parse foreign key with cardinality configuration") {
    val yamlContent =
      """name: "test_fk_cardinality"
        |description: "Test FK with cardinality"
        |tasks:
        |  - name: customers
        |    enabled: true
        |  - name: orders
        |    enabled: true
        |sinkOptions:
        |  foreignKeys:
        |    - source:
        |        dataSource: customers
        |        step: customers
        |        fields: [customer_id]
        |      generate:
        |        - dataSource: orders
        |          step: orders
        |          fields: [customer_id]
        |      relationshipType: "one-to-many"
        |      cardinality:
        |        min: 2
        |        max: 5
        |        distribution: "normal"
        |""".stripMargin

    import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
    val plan = ObjectMapperUtil.yamlObjectMapper.readValue(yamlContent, classOf[io.github.datacatering.datacaterer.api.model.Plan])

    assert(plan.sinkOptions.isDefined)
    assertResult(1)(plan.sinkOptions.get.foreignKeys.size)

    val fk = plan.sinkOptions.get.foreignKeys.head
    assert(fk.relationshipType.isDefined)
    assertResult("one-to-many")(fk.relationshipType.get)

    assert(fk.cardinality.isDefined)
    val cardinality = fk.cardinality.get
    assertResult(Some(2))(cardinality.min)
    assertResult(Some(5))(cardinality.max)
    assertResult("normal")(cardinality.distribution)
  }

  test("Can parse foreign key with nullability configuration") {
    val yamlContent =
      """name: "test_fk_nullability"
        |description: "Test FK with nullability"
        |tasks:
        |  - name: customers
        |    enabled: true
        |  - name: orders
        |    enabled: true
        |sinkOptions:
        |  foreignKeys:
        |    - source:
        |        dataSource: customers
        |        step: customers
        |        fields: [customer_id]
        |      generate:
        |        - dataSource: orders
        |          step: orders
        |          fields: [customer_id]
        |      nullability:
        |        nullPercentage: 0.2
        |        strategy: "random"
        |""".stripMargin

    import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
    val plan = ObjectMapperUtil.yamlObjectMapper.readValue(yamlContent, classOf[io.github.datacatering.datacaterer.api.model.Plan])

    assert(plan.sinkOptions.isDefined)
    assertResult(1)(plan.sinkOptions.get.foreignKeys.size)

    val fk = plan.sinkOptions.get.foreignKeys.head
    assert(fk.nullability.isDefined)

    val nullability = fk.nullability.get
    assertResult(0.2)(nullability.nullPercentage)
    assertResult("random")(nullability.strategy)
  }

  test("Can parse foreign key with generation mode") {
    val yamlContent =
      """name: "test_fk_generation_mode"
        |description: "Test FK with generation mode"
        |tasks:
        |  - name: countries
        |    enabled: true
        |  - name: cities
        |    enabled: true
        |sinkOptions:
        |  foreignKeys:
        |    - source:
        |        dataSource: countries
        |        step: countries
        |        fields: [country_code, region_code]
        |      generate:
        |        - dataSource: cities
        |          step: cities
        |          fields: [country_code, region_code]
        |      generationMode: "all-combinations"
        |""".stripMargin

    import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
    val plan = ObjectMapperUtil.yamlObjectMapper.readValue(yamlContent, classOf[io.github.datacatering.datacaterer.api.model.Plan])

    assert(plan.sinkOptions.isDefined)
    assertResult(1)(plan.sinkOptions.get.foreignKeys.size)

    val fk = plan.sinkOptions.get.foreignKeys.head
    assert(fk.generationMode.isDefined)
    assertResult("all-combinations")(fk.generationMode.get)
  }

  test("Can parse foreign key with all enhanced configurations") {
    val yamlContent =
      """name: "test_fk_complete"
        |description: "Test FK with all enhancements"
        |tasks:
        |  - name: customers
        |    enabled: true
        |  - name: orders
        |    enabled: true
        |sinkOptions:
        |  foreignKeys:
        |    - source:
        |        dataSource: customers
        |        step: customers
        |        fields: [customer_id]
        |      generate:
        |        - dataSource: orders
        |          step: orders
        |          fields: [customer_id]
        |      relationshipType: "one-to-many"
        |      cardinality:
        |        ratio: 3.5
        |        distribution: "zipf"
        |      nullability:
        |        nullPercentage: 0.1
        |        strategy: "tail"
        |      generationMode: "partial"
        |""".stripMargin

    import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
    val plan = ObjectMapperUtil.yamlObjectMapper.readValue(yamlContent, classOf[io.github.datacatering.datacaterer.api.model.Plan])

    assert(plan.sinkOptions.isDefined)
    assertResult(1)(plan.sinkOptions.get.foreignKeys.size)

    val fk = plan.sinkOptions.get.foreignKeys.head

    // Verify relationshipType
    assert(fk.relationshipType.isDefined)
    assertResult("one-to-many")(fk.relationshipType.get)

    // Verify cardinality
    assert(fk.cardinality.isDefined)
    val cardinality = fk.cardinality.get
    assertResult(Some(3.5))(cardinality.ratio)
    assertResult("zipf")(cardinality.distribution)

    // Verify nullability
    assert(fk.nullability.isDefined)
    val nullability = fk.nullability.get
    assertResult(0.1)(nullability.nullPercentage)
    assertResult("tail")(nullability.strategy)

    // Verify generationMode
    assert(fk.generationMode.isDefined)
    assertResult("partial")(fk.generationMode.get)
  }

  test("Can parse foreign key with backward compatibility (no new fields)") {
    val yamlContent =
      """name: "test_fk_backward_compat"
        |description: "Test FK backward compatibility"
        |tasks:
        |  - name: accounts
        |    enabled: true
        |  - name: transactions
        |    enabled: true
        |sinkOptions:
        |  foreignKeys:
        |    - source:
        |        dataSource: accounts
        |        step: accounts
        |        fields: [account_id]
        |      generate:
        |        - dataSource: transactions
        |          step: transactions
        |          fields: [account_id]
        |""".stripMargin

    import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
    val plan = ObjectMapperUtil.yamlObjectMapper.readValue(yamlContent, classOf[io.github.datacatering.datacaterer.api.model.Plan])

    assert(plan.sinkOptions.isDefined)
    assertResult(1)(plan.sinkOptions.get.foreignKeys.size)

    val fk = plan.sinkOptions.get.foreignKeys.head

    // Verify old structure still works
    assertResult(ForeignKeyRelation("accounts", "accounts", List("account_id")))(fk.source)
    assertResult(1)(fk.generate.size)
    assertResult(ForeignKeyRelation("transactions", "transactions", List("account_id")))(fk.generate.head)

    // Verify new fields are None (backward compatibility)
    assert(fk.relationshipType.isEmpty)
    assert(fk.cardinality.isEmpty)
    assert(fk.nullability.isEmpty)
    assert(fk.generationMode.isEmpty)
  }

  test("Can parse cardinality with ratio only (uniform distribution by default)") {
    val yamlContent =
      """name: "test_cardinality_ratio"
        |tasks:
        |  - name: products
        |    enabled: true
        |  - name: reviews
        |    enabled: true
        |sinkOptions:
        |  foreignKeys:
        |    - source:
        |        dataSource: products
        |        step: products
        |        fields: [product_id]
        |      generate:
        |        - dataSource: reviews
        |          step: reviews
        |          fields: [product_id]
        |      cardinality:
        |        ratio: 10.0
        |""".stripMargin

    import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
    val plan = ObjectMapperUtil.yamlObjectMapper.readValue(yamlContent, classOf[io.github.datacatering.datacaterer.api.model.Plan])

    val fk = plan.sinkOptions.get.foreignKeys.head
    assert(fk.cardinality.isDefined)

    val cardinality = fk.cardinality.get
    assertResult(Some(10.0))(cardinality.ratio)
    assertResult("uniform")(cardinality.distribution) // Default value
    assert(cardinality.min.isEmpty)
    assert(cardinality.max.isEmpty)
  }
}
