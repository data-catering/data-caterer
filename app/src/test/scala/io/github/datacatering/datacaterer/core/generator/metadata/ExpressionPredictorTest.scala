package io.github.datacatering.datacaterer.core.generator.metadata

import io.github.datacatering.datacaterer.api.model.Constants.{EXPRESSION, FIELD_LABEL, IS_PII, LABEL_ADDRESS, LABEL_APP, LABEL_FOOD, LABEL_INTERNET, LABEL_JOB, LABEL_MONEY, LABEL_NAME, LABEL_NATION, LABEL_PHONE, LABEL_RELATIONSHIP, LABEL_USERNAME, LABEL_WEATHER}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.{Files, Paths, StandardOpenOption}

@RunWith(classOf[JUnitRunner])
class ExpressionPredictorTest extends AnyFunSuite {

  test("Can get all data faker expressions and write to file") {
    val allExpressions = ExpressionPredictor.getAllFakerExpressionTypes.sorted
    val testResourcesFolder = getClass.getResource("/datafaker").getPath
    val file = Paths.get(s"$testResourcesFolder/expressions.txt")
    Files.write(file, allExpressions.mkString("\n").getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  test("When given field that has nested fields, generate predictions for nested fields as well") {
    val nestedFields = Array(StructField("firstName", StringType), StructField("my_custom_field", StringType))
    val structType = StructType(nestedFields)
    val baseField = StructField("my_nested_struct", structType)

    val result = ExpressionPredictor.getFieldPredictions(baseField)

    assertResult("struct")(result.dataType.typeName)
    val resNested = result.dataType.asInstanceOf[StructType].fields
    assert(resNested.length == 2)
    assert(resNested.exists(_.name == "firstName"))
    assertResult("#{Name.firstname}")(resNested.filter(_.name == "firstName").head.metadata.getString(EXPRESSION))
    assertResult(LABEL_NAME)(resNested.filter(_.name == "firstName").head.metadata.getString(FIELD_LABEL))
    assertResult("true")(resNested.filter(_.name == "firstName").head.metadata.getString(IS_PII))
    assert(resNested.exists(_.name == "my_custom_field"))
  }

  test("When given field with name first_name, use first name expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("first_name"))

    assertResult("#{Name.firstname}")(result.get.fakerExpression)
    assertResult(LABEL_NAME)(result.get.label)
    assert(result.get.isPII)
  }

  test("When given field with name last_name, use last name expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("last_name"))

    assertResult("#{Name.lastname}")(result.get.fakerExpression)
    assertResult(LABEL_NAME)(result.get.label)
    assert(result.get.isPII)
  }

  test("When given field with name username, use username expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("username"))

    assertResult("#{Name.username}")(result.get.fakerExpression)
    assertResult(LABEL_USERNAME)(result.get.label)
    assert(result.get.isPII)
  }

  test("When given field called name, use name expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("name"))

    assertResult("#{Name.name}")(result.get.fakerExpression)
    assertResult(LABEL_NAME)(result.get.label)
    assert(result.get.isPII)
  }

  test("When given field called full_name, use name expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("full_name"))

    assertResult("#{Name.name}")(result.get.fakerExpression)
    assertResult(LABEL_NAME)(result.get.label)
    assert(result.get.isPII)
  }

  test("When given field called city, use city expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("city"))

    assertResult("#{Address.city}")(result.get.fakerExpression)
    assertResult(LABEL_ADDRESS)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called country, use country expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("country"))

    assertResult("#{Address.country}")(result.get.fakerExpression)
    assertResult(LABEL_ADDRESS)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called country_code, use country code expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("country_code"))

    assertResult("#{Address.countryCode}")(result.get.fakerExpression)
    assertResult(LABEL_ADDRESS)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called nationality, use nationality expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("nationality"))

    assertResult("#{Nation.nationality}")(result.get.fakerExpression)
    assertResult(LABEL_NATION)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called capital_city, use capital city expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("capital_city"))

    assertResult("#{Nation.capitalCity}")(result.get.fakerExpression)
    assertResult(LABEL_NATION)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called capital, use capital city expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("capital"))

    assertResult("#{Nation.capitalCity}")(result.get.fakerExpression)
    assertResult(LABEL_NATION)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called address, use full address expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("address"))

    assertResult("#{Address.fullAddress}")(result.get.fakerExpression)
    assertResult(LABEL_ADDRESS)(result.get.label)
    assert(result.get.isPII)
  }

  test("When given field called customer_address, use full address expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("customer_address"))

    assertResult("#{Address.fullAddress}")(result.get.fakerExpression)
    assertResult(LABEL_ADDRESS)(result.get.label)
    assert(result.get.isPII)
  }

  test("When given field called version, use version expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("version"))

    assertResult("#{App.version}")(result.get.fakerExpression)
    assertResult(LABEL_APP)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called payment_method, use payment method expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("payment_method"))

    assertResult("#{Subscription.paymentMethods}")(result.get.fakerExpression)
    assertResult(LABEL_MONEY)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field with name email_address, use email expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("email_address"))

    assertResult("#{Internet.emailAddress}")(result.get.fakerExpression)
    assertResult(LABEL_INTERNET)(result.get.label)
    assert(result.get.isPII)
  }

  test("When given field with name containing email, use email expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("customer_email"))

    assertResult("#{Internet.emailAddress}")(result.get.fakerExpression)
    assertResult(LABEL_INTERNET)(result.get.label)
    assert(result.get.isPII)
  }

  test("When given field called mac_address, use mac address expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("mac_address"))

    assertResult("#{Internet.macAddress}")(result.get.fakerExpression)
    assertResult(LABEL_INTERNET)(result.get.label)
    assert(result.get.isPII)
  }

  test("When given field called ipv4, use ipv4 address expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("ipv4"))

    assertResult("#{Internet.ipV4Address}")(result.get.fakerExpression)
    assertResult(LABEL_INTERNET)(result.get.label)
    assert(result.get.isPII)
  }

  test("When given field called ipv4_address, use ipv4 address expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("ipv4_address"))

    assertResult("#{Internet.ipV4Address}")(result.get.fakerExpression)
    assertResult(LABEL_INTERNET)(result.get.label)
    assert(result.get.isPII)
  }

  test("When given field called ipv6, use ipv6 address expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("ipv6"))

    assertResult("#{Internet.ipV6Address}")(result.get.fakerExpression)
    assertResult(LABEL_INTERNET)(result.get.label)
    assert(result.get.isPII)
  }

  test("When given field called ipv6_address, use ipv6 address expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("ipv6_address"))

    assertResult("#{Internet.ipV6Address}")(result.get.fakerExpression)
    assertResult(LABEL_INTERNET)(result.get.label)
    assert(result.get.isPII)
  }

  test("When given field called currency, use currency expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("currency"))

    assertResult("#{Money.currency}")(result.get.fakerExpression)
    assertResult(LABEL_MONEY)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called currency_code, use currency code expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("currency_code"))

    assertResult("#{Money.currencyCode}")(result.get.fakerExpression)
    assertResult(LABEL_MONEY)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called credit_card, use credit card expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("credit_card"))

    assertResult("#{Finance.creditCard}")(result.get.fakerExpression)
    assertResult(LABEL_MONEY)(result.get.label)
    assert(result.get.isPII)
  }

  test("When given field called food, use dish expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("food"))

    assertResult("#{Food.dish}")(result.get.fakerExpression)
    assertResult(LABEL_FOOD)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called dish, use dish expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("dish"))

    assertResult("#{Food.dish}")(result.get.fakerExpression)
    assertResult(LABEL_FOOD)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called ingredient, use ingredient expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("ingredient"))

    assertResult("#{Food.ingredient}")(result.get.fakerExpression)
    assertResult(LABEL_FOOD)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called job_field, use job field expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("job_field"))

    assertResult("#{Job.field}")(result.get.fakerExpression)
    assertResult(LABEL_JOB)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called job_position, use job position expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("job_position"))

    assertResult("#{Job.position}")(result.get.fakerExpression)
    assertResult(LABEL_JOB)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called job_title, use job title expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("job_title"))

    assertResult("#{Job.title}")(result.get.fakerExpression)
    assertResult(LABEL_JOB)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called relationship, use relationship expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("relationship"))

    assertResult("#{Relationship.any}")(result.get.fakerExpression)
    assertResult(LABEL_RELATIONSHIP)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field called weather, use weather description expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("weather"))

    assertResult("#{Weather.description}")(result.get.fakerExpression)
    assertResult(LABEL_WEATHER)(result.get.label)
    assert(!result.get.isPII)
  }

  test("When given field name that contains phone, use phone number expression") {
    val results = List(
      ExpressionPredictor.tryGetFieldPrediction(field("cell_phone")),
      ExpressionPredictor.tryGetFieldPrediction(field("mobile_phone")),
      ExpressionPredictor.tryGetFieldPrediction(field("home_phone")),
      ExpressionPredictor.tryGetFieldPrediction(field("HomePhone")),
      ExpressionPredictor.tryGetFieldPrediction(field("Homephone")),
      ExpressionPredictor.tryGetFieldPrediction(field("home phone")),
      ExpressionPredictor.tryGetFieldPrediction(field("phone")),
    )

    results.foreach(result => {
      assertResult("#{PhoneNumber.cellPhone}")(result.get.fakerExpression)
      assertResult(LABEL_PHONE)(result.get.label)
      assert(result.get.isPII)
    })
  }

  private def field(name: String): StructField = StructField(name, StringType)
}
