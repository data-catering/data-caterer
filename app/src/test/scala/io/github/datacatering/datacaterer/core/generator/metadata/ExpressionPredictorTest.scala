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

    assert(result.dataType.typeName == "struct")
    val resNested = result.dataType.asInstanceOf[StructType].fields
    assert(resNested.length == 2)
    assert(resNested.exists(_.name == "firstName"))
    assert(resNested.filter(_.name == "firstName").head.metadata.getString(EXPRESSION) == "#{Name.firstname}")
    assert(resNested.filter(_.name == "firstName").head.metadata.getString(FIELD_LABEL) == LABEL_NAME)
    assert(resNested.filter(_.name == "firstName").head.metadata.getString(IS_PII) == "true")
    assert(resNested.exists(_.name == "my_custom_field"))
  }

  test("When given field with name first_name, use first name expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("first_name"))

    assert(result.get.fakerExpression == "#{Name.firstname}")
    assert(result.get.label == LABEL_NAME)
    assert(result.get.isPII)
  }

  test("When given field with name last_name, use last name expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("last_name"))

    assert(result.get.fakerExpression == "#{Name.lastname}")
    assert(result.get.label == LABEL_NAME)
    assert(result.get.isPII)
  }

  test("When given field with name username, use username expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("username"))

    assert(result.get.fakerExpression == "#{Name.username}")
    assert(result.get.label == LABEL_USERNAME)
    assert(result.get.isPII)
  }

  test("When given field called name, use name expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("name"))

    assert(result.get.fakerExpression == "#{Name.name}")
    assert(result.get.label == LABEL_NAME)
    assert(result.get.isPII)
  }

  test("When given field called full_name, use name expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("full_name"))

    assert(result.get.fakerExpression == "#{Name.name}")
    assert(result.get.label == LABEL_NAME)
    assert(result.get.isPII)
  }

  test("When given field called city, use city expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("city"))

    assert(result.get.fakerExpression == "#{Address.city}")
    assert(result.get.label == LABEL_ADDRESS)
    assert(!result.get.isPII)
  }

  test("When given field called country, use country expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("country"))

    assert(result.get.fakerExpression == "#{Address.country}")
    assert(result.get.label == LABEL_ADDRESS)
    assert(!result.get.isPII)
  }

  test("When given field called country_code, use country code expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("country_code"))

    assert(result.get.fakerExpression == "#{Address.countryCode}")
    assert(result.get.label == LABEL_ADDRESS)
    assert(!result.get.isPII)
  }

  test("When given field called nationality, use nationality expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("nationality"))

    assert(result.get.fakerExpression == "#{Nation.nationality}")
    assert(result.get.label == LABEL_NATION)
    assert(!result.get.isPII)
  }

  test("When given field called capital_city, use capital city expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("capital_city"))

    assert(result.get.fakerExpression == "#{Nation.capitalCity}")
    assert(result.get.label == LABEL_NATION)
    assert(!result.get.isPII)
  }

  test("When given field called capital, use capital city expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("capital"))

    assert(result.get.fakerExpression == "#{Nation.capitalCity}")
    assert(result.get.label == LABEL_NATION)
    assert(!result.get.isPII)
  }

  test("When given field called address, use full address expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("address"))

    assert(result.get.fakerExpression == "#{Address.fullAddress}")
    assert(result.get.label == LABEL_ADDRESS)
    assert(result.get.isPII)
  }

  test("When given field called customer_address, use full address expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("customer_address"))

    assert(result.get.fakerExpression == "#{Address.fullAddress}")
    assert(result.get.label == LABEL_ADDRESS)
    assert(result.get.isPII)
  }

  test("When given field called version, use version expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("version"))

    assert(result.get.fakerExpression == "#{App.version}")
    assert(result.get.label == LABEL_APP)
    assert(!result.get.isPII)
  }

  test("When given field called payment_method, use payment method expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("payment_method"))

    assert(result.get.fakerExpression == "#{Subscription.paymentMethods}")
    assert(result.get.label == LABEL_MONEY)
    assert(!result.get.isPII)
  }

  test("When given field with name email_address, use email expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("email_address"))

    assert(result.get.fakerExpression == "#{Internet.emailAddress}")
    assert(result.get.label == LABEL_INTERNET)
    assert(result.get.isPII)
  }

  test("When given field with name containing email, use email expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("customer_email"))

    assert(result.get.fakerExpression == "#{Internet.emailAddress}")
    assert(result.get.label == LABEL_INTERNET)
    assert(result.get.isPII)
  }

  test("When given field called mac_address, use mac address expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("mac_address"))

    assert(result.get.fakerExpression == "#{Internet.macAddress}")
    assert(result.get.label == LABEL_INTERNET)
    assert(result.get.isPII)
  }

  test("When given field called ipv4, use ipv4 address expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("ipv4"))

    assert(result.get.fakerExpression == "#{Internet.ipV4Address}")
    assert(result.get.label == LABEL_INTERNET)
    assert(result.get.isPII)
  }

  test("When given field called ipv4_address, use ipv4 address expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("ipv4_address"))

    assert(result.get.fakerExpression == "#{Internet.ipV4Address}")
    assert(result.get.label == LABEL_INTERNET)
    assert(result.get.isPII)
  }

  test("When given field called ipv6, use ipv6 address expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("ipv6"))

    assert(result.get.fakerExpression == "#{Internet.ipV6Address}")
    assert(result.get.label == LABEL_INTERNET)
    assert(result.get.isPII)
  }

  test("When given field called ipv6_address, use ipv6 address expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("ipv6_address"))

    assert(result.get.fakerExpression == "#{Internet.ipV6Address}")
    assert(result.get.label == LABEL_INTERNET)
    assert(result.get.isPII)
  }

  test("When given field called currency, use currency expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("currency"))

    assert(result.get.fakerExpression == "#{Money.currency}")
    assert(result.get.label == LABEL_MONEY)
    assert(!result.get.isPII)
  }

  test("When given field called currency_code, use currency code expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("currency_code"))

    assert(result.get.fakerExpression == "#{Money.currencyCode}")
    assert(result.get.label == LABEL_MONEY)
    assert(!result.get.isPII)
  }

  test("When given field called credit_card, use credit card expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("credit_card"))

    assert(result.get.fakerExpression == "#{Finance.creditCard}")
    assert(result.get.label == LABEL_MONEY)
    assert(result.get.isPII)
  }

  test("When given field called food, use dish expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("food"))

    assert(result.get.fakerExpression == "#{Food.dish}")
    assert(result.get.label == LABEL_FOOD)
    assert(!result.get.isPII)
  }

  test("When given field called dish, use dish expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("dish"))

    assert(result.get.fakerExpression == "#{Food.dish}")
    assert(result.get.label == LABEL_FOOD)
    assert(!result.get.isPII)
  }

  test("When given field called ingredient, use ingredient expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("ingredient"))

    assert(result.get.fakerExpression == "#{Food.ingredient}")
    assert(result.get.label == LABEL_FOOD)
    assert(!result.get.isPII)
  }

  test("When given field called job_field, use job field expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("job_field"))

    assert(result.get.fakerExpression == "#{Job.field}")
    assert(result.get.label == LABEL_JOB)
    assert(!result.get.isPII)
  }

  test("When given field called job_position, use job position expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("job_position"))

    assert(result.get.fakerExpression == "#{Job.position}")
    assert(result.get.label == LABEL_JOB)
    assert(!result.get.isPII)
  }

  test("When given field called job_title, use job title expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("job_title"))

    assert(result.get.fakerExpression == "#{Job.title}")
    assert(result.get.label == LABEL_JOB)
    assert(!result.get.isPII)
  }

  test("When given field called relationship, use relationship expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("relationship"))

    assert(result.get.fakerExpression == "#{Relationship.any}")
    assert(result.get.label == LABEL_RELATIONSHIP)
    assert(!result.get.isPII)
  }

  test("When given field called weather, use weather description expression") {
    val result = ExpressionPredictor.tryGetFieldPrediction(field("weather"))

    assert(result.get.fakerExpression == "#{Weather.description}")
    assert(result.get.label == LABEL_WEATHER)
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
      assert(result.get.fakerExpression == "#{PhoneNumber.cellPhone}")
      assert(result.get.label == LABEL_PHONE)
      assert(result.get.isPII)
    })
  }

  private def field(name: String): StructField = StructField(name, StringType)
}
