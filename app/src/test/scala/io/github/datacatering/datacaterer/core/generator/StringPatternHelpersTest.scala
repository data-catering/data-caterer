package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.FieldBuilder
import io.github.datacatering.datacaterer.api.model.Constants._
import org.scalatest.funsuite.AnyFunSuite

class StringPatternHelpersTest extends AnyFunSuite {

  test("email() with default domain should use DataFaker expression") {
    val field = FieldBuilder()
      .name("email")
      .email()
      .field

    val expression = field.options.get(EXPRESSION)
    assert(expression.isDefined)
    assert(expression.get == "#{Internet.emailAddress}")
  }

  test("email() with custom domain should generate proper expression") {
    val field = FieldBuilder()
      .name("work_email")
      .email("company.com")
      .field

    val expression = field.options.get(EXPRESSION)
    assert(expression.isDefined)
    assert(expression.get == "#{Name.first_name}.#{Name.last_name}@company.com")
  }

  test("phone() with default US format should use cell phone expression") {
    val field = FieldBuilder()
      .name("phone")
      .phone()
      .field

    val expression = field.options.get(EXPRESSION)
    assert(expression.isDefined)
    assert(expression.get == "#{PhoneNumber.cellPhone}")
  }

  test("phone() with UK format should use UK phone expression") {
    val field = FieldBuilder()
      .name("uk_phone")
      .phone("UK")
      .field

    val expression = field.options.get(EXPRESSION)
    assert(expression.isDefined)
    assert(expression.get == "#{PhoneNumber.phoneNumber}")
  }

  test("phone() with AU format should use Australian phone expression") {
    val field = FieldBuilder()
      .name("au_phone")
      .phone("AU")
      .field

    val expression = field.options.get(EXPRESSION)
    assert(expression.isDefined)
    assert(expression.get == "#{PhoneNumber.phoneNumber}")
  }

  test("phone() with unknown format should throw an exception") {
    assertThrows[IllegalArgumentException] {
      val field = FieldBuilder()
        .name("unknown_phone")
        .phone("UNKNOWN")
        .field
    }
  }

  test("uuidPattern() should use UUID generator") {
    val field = FieldBuilder()
      .name("id")
      .uuidPattern()
      .field

    val sqlGenerator = field.options.get(SQL_GENERATOR)
    assert(sqlGenerator.isDefined)
    assert(sqlGenerator.get == "UUID()")
  }

  test("url() with default https protocol should generate https URL") {
    val field = FieldBuilder()
      .name("website")
      .url()
      .field

    val expression = field.options.get(EXPRESSION)
    assert(expression.isDefined)
    assert(expression.get == "https://#{Internet.domainName}")
  }

  test("url() with http protocol should generate http URL") {
    val field = FieldBuilder()
      .name("api")
      .url("http")
      .field

    val expression = field.options.get(EXPRESSION)
    assert(expression.isDefined)
    assert(expression.get == "http://#{Internet.domainName}")
  }

  test("ipv4() should use IPv4 address expression") {
    val field = FieldBuilder()
      .name("ip")
      .ipv4()
      .field

    val expression = field.options.get(EXPRESSION)
    assert(expression.isDefined)
    assert(expression.get == "#{Internet.ipV4Address}")
  }

  test("ipv6() should use IPv6 address expression") {
    val field = FieldBuilder()
      .name("ip6")
      .ipv6()
      .field

    val expression = field.options.get(EXPRESSION)
    assert(expression.isDefined)
    assert(expression.get == "#{Internet.ipV6Address}")
  }

  test("ssnPattern() should use regex pattern for SSN") {
    val field = FieldBuilder()
      .name("ssn")
      .ssnPattern()
      .field

    val regex = field.options.get(REGEX_GENERATOR)
    assert(regex.isDefined)
    assert(regex.get == "[0-9]{3}-[0-9]{2}-[0-9]{4}")
  }

  test("creditCard() with default visa type should use credit card expression") {
    val field = FieldBuilder()
      .name("card")
      .creditCard()
      .field

    val expression = field.options.get(EXPRESSION)
    assert(expression.isDefined)
    assert(expression.get == "#{Finance.creditCard 'VISA'}")
  }

  test("creditCard() with mastercard type should use credit card expression") {
    val field = FieldBuilder()
      .name("card")
      .creditCard("mastercard")
      .field

    val expression = field.options.get(EXPRESSION)
    assert(expression.isDefined)
    assert(expression.get == "#{Finance.creditCard 'MASTERCARD'}")
  }

  test("creditCard() with amex type should use credit card expression") {
    val field = FieldBuilder()
      .name("card")
      .creditCard("amex")
      .field

    val expression = field.options.get(EXPRESSION)
    assert(expression.isDefined)
    assert(expression.get == "#{Finance.creditCard 'AMEX'}")
  }

  test("string pattern helpers should be chainable with other methods") {
    val field = FieldBuilder()
      .name("primary_email")
      .email("company.com")
      .field

    assert(field.name == "primary_email")
    assert(field.options.get(EXPRESSION).isDefined)
  }

  test("Java varargs compatibility - email should work from Java") {
    // Simulate Java call with default parameter
    val field = FieldBuilder()
      .name("java_email")
      .email()

    assert(field.field.options.get(EXPRESSION).isDefined)
  }
}
