package io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.jsonschema.model._
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class JsonSchemaConverterTest extends AnyFunSuite {

  test("can convert simple primitive schema") {
    val schema = JsonSchemaDefinition(
      `type` = Some("string"),
      minLength = Some(5),
      maxLength = Some(20),
      pattern = Some("^[A-Za-z]+$")
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-simple")

    assert(result.size == 1)
    val field = result.head
    assert(field.name == "root_value")
    assert(field.`type`.contains("string"))
    assert(field.options.contains(MINIMUM_LENGTH))
    assert(field.options(MINIMUM_LENGTH) == 5)
    assert(field.options(MAXIMUM_LENGTH) == 20)
    assert(field.options(REGEX_GENERATOR) == "^[A-Za-z]+$")
  }

  test("can convert object schema with properties") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "id" -> JsonSchemaProperty(`type` = Some("integer"), minimum = Some("1"), maximum = Some("1000")),
        "name" -> JsonSchemaProperty(`type` = Some("string"), minLength = Some(1), maxLength = Some(50)),
        "email" -> JsonSchemaProperty(`type` = Some("string"), format = Some("email")),
        "status" -> JsonSchemaProperty(`type` = Some("string"), `enum` = Some(List("active", "inactive")))
      )),
      required = Some(List("id", "name"))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-object")

    assert(result.size == 4)
    
    val idField = result.find(_.name == "id").get
    assert(idField.`type`.contains("integer"))
    assert(idField.options(MINIMUM) == 1.0)
    assert(idField.options(MAXIMUM) == 1000.0)
    assert(!idField.nullable) // required

    val nameField = result.find(_.name == "name").get
    assert(nameField.`type`.contains("string"))
    assert(nameField.options(MINIMUM_LENGTH) == 1)
    assert(nameField.options(MAXIMUM_LENGTH) == 50)
    assert(!nameField.nullable) // required

    val emailField = result.find(_.name == "email").get
    assert(emailField.`type`.contains("string"))
    assert(emailField.options.contains(REGEX_GENERATOR)) // email format should add regex
    assert(emailField.nullable) // not required

    val statusField = result.find(_.name == "status").get
    assert(statusField.`type`.contains("string"))
    assert(statusField.options.contains(ONE_OF_GENERATOR))
    assert(statusField.options(ONE_OF_GENERATOR) == "active,inactive")
  }

  test("can convert nested object schema") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "user" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "name" -> JsonSchemaProperty(`type` = Some("string")),
            "age" -> JsonSchemaProperty(`type` = Some("integer"), minimum = Some("0"), maximum = Some("150"))
          )),
          required = Some(List("name"))
        )
      )),
      required = Some(List("user"))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-nested")

    assert(result.size == 1)
    val userField = result.head
    assert(userField.name == "user")
    assert(userField.`type`.get.contains("struct"))
    assert(userField.fields.size == 2)
    
    val nameField = userField.fields.find(_.name == "name").get
    assert(nameField.`type`.contains("string"))
    assert(!nameField.nullable) // required in nested object
    
    val ageField = userField.fields.find(_.name == "age").get
    assert(ageField.`type`.contains("integer"))
    assert(ageField.options(MINIMUM) == 0.0)
    assert(ageField.options(MAXIMUM) == 150.0)
    assert(ageField.nullable) // not required in nested object
  }

  test("can convert array schema with primitive items") {
    val schema = JsonSchemaDefinition(
      `type` = Some("array"),
      items = Some(JsonSchemaProperty(
        `type` = Some("string"),
        pattern = Some("^[a-z]+$")
      )),
      minItems = Some(1),
      maxItems = Some(10),
      uniqueItems = Some(true)
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-array")

    assert(result.size == 1)
    val arrayField = result.head
    assert(arrayField.name == "root_array")
    assert(arrayField.`type`.get.contains("array<string>"))
    assert(arrayField.options(ARRAY_MINIMUM_LENGTH) == 1)
    assert(arrayField.options(ARRAY_MAXIMUM_LENGTH) == 10)
    assert(arrayField.options(IS_UNIQUE) == true)
  }

  test("can convert array schema with object items") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "items" -> JsonSchemaProperty(
          `type` = Some("array"),
          items = Some(JsonSchemaProperty(
            `type` = Some("object"),
            properties = Some(Map(
              "id" -> JsonSchemaProperty(`type` = Some("integer")),
              "value" -> JsonSchemaProperty(`type` = Some("string"))
            )),
            required = Some(List("id"))
          )),
          minItems = Some(2),
          maxItems = Some(5)
        )
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-array-object")

    assert(result.size == 1)
    val itemsField = result.head
    assert(itemsField.name == "items")
    assert(itemsField.`type`.get.contains("array<struct"))
    assert(itemsField.options(ARRAY_MINIMUM_LENGTH) == 2)
    assert(itemsField.options(ARRAY_MAXIMUM_LENGTH) == 5)
    assert(itemsField.fields.size == 2)
    
    val idField = itemsField.fields.find(_.name == "id").get
    assert(!idField.nullable) // required
    
    val valueField = itemsField.fields.find(_.name == "value").get
    assert(valueField.nullable) // not required
  }

  test("can handle allOf composition") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "merged" -> JsonSchemaProperty(
          allOf = Some(List(
            JsonSchemaProperty(
              `type` = Some("object"),
              properties = Some(Map(
                "id" -> JsonSchemaProperty(`type` = Some("integer"))
              )),
              required = Some(List("id"))
            ),
            JsonSchemaProperty(
              `type` = Some("object"),
              properties = Some(Map(
                "name" -> JsonSchemaProperty(`type` = Some("string"))
              )),
              required = Some(List("name"))
            )
          ))
        )
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-allof")

    assert(result.size == 1)
    val mergedField = result.head
    assert(mergedField.name == "merged")
    assert(mergedField.fields.size == 2)
    
    val idField = mergedField.fields.find(_.name == "id").get
    assert(!idField.nullable) // required from first allOf schema
    
    val nameField = mergedField.fields.find(_.name == "name").get
    assert(!nameField.nullable) // required from second allOf schema
  }

  test("can handle oneOf composition") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "contact" -> JsonSchemaProperty(
          oneOf = Some(List(
            JsonSchemaProperty(
              `type` = Some("object"),
              properties = Some(Map(
                "email" -> JsonSchemaProperty(`type` = Some("string"), format = Some("email"))
              ))
            ),
            JsonSchemaProperty(
              `type` = Some("object"),
              properties = Some(Map(
                "phone" -> JsonSchemaProperty(`type` = Some("string"))
              ))
            )
          ))
        )
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-oneof")

    assert(result.size == 1)
    val contactField = result.head
    assert(contactField.name == "contact")
    // oneOf should use first schema option
    assert(contactField.`type`.get.contains("string")) // should be converted to single email field
  }

  test("can handle format constraints") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "email" -> JsonSchemaProperty(`type` = Some("string"), format = Some("email")),
        "website" -> JsonSchemaProperty(`type` = Some("string"), format = Some("uri")),
        "uuid" -> JsonSchemaProperty(`type` = Some("string"), format = Some("uuid")),
        "timestamp" -> JsonSchemaProperty(`type` = Some("string"), format = Some("date-time")),
        "birthday" -> JsonSchemaProperty(`type` = Some("string"), format = Some("date")),
        "ipv4" -> JsonSchemaProperty(`type` = Some("string"), format = Some("ipv4"))
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-formats")

    val emailField = result.find(_.name == "email").get
    assert(emailField.options.contains(REGEX_GENERATOR))
    assert(emailField.options(REGEX_GENERATOR).toString.contains("@"))

    val websiteField = result.find(_.name == "website").get
    assert(websiteField.options.contains(REGEX_GENERATOR))
    assert(websiteField.options(REGEX_GENERATOR).toString.contains("http"))

    val uuidField = result.find(_.name == "uuid").get
    assert(uuidField.options.contains("uuid"))

    val timestampField = result.find(_.name == "timestamp").get
    assert(timestampField.options.contains("type"))
    assert(timestampField.options("type") == "timestamp")

    val birthdayField = result.find(_.name == "birthday").get
    assert(birthdayField.options.contains("type"))
    assert(birthdayField.options("type") == "date")

    val ipv4Field = result.find(_.name == "ipv4").get
    assert(ipv4Field.options.contains(REGEX_GENERATOR))
  }

  test("can handle const and default values") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "version" -> JsonSchemaProperty(`type` = Some("string"), const = Some("1.0")),
        "status" -> JsonSchemaProperty(`type` = Some("string"), `default` = Some("pending"))
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-const-default")

    val versionField = result.find(_.name == "version").get
    assert(versionField.options.contains(ONE_OF_GENERATOR))
    assert(versionField.options(ONE_OF_GENERATOR) == "1.0")

    val statusField = result.find(_.name == "status").get
    assert(statusField.options.contains("default"))
    assert(statusField.options("default") == "pending")
  }

  // TODO: Add file-based tests once parser issue is resolved
  test("can convert schema from file") {
    val schemaPath = getClass.getResource("/sample/schema/simple-user-schema.json").getPath
    val schema = JsonSchemaParser.parseSchema(schemaPath)
    
    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, schemaPath)
    assert(result.nonEmpty)
  }

  test("handles empty schemas gracefully") {
    val schema = JsonSchemaDefinition()

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-empty")

    assert(result.size == 1)
    assert(result.head.name == "root")
    assert(result.head.`type`.contains("string")) // defaults to string
  }

  test("handles unknown types gracefully") {
    val schema = JsonSchemaDefinition(
      `type` = Some("unknown-type")
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-unknown")

    assert(result.size == 1)
    assert(result.head.`type`.contains("string")) // defaults to string
  }

  test("can handle definitions with $ref resolution - simple reference") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "user" -> JsonSchemaProperty(ref = Some("#/definitions/User"))
      )),
      definitions = Some(Map(
        "User" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "id" -> JsonSchemaProperty(`type` = Some("integer")),
            "name" -> JsonSchemaProperty(`type` = Some("string"))
          )),
          required = Some(List("id"))
        )
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-ref-simple")

    assert(result.size == 1)
    val userField = result.head
    assert(userField.name == "user")
    assert(userField.`type`.get.contains("struct"))
    assert(userField.fields.size == 2)
    
    val idField = userField.fields.find(_.name == "id").get
    assert(idField.`type`.contains("integer"))
    assert(!idField.nullable) // required in referenced definition
    
    val nameField = userField.fields.find(_.name == "name").get
    assert(nameField.`type`.contains("string"))
    assert(nameField.nullable) // not required in referenced definition
  }

  test("can handle root-level $ref resolution") {
    val schema = JsonSchemaDefinition(
      ref = Some("#/definitions/Customer"),
      definitions = Some(Map(
        "Customer" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "customerId" -> JsonSchemaProperty(`type` = Some("string"), pattern = Some("^CUST[0-9]+$")),
            "customerName" -> JsonSchemaProperty(`type` = Some("string"), minLength = Some(1), maxLength = Some(100)),
            "active" -> JsonSchemaProperty(`type` = Some("boolean"))
          )),
          required = Some(List("customerId", "customerName"))
        )
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-ref-root")

    assert(result.size == 1)
    val rootField = result.head
    assert(rootField.name == "root")
    assert(rootField.fields.size == 3)
    
    val customerIdField = rootField.fields.find(_.name == "customerId").get
    assert(customerIdField.`type`.contains("string"))
    assert(customerIdField.options.contains(REGEX_GENERATOR))
    assert(!customerIdField.nullable) // required
    
    val customerNameField = rootField.fields.find(_.name == "customerName").get
    assert(customerNameField.`type`.contains("string"))
    assert(customerNameField.options(MINIMUM_LENGTH) == 1)
    assert(customerNameField.options(MAXIMUM_LENGTH) == 100)
    assert(!customerNameField.nullable) // required
    
    val activeField = rootField.fields.find(_.name == "active").get
    assert(activeField.`type`.contains("boolean"))
    assert(activeField.nullable) // not required
  }

  test("can handle nested $ref resolution") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "order" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "orderId" -> JsonSchemaProperty(`type` = Some("string")),
            "customer" -> JsonSchemaProperty(ref = Some("#/definitions/Customer")),
            "items" -> JsonSchemaProperty(
              `type` = Some("array"),
              items = Some(JsonSchemaProperty(ref = Some("#/definitions/OrderItem")))
            )
          )),
          required = Some(List("orderId", "customer"))
        )
      )),
      definitions = Some(Map(
        "Customer" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "id" -> JsonSchemaProperty(`type` = Some("integer")),
            "name" -> JsonSchemaProperty(`type` = Some("string"))
          )),
          required = Some(List("id"))
        ),
        "OrderItem" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "productId" -> JsonSchemaProperty(`type` = Some("string")),
            "quantity" -> JsonSchemaProperty(`type` = Some("integer"), minimum = Some("1")),
            "price" -> JsonSchemaProperty(`type` = Some("number"), minimum = Some("0"))
          )),
          required = Some(List("productId", "quantity"))
        )
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-ref-nested")

    assert(result.size == 1)
    val orderField = result.head
    assert(orderField.name == "order")
    assert(orderField.fields.size == 3)
    
    // Test customer reference resolution
    val customerField = orderField.fields.find(_.name == "customer").get
    assert(customerField.`type`.get.contains("struct"))
    assert(customerField.fields.size == 2)
    assert(!customerField.nullable) // required
    
    val customerIdField = customerField.fields.find(_.name == "id").get
    assert(customerIdField.`type`.contains("integer"))
    assert(!customerIdField.nullable) // required in Customer definition
    
    // Test array items reference resolution
    val itemsField = orderField.fields.find(_.name == "items").get
    assert(itemsField.`type`.get.contains("array<struct"))
    assert(itemsField.fields.size == 3)
    
    val productIdField = itemsField.fields.find(_.name == "productId").get
    assert(productIdField.`type`.contains("string"))
    assert(!productIdField.nullable) // required in OrderItem definition
    
    val quantityField = itemsField.fields.find(_.name == "quantity").get
    assert(quantityField.`type`.contains("integer"))
    assert(quantityField.options(MINIMUM) == 1.0)
    assert(!quantityField.nullable) // required in OrderItem definition
    
    val priceField = itemsField.fields.find(_.name == "price").get
    assert(priceField.`type`.contains("double"))
    assert(priceField.options(MINIMUM) == 0.0)
    assert(priceField.nullable) // not required in OrderItem definition
  }

  test("can handle allOf composition with $ref resolution") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "entity" -> JsonSchemaProperty(
          allOf = Some(List(
            JsonSchemaProperty(ref = Some("#/definitions/BaseEntity")),
            JsonSchemaProperty(ref = Some("#/definitions/TimestampFields")),
            JsonSchemaProperty(
              `type` = Some("object"),
              properties = Some(Map(
                "description" -> JsonSchemaProperty(`type` = Some("string"))
              ))
            )
          ))
        )
      )),
      definitions = Some(Map(
        "BaseEntity" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "id" -> JsonSchemaProperty(`type` = Some("string")),
            "type" -> JsonSchemaProperty(`type` = Some("string"))
          )),
          required = Some(List("id"))
        ),
        "TimestampFields" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "createdAt" -> JsonSchemaProperty(`type` = Some("string"), format = Some("date-time")),
            "updatedAt" -> JsonSchemaProperty(`type` = Some("string"), format = Some("date-time"))
          ))
        )
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-ref-allof")

    assert(result.size == 1)
    val entityField = result.head
    assert(entityField.name == "entity")
    assert(entityField.fields.size == 5) // id, type from BaseEntity + createdAt, updatedAt from TimestampFields + description
    
    val idField = entityField.fields.find(_.name == "id").get
    assert(idField.`type`.contains("string"))
    assert(!idField.nullable) // required from BaseEntity
    
    val typeField = entityField.fields.find(_.name == "type").get
    assert(typeField.`type`.contains("string"))
    assert(typeField.nullable) // not required from BaseEntity
    
    val createdAtField = entityField.fields.find(_.name == "createdAt").get
    assert(createdAtField.`type`.contains("string"))
    assert(createdAtField.nullable) // not required from TimestampFields
    
    val descriptionField = entityField.fields.find(_.name == "description").get
    assert(descriptionField.`type`.contains("string"))
    assert(descriptionField.nullable) // not required
  }

  test("can handle oneOf composition with $ref resolution") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "payment" -> JsonSchemaProperty(
          oneOf = Some(List(
            JsonSchemaProperty(ref = Some("#/definitions/CreditCardPayment")),
            JsonSchemaProperty(ref = Some("#/definitions/BankTransferPayment"))
          ))
        )
      )),
      definitions = Some(Map(
        "CreditCardPayment" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "cardNumber" -> JsonSchemaProperty(`type` = Some("string"), pattern = Some("^[0-9]{16}$")),
            "expiryDate" -> JsonSchemaProperty(`type` = Some("string"), pattern = Some("^[0-9]{2}/[0-9]{2}$"))
          )),
          required = Some(List("cardNumber"))
        ),
        "BankTransferPayment" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "accountNumber" -> JsonSchemaProperty(`type` = Some("string")),
            "routingNumber" -> JsonSchemaProperty(`type` = Some("string"))
          )),
          required = Some(List("accountNumber", "routingNumber"))
        )
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-ref-oneof")

    assert(result.size == 1)
    val paymentField = result.head
    assert(paymentField.name == "payment")
    // oneOf uses first schema (CreditCardPayment)
    assert(paymentField.fields.size == 2)
    
    val cardNumberField = paymentField.fields.find(_.name == "cardNumber").get
    assert(cardNumberField.`type`.contains("string"))
    assert(cardNumberField.options.contains(REGEX_GENERATOR))
    assert(!cardNumberField.nullable) // required in CreditCardPayment
  }

  test("handles missing reference gracefully") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "user" -> JsonSchemaProperty(ref = Some("#/definitions/NonExistentUser"))
      )),
      definitions = Some(Map(
        "Customer" -> JsonSchemaProperty(`type` = Some("string"))
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-ref-missing")

    // Should handle missing reference gracefully and create a string field
    assert(result.size == 1)
    val userField = result.head
    assert(userField.name == "user")
    assert(userField.`type`.contains("string")) // fallback to string type
  }

  test("handles complex MX schema structure with references") {
    // Simplified version of the MX schema structure
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "customer_direct_debit_initiation_v11" -> JsonSchemaProperty(ref = Some("#/definitions/CustomerDirectDebitInitiationV11"))
      )),
      definitions = Some(Map(
        "CustomerDirectDebitInitiationV11" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "group_header" -> JsonSchemaProperty(ref = Some("#/definitions/GroupHeader118")),
            "payment_information" -> JsonSchemaProperty(
              `type` = Some("array"),
              items = Some(JsonSchemaProperty(ref = Some("#/definitions/PaymentInstruction45")))
            )
          )),
          required = Some(List("group_header", "payment_information"))
        ),
        "GroupHeader118" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "message_identification" -> JsonSchemaProperty(ref = Some("#/definitions/Max35Text")),
            "creation_date_time" -> JsonSchemaProperty(ref = Some("#/definitions/ISODateTime")),
            "number_of_transactions" -> JsonSchemaProperty(ref = Some("#/definitions/Max15NumericText"))
          )),
          required = Some(List("message_identification", "creation_date_time", "number_of_transactions"))
        ),
        "PaymentInstruction45" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "payment_information_identification" -> JsonSchemaProperty(ref = Some("#/definitions/Max35Text")),
            "payment_method" -> JsonSchemaProperty(ref = Some("#/definitions/PaymentMethod2Code"))
          )),
          required = Some(List("payment_information_identification", "payment_method"))
        ),
        "Max35Text" -> JsonSchemaProperty(
          `type` = Some("string"),
          minLength = Some(1),
          maxLength = Some(35)
        ),
        "Max15NumericText" -> JsonSchemaProperty(
          `type` = Some("string"),
          pattern = Some("^[0-9]{1,15}$")
        ),
        "ISODateTime" -> JsonSchemaProperty(
          `type` = Some("string"),
          pattern = Some("^(?:[1-9]\\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.[0-9]+)?(?:Z|[+-][01]\\d:[0-5]\\d)?$")
        ),
        "PaymentMethod2Code" -> JsonSchemaProperty(
          `type` = Some("string"),
          `enum` = Some(List("DD"))
        )
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-mx-schema")

    assert(result.size == 1)
    val rootField = result.head
    assert(rootField.name == "customer_direct_debit_initiation_v11")
    assert(rootField.fields.size == 2)
    
    // Test group_header resolution
    val groupHeaderField = rootField.fields.find(_.name == "group_header").get
    assert(groupHeaderField.`type`.get.contains("struct"))
    assert(groupHeaderField.fields.size == 3)
    assert(!groupHeaderField.nullable) // required
    
    val messageIdField = groupHeaderField.fields.find(_.name == "message_identification").get
    assert(messageIdField.`type`.contains("string"))
    assert(messageIdField.options(MINIMUM_LENGTH) == 1)
    assert(messageIdField.options(MAXIMUM_LENGTH) == 35)
    assert(!messageIdField.nullable) // required
    
    // Test payment_information array resolution
    val paymentInfoField = rootField.fields.find(_.name == "payment_information").get
    assert(paymentInfoField.`type`.get.contains("array"), "payment_information should be array type")
    assert(paymentInfoField.fields.nonEmpty, "payment_information array should have item fields from resolved reference")
  }

  test("can generate data for oneOf composition with struct objects") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "notification" -> JsonSchemaProperty(
          oneOf = Some(List(
            JsonSchemaProperty(
              `type` = Some("object"),
              properties = Some(Map(
                "email" -> JsonSchemaProperty(`type` = Some("string"), format = Some("email")),
                "priority" -> JsonSchemaProperty(`type` = Some("string"), `enum` = Some(List("high", "medium", "low")))
              )),
              required = Some(List("email"))
            ),
            JsonSchemaProperty(
              `type` = Some("object"),
              properties = Some(Map(
                "phone" -> JsonSchemaProperty(`type` = Some("string"), pattern = Some("^\\+[1-9]\\d{1,14}$")),
                "sms_text" -> JsonSchemaProperty(`type` = Some("string"), minLength = Some(1), maxLength = Some(160))
              )),
              required = Some(List("phone", "sms_text"))
            )
          ))
        )
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-oneof-struct")

    assert(result.size == 1)
    val notificationField = result.head
    assert(notificationField.name == "notification")
    
    // oneOf should use first schema option (email notification)
    assert(notificationField.fields.size == 2)
    
    val emailField = notificationField.fields.find(_.name == "email").get
    assert(emailField.`type`.contains("string"))
    assert(emailField.options.contains(REGEX_GENERATOR)) // email format creates regex
    assert(!emailField.nullable) // required
    
    val priorityField = notificationField.fields.find(_.name == "priority").get  
    assert(priorityField.`type`.contains("string"))
    assert(priorityField.options.contains(ONE_OF_GENERATOR))
    assert(priorityField.options(ONE_OF_GENERATOR) == "high,medium,low")
    assert(priorityField.nullable) // not required
  }

  test("array field constraints are correctly parsed and preserved") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "users" -> JsonSchemaProperty(
          `type` = Some("array"),
          items = Some(JsonSchemaProperty(
            `type` = Some("object"),
            properties = Some(Map(
              "id" -> JsonSchemaProperty(`type` = Some("integer"), minimum = Some("1"), maximum = Some("1000")),
              "status" -> JsonSchemaProperty(`type` = Some("string"), `enum` = Some(List("active", "inactive", "pending"))),
              "email" -> JsonSchemaProperty(`type` = Some("string"), format = Some("email")),
              "age" -> JsonSchemaProperty(`type` = Some("integer"), minimum = Some("18"), maximum = Some("65"))
            ))
          ))
        )
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test_schema")
    
    // Find the users array field
    val usersField = result.find(_.name == "users")
    assert(usersField.isDefined, "Users field should be present")
    
    // Check that it's an array type
    assert(usersField.get.`type`.get.startsWith("array"), s"Users field should be array type, got: ${usersField.get.`type`}")
    
    // Check nested fields have correct constraints
    val userFields = usersField.get.fields
    assert(userFields.nonEmpty, "Users array should have nested fields")
    
    // Check status field has enum constraint as comma-separated string
    val statusField = userFields.find(_.name == "status")
    assert(statusField.isDefined, "Status field should be present")
    assert(statusField.get.options.contains("oneOf"), "Status field should have oneOf constraint")
    assert(statusField.get.options("oneOf") == "active,inactive,pending", 
      s"Status field should have comma-separated enum values, got: ${statusField.get.options("oneOf")}")
    
    // Check id field has min/max constraints
    val idField = userFields.find(_.name == "id")
    assert(idField.isDefined, "ID field should be present")
    assert(idField.get.options.contains("min"), "ID field should have min constraint")
    assert(idField.get.options.contains("max"), "ID field should have max constraint")
    assert(idField.get.options("min") == 1.0, s"ID min should be 1.0, got: ${idField.get.options("min")}")
    assert(idField.get.options("max") == 1000.0, s"ID max should be 1000.0, got: ${idField.get.options("max")}")
    
    // Check age field has min/max constraints
    val ageField = userFields.find(_.name == "age")
    assert(ageField.isDefined, "Age field should be present")
    assert(ageField.get.options.contains("min"), "Age field should have min constraint")
    assert(ageField.get.options.contains("max"), "Age field should have max constraint")
    assert(ageField.get.options("min") == 18.0, s"Age min should be 18.0, got: ${ageField.get.options("min")}")
    assert(ageField.get.options("max") == 65.0, s"Age max should be 65.0, got: ${ageField.get.options("max")}")
    
    // Check email field has format constraint
    val emailField = userFields.find(_.name == "email")
    assert(emailField.isDefined, "Email field should be present")
    // Email format should be converted to regex pattern
    assert(emailField.get.options.contains("regex"), "Email field should have regex constraint")
    
    println("✅ Array field constraints correctly parsed and preserved!")
  }

  test("nested array constraints with multiple levels are preserved") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "companies" -> JsonSchemaProperty(
          `type` = Some("array"),
          items = Some(JsonSchemaProperty(
            `type` = Some("object"),
            properties = Some(Map(
              "company_id" -> JsonSchemaProperty(`type` = Some("string"), pattern = Some("^COMP-[0-9]{4}$")),
              "departments" -> JsonSchemaProperty(
                `type` = Some("array"),
                items = Some(JsonSchemaProperty(
                  `type` = Some("object"),
                  properties = Some(Map(
                    "dept_id" -> JsonSchemaProperty(`type` = Some("integer"), minimum = Some("100"), maximum = Some("999")),
                    "employees" -> JsonSchemaProperty(
                      `type` = Some("array"),
                      items = Some(JsonSchemaProperty(
                        `type` = Some("object"),
                        properties = Some(Map(
                          "emp_id" -> JsonSchemaProperty(`type` = Some("string"), pattern = Some("^EMP-[0-9]{6}$")),
                          "salary" -> JsonSchemaProperty(`type` = Some("number"), minimum = Some("30000"), maximum = Some("200000"))
                        )),
                        required = Some(List("emp_id"))
                      )),
                      minItems = Some(1),
                      maxItems = Some(10)
                    )
                  )),
                  required = Some(List("dept_id"))
                )),
                minItems = Some(1),
                maxItems = Some(5)
              )
            )),
            required = Some(List("company_id"))
          )),
          minItems = Some(1),
          maxItems = Some(3)
        )
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-nested-array-constraints")

    assert(result.size == 1)
    val companiesField = result.head
    assert(companiesField.name == "companies")
    assert(companiesField.`type`.get.contains("array"))
    
    // Top-level array constraints
    assert(companiesField.options(ARRAY_MINIMUM_LENGTH) == 1)
    assert(companiesField.options(ARRAY_MAXIMUM_LENGTH) == 3)
    
    // Company-level fields
    assert(companiesField.fields.size == 2)
    val companyIdField = companiesField.fields.find(_.name == "company_id").get
    assert(companyIdField.options.contains(REGEX_GENERATOR), s"Company ID regex not preserved: ${companyIdField.options}")
    assert(companyIdField.options(REGEX_GENERATOR) == "^COMP-[0-9]{4}$")
    assert(!companyIdField.nullable) // required
    
    // Departments array
    val departmentsField = companiesField.fields.find(_.name == "departments").get
    assert(departmentsField.`type`.get.contains("array"))
    assert(departmentsField.options(ARRAY_MINIMUM_LENGTH) == 1)
    assert(departmentsField.options(ARRAY_MAXIMUM_LENGTH) == 5)
    
    // Department-level fields
    assert(departmentsField.fields.size == 2)
    val deptIdField = departmentsField.fields.find(_.name == "dept_id").get
    assert(deptIdField.options(MINIMUM) == 100.0, s"Dept ID minimum not preserved: ${deptIdField.options}")
    assert(deptIdField.options(MAXIMUM) == 999.0, s"Dept ID maximum not preserved: ${deptIdField.options}")
    assert(!deptIdField.nullable) // required
    
    // Employees array
    val employeesField = departmentsField.fields.find(_.name == "employees").get
    assert(employeesField.`type`.get.contains("array"))
    assert(employeesField.options(ARRAY_MINIMUM_LENGTH) == 1)
    assert(employeesField.options(ARRAY_MAXIMUM_LENGTH) == 10)
    
    // Employee-level fields
    assert(employeesField.fields.size == 2)
    val empIdField = employeesField.fields.find(_.name == "emp_id").get
    assert(empIdField.options.contains(REGEX_GENERATOR), s"Employee ID regex not preserved: ${empIdField.options}")
    assert(empIdField.options(REGEX_GENERATOR) == "^EMP-[0-9]{6}$")
    assert(!empIdField.nullable) // required
    
    val salaryField = employeesField.fields.find(_.name == "salary").get
    assert(salaryField.options(MINIMUM) == 30000.0, s"Salary minimum not preserved: ${salaryField.options}")
    assert(salaryField.options(MAXIMUM) == 200000.0, s"Salary maximum not preserved: ${salaryField.options}")
    assert(salaryField.nullable) // not required
  }

  test("empty struct fields issue investigation - simple case") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "transaction_info" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "transaction_id" -> JsonSchemaProperty(`type` = Some("string"), pattern = Some("^TXN-[0-9]{8}$")),
            "amount" -> JsonSchemaProperty(`type` = Some("number"), minimum = Some("0.01"), maximum = Some("10000.00")),
            "currency" -> JsonSchemaProperty(`type` = Some("string"), `enum` = Some(List("USD", "EUR", "GBP")))
          )),
          required = Some(List("transaction_id", "amount"))
        )
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-empty-struct-simple")

    assert(result.size == 1)
    val transactionInfoField = result.head
    assert(transactionInfoField.name == "transaction_info")
    assert(transactionInfoField.`type`.get.contains("struct"))
    
    // This should NOT be empty - if it is, we have the bug
    assert(transactionInfoField.fields.nonEmpty, s"transaction_info struct should have fields, but got empty fields list. Type: ${transactionInfoField.`type`}")
    assert(transactionInfoField.fields.size == 3, s"Expected 3 fields in struct, got ${transactionInfoField.fields.size}: ${transactionInfoField.fields.map(_.name)}")
    
    val transactionIdField = transactionInfoField.fields.find(_.name == "transaction_id").get
    assert(transactionIdField.options.contains(REGEX_GENERATOR), s"Transaction ID regex not preserved: ${transactionIdField.options}")
    assert(!transactionIdField.nullable) // required
    
    val amountField = transactionInfoField.fields.find(_.name == "amount").get
    assert(amountField.options(MINIMUM) == 0.01, s"Amount minimum not preserved: ${amountField.options}")
    assert(amountField.options(MAXIMUM) == 10000.0, s"Amount maximum not preserved: ${amountField.options}")
    assert(!amountField.nullable) // required
    
    val currencyField = transactionInfoField.fields.find(_.name == "currency").get
    assert(currencyField.options.contains(ONE_OF_GENERATOR), s"Currency enum not preserved: ${currencyField.options}")
    assert(currencyField.nullable) // not required
  }

  test("empty struct fields issue investigation - with $ref resolution") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "direct_debit_transaction_information" -> JsonSchemaProperty(ref = Some("#/definitions/DirectDebitTransactionInformation45"))
      )),
      definitions = Some(Map(
        "DirectDebitTransactionInformation45" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "payment_identification" -> JsonSchemaProperty(ref = Some("#/definitions/PaymentIdentification13")),
            "instructed_amount" -> JsonSchemaProperty(ref = Some("#/definitions/ActiveOrHistoricCurrencyAndAmount")),
            "charge_bearer" -> JsonSchemaProperty(ref = Some("#/definitions/ChargeBearerType1Code"))
          )),
          required = Some(List("payment_identification", "instructed_amount"))
        ),
        "PaymentIdentification13" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "instruction_identification" -> JsonSchemaProperty(ref = Some("#/definitions/Max35Text")),
            "end_to_end_identification" -> JsonSchemaProperty(ref = Some("#/definitions/Max35Text"))
          )),
          required = Some(List("instruction_identification"))
        ),
        "ActiveOrHistoricCurrencyAndAmount" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "currency" -> JsonSchemaProperty(`type` = Some("string"), pattern = Some("^[A-Z]{3}$")),
                         "amount" -> JsonSchemaProperty(`type` = Some("number"), minimum = Some("0"))
          )),
          required = Some(List("currency", "amount"))
        ),
        "ChargeBearerType1Code" -> JsonSchemaProperty(
          `type` = Some("string"),
          `enum` = Some(List("DEBT", "CRED", "SHAR", "SLEV"))
        ),
        "Max35Text" -> JsonSchemaProperty(
          `type` = Some("string"),
          minLength = Some(1),
          maxLength = Some(35)
        )
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-empty-struct-ref")

    assert(result.size == 1)
    val directDebitField = result.head
    assert(directDebitField.name == "direct_debit_transaction_information")
    assert(directDebitField.`type`.get.contains("struct"))
    
    // This is the key test - if this fails, we have the empty struct bug
    assert(directDebitField.fields.nonEmpty, s"direct_debit_transaction_information should have fields from resolved reference, but got empty fields list. Type: ${directDebitField.`type`}")
    assert(directDebitField.fields.size == 3, s"Expected 3 fields from resolved reference, got ${directDebitField.fields.size}: ${directDebitField.fields.map(_.name)}")
    
    // Test nested reference resolution
    val paymentIdField = directDebitField.fields.find(_.name == "payment_identification").get
    assert(paymentIdField.`type`.get.contains("struct"))
    assert(paymentIdField.fields.nonEmpty, s"payment_identification should have nested fields from resolved reference")
    assert(!paymentIdField.nullable) // required
    
    val instructedAmountField = directDebitField.fields.find(_.name == "instructed_amount").get
    assert(instructedAmountField.`type`.get.contains("struct"))
    assert(instructedAmountField.fields.nonEmpty, s"instructed_amount should have nested fields from resolved reference")
    assert(!instructedAmountField.nullable) // required
    
    // Test that constraints are preserved through reference resolution
    val currencyField = instructedAmountField.fields.find(_.name == "currency").get
    assert(currencyField.options.contains(REGEX_GENERATOR), s"Currency pattern not preserved through reference: ${currencyField.options}")
    assert(currencyField.options(REGEX_GENERATOR) == "^[A-Z]{3}$")
    
    val amountField = instructedAmountField.fields.find(_.name == "amount").get
    assert(amountField.options(MINIMUM) == 0.0, s"Amount minimum not preserved through reference: ${amountField.options}")
    
    val chargeBearerField = directDebitField.fields.find(_.name == "charge_bearer").get
    assert(chargeBearerField.`type`.contains("string"))
    assert(chargeBearerField.options.contains(ONE_OF_GENERATOR), s"Charge bearer enum not preserved through reference: ${chargeBearerField.options}")
    assert(chargeBearerField.options(ONE_OF_GENERATOR) == "DEBT,CRED,SHAR,SLEV")
  }

  test("array with $ref items constraints preservation") {
    val schema = JsonSchemaDefinition(
      `type` = Some("object"),
      properties = Some(Map(
        "payment_information" -> JsonSchemaProperty(
          `type` = Some("array"),
          items = Some(JsonSchemaProperty(ref = Some("#/definitions/PaymentInstruction45"))),
          minItems = Some(1),
          maxItems = Some(100)
        )
      )),
      definitions = Some(Map(
        "PaymentInstruction45" -> JsonSchemaProperty(
          `type` = Some("object"),
          properties = Some(Map(
            "payment_information_identification" -> JsonSchemaProperty(ref = Some("#/definitions/Max35Text")),
            "payment_method" -> JsonSchemaProperty(ref = Some("#/definitions/PaymentMethod2Code")),
            "batch_booking" -> JsonSchemaProperty(`type` = Some("boolean")),
            "number_of_transactions" -> JsonSchemaProperty(ref = Some("#/definitions/Max15NumericText")),
            "control_sum" -> JsonSchemaProperty(`type` = Some("number"), minimum = Some("0"))
          )),
          required = Some(List("payment_information_identification", "payment_method"))
        ),
        "Max35Text" -> JsonSchemaProperty(
          `type` = Some("string"),
          minLength = Some(1),
          maxLength = Some(35)
        ),
        "PaymentMethod2Code" -> JsonSchemaProperty(
          `type` = Some("string"),
          `enum` = Some(List("DD"))
        ),
        "Max15NumericText" -> JsonSchemaProperty(
          `type` = Some("string"),
          pattern = Some("^[0-9]{1,15}$")
        )
      ))
    )

    val result = JsonSchemaConverter.convertSchemaWithRefs(schema, "test-array-ref-constraints")

    assert(result.size == 1)
    val paymentInfoField = result.head
    assert(paymentInfoField.name == "payment_information")
    assert(paymentInfoField.`type`.get.contains("array"))
    
    // Array-level constraints
    assert(paymentInfoField.options(ARRAY_MINIMUM_LENGTH) == 1)
    assert(paymentInfoField.options(ARRAY_MAXIMUM_LENGTH) == 100)
    
    // Array items should have fields from resolved reference
    assert(paymentInfoField.fields.nonEmpty, s"payment_information array should have item fields from resolved reference, but got empty fields")
    assert(paymentInfoField.fields.size == 5, s"Expected 5 fields from resolved PaymentInstruction45, got ${paymentInfoField.fields.size}: ${paymentInfoField.fields.map(_.name)}")
    
    // Test constraints preservation through reference resolution
    val paymentIdField = paymentInfoField.fields.find(_.name == "payment_information_identification").get
    assert(paymentIdField.`type`.contains("string"))
    assert(paymentIdField.options(MINIMUM_LENGTH) == 1, s"Payment ID minLength not preserved: ${paymentIdField.options}")
    assert(paymentIdField.options(MAXIMUM_LENGTH) == 35, s"Payment ID maxLength not preserved: ${paymentIdField.options}")
    assert(!paymentIdField.nullable) // required
    
    val paymentMethodField = paymentInfoField.fields.find(_.name == "payment_method").get
    assert(paymentMethodField.`type`.contains("string"))
    assert(paymentMethodField.options.contains(ONE_OF_GENERATOR), s"Payment method enum not preserved: ${paymentMethodField.options}")
    assert(paymentMethodField.options(ONE_OF_GENERATOR) == "DD")
    assert(!paymentMethodField.nullable) // required
    
    val numTransactionsField = paymentInfoField.fields.find(_.name == "number_of_transactions").get
    assert(numTransactionsField.`type`.contains("string"))
    assert(numTransactionsField.options.contains(REGEX_GENERATOR), s"Number of transactions pattern not preserved: ${numTransactionsField.options}")
    assert(numTransactionsField.options(REGEX_GENERATOR) == "^[0-9]{1,15}$")
    assert(numTransactionsField.nullable) // not required
    
    val controlSumField = paymentInfoField.fields.find(_.name == "control_sum").get
    assert(controlSumField.`type`.contains("double"))
    assert(controlSumField.options(MINIMUM) == 0.0, s"Control sum minimum not preserved: ${controlSumField.options}")
    assert(controlSumField.nullable) // not required
  }

  private def countTotalFields(fields: List[Field]): Int = {
    fields.size + fields.map(f => countTotalFields(f.fields)).sum
  }
} 