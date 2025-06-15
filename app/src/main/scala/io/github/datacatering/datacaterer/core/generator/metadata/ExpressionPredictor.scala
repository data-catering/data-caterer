package io.github.datacatering.datacaterer.core.generator.metadata

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.{DateType, TimestampType}
import io.github.datacatering.datacaterer.core.model.FieldPrediction
import net.datafaker.providers.base.AbstractProvider
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{ArrayType, DataType, MetadataBuilder, StringType, StructField, StructType}
import org.reflections.Reflections

import scala.collection.JavaConverters.asScalaSetConverter

object ExpressionPredictor {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val DEFAULT_METHODS = List("toString")

  def getAllFakerExpressionTypes: List[String] = {
    val reflections = new Reflections("net.datafaker.providers")
    val allFakerDataProviders = reflections.getSubTypesOf(classOf[AbstractProvider[_]]).asScala
    allFakerDataProviders.flatMap(c => {
      c.getMethods
        .filter(m => isStringType(m.getReturnType) && !DEFAULT_METHODS.contains(m.getName))
        .map(m => s"${c.getSimpleName}.${m.getName}")
    }).toList
  }

  def getFieldPredictions(structField: StructField): StructField = {
    if (structField.dataType.typeName == "struct") {
      val updatedDataType: StructType = structTypeWithFieldPredictions(structField.dataType)
      StructField(structField.name, updatedDataType, structField.nullable, structField.metadata)
    } else if (structField.dataType.typeName == "array") {
      val nestedType = structField.dataType.asInstanceOf[ArrayType].elementType
      if (nestedType.typeName == "struct") {
        val updatedNestedDataType = structTypeWithFieldPredictions(nestedType)
        val updatedArrayType = ArrayType(updatedNestedDataType)
        StructField(structField.name, updatedArrayType, structField.nullable, structField.metadata)
      } else {
        structField
      }
    } else if (structField.dataType.typeName == "string") {
      // Check if field already has constraints from metadata sources
      val hasExistingConstraints = hasFieldConstraints(structField)
      
      if (hasExistingConstraints) {
        // Field already has constraints from data source (e.g. JSON schema), preserve them
        LOGGER.info(s"Field '${structField.name}' already has metadata constraints, skipping expression prediction, ${structField.metadata.json}")
        structField
      } else {
        // No existing constraints, apply expression predictions
        val optFieldPrediction = tryGetFieldPrediction(structField)
        val metadata = optFieldPrediction.map(prediction => {
          val metadataBuilder = new MetadataBuilder().withMetadata(structField.metadata)
          prediction.toMap.foreach(p => metadataBuilder.putString(p._1, p._2))
          metadataBuilder.build()
        }).getOrElse(structField.metadata)
        val updatedDataType = if (metadata.contains(FIELD_DATA_TYPE) && metadata.getString(FIELD_DATA_TYPE) != "string") {
          DataType.fromDDL(metadata.getString(FIELD_DATA_TYPE))
        } else {
          structField.dataType
        }
        StructField(structField.name, updatedDataType, structField.nullable, metadata)
      }
    } else {
      structField
    }
  }

  /**
   * Check if a field already has constraints that indicate it comes from a data source
   * like JSON schema, database metadata, etc.
   */
  private def hasFieldConstraints(structField: StructField): Boolean = {
    val metadata = structField.metadata
    // Check for common field constraint metadata keys that indicate data source constraints
    metadata.contains(MINIMUM_LENGTH) ||
    metadata.contains(MAXIMUM_LENGTH) ||
    metadata.contains(MINIMUM) ||
    metadata.contains(MAXIMUM) ||
    metadata.contains(ONE_OF_GENERATOR) ||
    metadata.contains(REGEX_GENERATOR) ||
    metadata.contains(EXPRESSION) ||
    metadata.contains(DEFAULT_VALUE) ||
    metadata.contains(CONSTRAINT_TYPE) ||
    metadata.contains(STATIC) ||
    metadata.contains(NULL_COUNT) ||
    metadata.contains(DISTINCT_COUNT) ||
    metadata.contains(HISTOGRAM) ||
    metadata.contains(AVERAGE_LENGTH) ||
    metadata.contains(STANDARD_DEVIATION) ||
    metadata.contains(MEAN) ||
    metadata.contains(DISTRIBUTION) ||
    metadata.contains(ARRAY_MINIMUM_LENGTH) ||
    metadata.contains(ARRAY_MAXIMUM_LENGTH) ||
    metadata.contains(MAP_MINIMUM_SIZE) ||
    metadata.contains(MAP_MAXIMUM_SIZE)
  }

  private def structTypeWithFieldPredictions(dataType: DataType): StructType = {
    val nestedFields = dataType.asInstanceOf[StructType].fields
    val updatedFields = nestedFields.map(field => {
      getFieldPredictions(field)
    })
    StructType(updatedFields)
  }

  def tryGetFieldPrediction(structField: StructField): Option[FieldPrediction] = {
    if (structField.dataType == StringType) {
      val cleanFieldName = structField.name.toLowerCase.replaceAll("[^a-z0-9]", "")
      val optExpression = cleanFieldName match {
        case "firstname" => Some(FieldPrediction(FAKER_EXPR_FIRST_NAME, LABEL_NAME, true))
        case "lastname" => Some(FieldPrediction(FAKER_EXPR_LAST_NAME, LABEL_NAME, true))
        case "username" => Some(FieldPrediction(FAKER_EXPR_USERNAME, LABEL_USERNAME, true))
        case "name" | "fullname" => Some(FieldPrediction(FAKER_EXPR_NAME, LABEL_NAME, true))
        case "postcode" => Some(FieldPrediction(FAKER_EXPR_ADDRESS_POSTCODE, LABEL_ADDRESS, false))
        case "city" => Some(FieldPrediction(FAKER_EXPR_CITY, LABEL_ADDRESS, false))
        case "country" => Some(FieldPrediction(FAKER_EXPR_COUNTRY, LABEL_ADDRESS, false))
        case "countrycode" => Some(FieldPrediction(FAKER_EXPR_COUNTRY_CODE, LABEL_ADDRESS, false))
        case "nationality" => Some(FieldPrediction(FAKER_EXPR_NATIONALITY, LABEL_NATION, false))
        case "language" => Some(FieldPrediction(FAKER_EXPR_LANGUAGE, LABEL_NATION, false))
        case "capital" | "capitalcity" => Some(FieldPrediction(FAKER_EXPR_CAPITAL, LABEL_NATION, false))
        case "version" => Some(FieldPrediction(FAKER_EXPR_APP_VERSION, LABEL_APP, false))
        case "paymentmethod" => Some(FieldPrediction(FAKER_EXPR_PAYMENT_METHODS, LABEL_MONEY, false))
        case "macaddress" | "macaddr" => Some(FieldPrediction(FAKER_EXPR_MAC_ADDRESS, LABEL_INTERNET, true))
        case "currency" => Some(FieldPrediction(FAKER_EXPR_CURRENCY, LABEL_MONEY, false))
        case "currencycode" => Some(FieldPrediction(FAKER_EXPR_CURRENCY_CODE, LABEL_MONEY, false))
        case "creditcard" => Some(FieldPrediction(FAKER_EXPR_CREDIT_CARD, LABEL_MONEY, true))
        case "food" | "dish" => Some(FieldPrediction(FAKER_EXPR_FOOD, LABEL_FOOD, false))
        case "ingredient" => Some(FieldPrediction(FAKER_EXPR_FOOD_INGREDIENT, LABEL_FOOD, false))
        case "jobfield" => Some(FieldPrediction(FAKER_EXPR_JOB_FIELD, LABEL_JOB, false))
        case "jobposition" => Some(FieldPrediction(FAKER_EXPR_JOB_POSITION, LABEL_JOB, false))
        case "jobtitle" => Some(FieldPrediction(FAKER_EXPR_JOB_TITLE, LABEL_JOB, false))
        case "relationship" => Some(FieldPrediction(FAKER_EXPR_RELATIONSHIP, LABEL_RELATIONSHIP, false))
        case "weather" => Some(FieldPrediction(FAKER_EXPR_WEATHER, LABEL_WEATHER, false))
        case "cellphone" | "mobilephone" | "homephone" | "phone" => Some(FieldPrediction(FAKER_EXPR_PHONE, LABEL_PHONE, true))
        case x if x.contains("email") => Some(FieldPrediction(FAKER_EXPR_EMAIL, LABEL_INTERNET, true))
        case x if x.contains("ipv4") => Some(FieldPrediction(FAKER_EXPR_IPV4, LABEL_INTERNET, true))
        case x if x.contains("ipv6") => Some(FieldPrediction(FAKER_EXPR_IPV6, LABEL_INTERNET, true))
        case x if x.contains("address") => Some(FieldPrediction(FAKER_EXPR_ADDRESS, LABEL_ADDRESS, true))
        case x if x.contains("datetime") => Some(FieldPrediction("", "", false, Map(FIELD_DATA_TYPE -> TimestampType.toString)))
        case x if x.contains("date") => Some(FieldPrediction("", "", false, Map(FIELD_DATA_TYPE -> DateType.toString)))
        case _ => None
      }
      if (optExpression.isDefined) {
        LOGGER.debug(s"Identified field as a faker expression, field-name=${structField.name}, expression=${optExpression.get}")
      }
      optExpression.map(e => e.copy(fakerExpression = s"#{${e.fakerExpression}}"))
    } else {
      None
    }
  }

  private def isStringType(clazz: Class[_]): Boolean = {
    clazz.getSimpleName match {
      case "String" => true
      case _ => false
    }
  }

}
