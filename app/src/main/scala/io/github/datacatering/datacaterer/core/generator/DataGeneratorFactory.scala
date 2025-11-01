package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.Constants.{ALL_COMBINATIONS, EXCLUDE_FIELDS, EXCLUDE_FIELD_PATTERNS, INCLUDE_FIELDS, INCLUDE_FIELD_PATTERNS, INDEX_INC_FIELD, ONE_OF_GENERATOR, SQL_GENERATOR, STATIC}
import io.github.datacatering.datacaterer.api.model.{Field, PerFieldCount, Step}
import io.github.datacatering.datacaterer.core.exception.InvalidStepCountGeneratorConfigurationException
import io.github.datacatering.datacaterer.core.generator.provider.DataGenerator
import io.github.datacatering.datacaterer.core.generator.provider.OneOfDataGenerator.RandomOneOfDataGenerator
import io.github.datacatering.datacaterer.core.generator.provider.RandomDataGenerator.RandomLongDataGenerator
import io.github.datacatering.datacaterer.core.model.Constants._
import io.github.datacatering.datacaterer.core.sink.SinkProcessor
import io.github.datacatering.datacaterer.core.util.GeneratorUtil.{applySqlExpressions, getDataGenerator}
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import io.github.datacatering.datacaterer.core.util.PlanImplicits.FieldOps
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec
import scala.util.Random


case class Holder(__index_inc: Long)

class DataGeneratorFactory(faker: Faker, enableFastGeneration: Boolean = false)(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.jsonObjectMapper
  registerSparkFunctions()

  if (enableFastGeneration) {
    LOGGER.info("Fast generation mode enabled - using SQL-only generators for maximum performance")
  }

  def generateDataForStep(step: Step, dataSourceName: String, startIndex: Long, endIndex: Long): DataFrame = {
    val structFieldsWithDataGenerators = getStructWithGenerators(step.fields, step.options)
    val indexedDf = sparkSession.createDataFrame(Seq.range(startIndex, endIndex).map(Holder))
    generateDataViaSql(structFieldsWithDataGenerators, step, indexedDf)
      .alias(s"$dataSourceName.${step.name}")
  }

  private def generateDataViaSql(dataGenerators: List[DataGenerator[_]], step: Step, indexedDf: DataFrame): DataFrame = {
    val structType = StructType(dataGenerators.map(_.structField))
    SinkProcessor.validateSchema(step.`type`, structType)

    val allRecordsDf = if (step.options.contains(ALL_COMBINATIONS) && step.options(ALL_COMBINATIONS).equalsIgnoreCase("true")) {
      generateCombinationRecords(dataGenerators, indexedDf)
    } else {
      val genSqlExpression = dataGenerators.map(dg => s"${dg.generateSqlExpressionWrapper} AS `${dg.structField.name}`")
      val df = indexedDf.selectExpr(genSqlExpression: _*)

      step.count.perField
        .map(perCol => generateRecordsPerField(dataGenerators, step, perCol, df))
        .getOrElse(df)
    }

    val dfWithMetadata = attachMetadata(allRecordsDf, structType)

    // Apply SQL expressions while all fields (including omitted helper fields) are still available
    val dfWithSqlExpressions = applySqlExpressions(dfWithMetadata)

    // Attach final metadata after SQL expressions
    val dfAllFields = attachMetadata(dfWithSqlExpressions, structType)

    // Only remove the index field here, leave other omitted fields for SinkFactory
    val finalDf = dfAllFields.drop(INDEX_INC_FIELD)

    // Single cache point - only cache final result
    if (!finalDf.storageLevel.useMemory) finalDf.cache()
    finalDf
  }

  private def generateRecordsPerField(dataGenerators: List[DataGenerator[_]], step: Step,
                                      perFieldCount: PerFieldCount, df: DataFrame): DataFrame = {
    val fieldsToBeGenerated = dataGenerators.filter(x => !perFieldCount.fieldNames.contains(x.structField.name))

    if (fieldsToBeGenerated.isEmpty) {
      LOGGER.warn(s"Per field count defined but no other fields to generate")
      df
    } else {
      val metadata = if (perFieldCount.options.nonEmpty) {
        Metadata.fromJson(OBJECT_MAPPER.writeValueAsString(perFieldCount.options))
      } else if (perFieldCount.count.isDefined) {
        Metadata.fromJson(OBJECT_MAPPER.writeValueAsString(Map(STATIC -> perFieldCount.count.get.toString)))
      } else {
        throw InvalidStepCountGeneratorConfigurationException(step)
      }

      val countStructField = StructField(RECORD_COUNT_GENERATOR_FIELD, IntegerType, false, metadata)
      val generatedCount = getDataGenerator(perFieldCount.options, countStructField, faker, enableFastGeneration).asInstanceOf[DataGenerator[Int]]
      val perFieldRange = generateDataWithSchemaSql(df, generatedCount, fieldsToBeGenerated)

      val explodeCount = perFieldRange.withColumn(PER_FIELD_INDEX_FIELD, explode(col(PER_FIELD_COUNT)))
        .drop(col(PER_FIELD_COUNT))
      explodeCount.select(PER_FIELD_INDEX_FIELD + ".*", perFieldCount.fieldNames: _*)
    }
  }

  private def generateCombinationRecords(dataGenerators: List[DataGenerator[_]], indexedDf: DataFrame) = {
    LOGGER.debug("Attempting to generate all combinations of 'oneOf' fields")
    val oneOfFields = dataGenerators
      .filter(x => x.isInstanceOf[RandomOneOfDataGenerator] || x.options.contains(ONE_OF_GENERATOR))
    val nonOneOfFields = dataGenerators.filter(x => !x.isInstanceOf[RandomOneOfDataGenerator] && !x.options.contains(ONE_OF_GENERATOR))

    val oneOfFieldsSql = oneOfFields.map(field => {
      val fieldValues = field.structField.metadata.getStringArray(ONE_OF_GENERATOR)
      sparkSession.createDataFrame(Seq(1L).map(Holder))
        .selectExpr(explode(typedlit(fieldValues)).as(field.structField.name).expr.sql)
    })
    val nonOneOfFieldsSql = nonOneOfFields.map(dg => {
      if (dg.structField.name == INDEX_INC_FIELD) s"CAST(ROW_NUMBER() OVER (ORDER BY 1) AS long) AS `${dg.structField.name}`"
      else s"${dg.generateSqlExpressionWrapper} AS `${dg.structField.name}`"
    })

    if (oneOfFields.nonEmpty) {
      LOGGER.debug("Found fields defined with 'oneOf', attempting to create all combinations of possible values")
      val pairwiseCombinations = oneOfFieldsSql.reduce((a, b) => a.crossJoin(b))
      val selectExpr = pairwiseCombinations.columns.toList ++ nonOneOfFieldsSql
      pairwiseCombinations.selectExpr(selectExpr: _*)
    } else {
      LOGGER.info("No fields defined with 'oneOf', unable to create all possible combinations")
      indexedDf
    }
  }

  private def generateDataWithSchemaSql(df: DataFrame, countGenerator: DataGenerator[_], dataGenerators: List[DataGenerator[_]]): DataFrame = {
    val namedStruct = dataGenerators.map(dg => s"'${dg.structField.name}', CAST(${dg.generateSqlExpressionWrapper} AS ${dg.structField.dataType.sql})").mkString(",")
    //if it is using a weighted oneOf generator, it will have a weight column
    val countGeneratorSql = countGenerator.generateSqlExpressionWrapper
    val optWeightCol = if (countGeneratorSql.contains(RECORD_COUNT_GENERATOR_WEIGHT_FIELD)) Array(s"RAND() AS $RECORD_COUNT_GENERATOR_WEIGHT_FIELD") else Array[String]()
    val perCountGeneratedExpr = df.columns ++ optWeightCol ++ Array(
      s"CAST($countGeneratorSql AS INT) AS $PER_FIELD_COUNT_GENERATED_NUM",
      s"CASE WHEN $PER_FIELD_COUNT_GENERATED_NUM = 0 THEN ARRAY() ELSE SEQUENCE(1, $PER_FIELD_COUNT_GENERATED_NUM) END AS $PER_FIELD_COUNT_GENERATED"
    )
    val transformPerCountExpr = df.columns ++ Array(s"TRANSFORM($PER_FIELD_COUNT_GENERATED, x -> NAMED_STRUCT($namedStruct)) AS $PER_FIELD_COUNT")

    df.selectExpr(perCountGeneratedExpr: _*)
      .selectExpr(transformPerCountExpr: _*)
      .drop(PER_FIELD_COUNT_GENERATED, PER_FIELD_COUNT_GENERATED_NUM, RECORD_COUNT_GENERATOR_WEIGHT_FIELD)
  }

  private def getStructWithGenerators(fields: List[Field], options: Map[String, String]): List[DataGenerator[_]] = {
    val faker = this.faker
    val indexIncMetadata = new MetadataBuilder().putString(SQL_GENERATOR, INDEX_INC_FIELD).build()
    val indexIncField = new RandomLongDataGenerator(StructField(INDEX_INC_FIELD, LongType, false, indexIncMetadata), faker)
    
    // Filter out struct fields that don't have any nested fields defined
    val filteredFields = filterEmptyStructFields(fields)
    
    // Apply user-defined field filters
    val userFilteredFields = applyFieldFilters(filteredFields, options)
    
    List(indexIncField) ++ userFilteredFields.map(field => getDataGenerator(field.options, field.toStructField, faker, enableFastGeneration))
  }

  /**
   * Recursively filters out struct fields that don't have any nested fields defined.
   * This prevents empty struct fields from being included in data generation.
   */
  private def filterEmptyStructFields(fields: List[Field]): List[Field] = {
    fields.flatMap { field =>
      val isStructType = field.`type`.exists(t => 
        t.toLowerCase.contains("struct") || 
        (field.fields.nonEmpty && !t.toLowerCase.startsWith("array"))
      )
      
      if (isStructType && field.fields.isEmpty) {
        // Skip struct fields with no nested fields
        LOGGER.debug(s"Filtering out empty struct field: ${field.name}")
        None
      } else if (field.fields.nonEmpty) {
        // Recursively filter nested fields for struct/array fields
        val filteredNestedFields = filterEmptyStructFields(field.fields)
        if (filteredNestedFields.nonEmpty) {
          Some(field.copy(fields = filteredNestedFields))
        } else if (isStructType) {
          // If all nested fields were filtered out, skip this struct too
          LOGGER.debug(s"Filtering out struct field with only empty nested structs: ${field.name}")
          None
        } else {
          Some(field)
        }
      } else {
        // Non-struct fields are kept as is
        Some(field)
      }
    }
  }

  /**
   * Applies user-defined field filters to include/exclude specific fields based on configuration.
   * Supports both exact field names and regex patterns.
   * For nested fields, uses dot notation (e.g., "user.name", "account.*").
   * Automatically handles Spark's "element" wrapper for array fields - users don't need to specify it.
   */
  private def applyFieldFilters(fields: List[Field], options: Map[String, String]): List[Field] = {
    val includeFields = parseFieldList(options.get(INCLUDE_FIELDS))
    val excludeFields = parseFieldList(options.get(EXCLUDE_FIELDS))
    val includePatterns = parseFieldList(options.get(INCLUDE_FIELD_PATTERNS))
    val excludePatterns = parseFieldList(options.get(EXCLUDE_FIELD_PATTERNS))

    // If no filters are specified, return all fields
    if (includeFields.isEmpty && excludeFields.isEmpty && includePatterns.isEmpty && excludePatterns.isEmpty) {
      return fields
    }

    // Collect all field paths that should be included, accounting for array "element" wrappers
    val allFieldPaths = collectAllFieldPathsWithArrayHandling(fields)
    val fieldsToInclude = allFieldPaths.filter { fieldPath =>
      val isExcludedByName = excludeFields.contains(fieldPath) || excludeFields.exists(excludeField => matchesArrayPath(fieldPath, excludeField))
      val isExcludedByPattern = excludePatterns.exists(pattern => fieldPath.matches(pattern) || matchesArrayPathPattern(fieldPath, pattern))
      
      if (isExcludedByName || isExcludedByPattern) {
        false
      } else if (includeFields.nonEmpty || includePatterns.nonEmpty) {
        val isIncludedByName = includeFields.contains(fieldPath) || includeFields.exists(includeField => matchesArrayPath(fieldPath, includeField))
        val isIncludedByPattern = includePatterns.exists(pattern => fieldPath.matches(pattern) || matchesArrayPathPattern(fieldPath, pattern))
        isIncludedByName || isIncludedByPattern
      } else {
        true
      }
    }.toSet

    def filterFieldsRecursively(fieldList: List[Field], pathPrefix: String = ""): List[Field] = {
      fieldList.flatMap { field =>
        val fieldPath = if (pathPrefix.isEmpty) field.name else s"$pathPrefix.${field.name}"
        
        if (field.fields.nonEmpty) {
          // For fields with nested fields, recursively filter
          val filteredNestedFields = filterFieldsRecursively(field.fields, fieldPath)
          
          // Include the parent field if:
          // 1. It should be included based on its own path, OR
          // 2. It has nested fields that should be included, OR
          // 3. Any of its descendant paths should be included (accounting for array paths)
          val hasIncludedDescendants = fieldsToInclude.exists(includePath => 
            includePath.startsWith(fieldPath + ".") || 
            (isArrayField(field) && includePath.startsWith(fieldPath + ".element."))
          )
          
          if (fieldsToInclude.contains(fieldPath) || filteredNestedFields.nonEmpty || hasIncludedDescendants) {
            Some(field.copy(fields = filteredNestedFields))
          } else {
            LOGGER.debug(s"Filtering out field due to field filters: $fieldPath")
            None
          }
        } else {
          // For leaf fields, check if they should be included
          if (fieldsToInclude.contains(fieldPath)) {
            Some(field)
          } else {
            LOGGER.debug(s"Filtering out field due to field filters: $fieldPath")
            None
          }
        }
      }
    }

    val filteredFields = filterFieldsRecursively(fields)
    
    if (filteredFields.size < fields.size) {
      LOGGER.info(s"Field filtering applied: ${fields.size} -> ${filteredFields.size} fields")
    }
    
    filteredFields
  }

  /**
   * Checks if a field is an array type based on its type string.
   */
  private def isArrayField(field: Field): Boolean = {
    field.`type`.exists(_.toLowerCase.startsWith("array"))
  }

  /**
   * Checks if a user-specified path matches an actual field path, accounting for array "element" wrappers.
   * For example, user path "payment_information.payment_id" should match actual path "payment_information.element.payment_id"
   */
  private def matchesArrayPath(actualPath: String, userPath: String): Boolean = {
    if (actualPath == userPath) {
      true
    } else {
      // Check if the actual path has "element" wrappers that the user path doesn't specify
      val actualParts = actualPath.split("\\.")
      val userParts = userPath.split("\\.")
      
      if (userParts.length >= actualParts.length) {
        false // User path can't be longer than actual path
      } else {
        // Try to match by skipping "element" parts in the actual path
        matchPathsSkippingElements(actualParts.toList, userParts.toList)
      }
    }
  }

  /**
   * Checks if a user-specified pattern matches an actual field path, accounting for array "element" wrappers.
   */
  private def matchesArrayPathPattern(actualPath: String, userPattern: String): Boolean = {
    // First try direct pattern matching
    if (actualPath.matches(userPattern)) {
      true
    } else {
      // Try pattern matching after removing "element" parts
      val pathWithoutElements = actualPath.replaceAll("\\.element\\.", ".")
      pathWithoutElements.matches(userPattern)
    }
  }

  /**
   * Recursively matches path parts, skipping "element" parts in the actual path.
   */
  @tailrec
  private def matchPathsSkippingElements(actualParts: List[String], userParts: List[String]): Boolean = {
    (actualParts, userParts) match {
      case (Nil, Nil) => true
      case (Nil, _) => false
      case (_, Nil) => true // User path fully matched
      case (actualHead :: actualTail, userHead :: userTail) =>
        if (actualHead == "element") {
          // Skip "element" in actual path and continue matching
          matchPathsSkippingElements(actualTail, userParts)
        } else if (actualHead == userHead) {
          // Parts match, continue with both tails
          matchPathsSkippingElements(actualTail, userTail)
        } else {
          // Parts don't match
          false
        }
    }
  }

  /**
   * Collects all field paths from a nested field structure using dot notation,
   * including both user-friendly paths (without "element") and actual Spark paths (with "element").
   */
  private def collectAllFieldPathsWithArrayHandling(fields: List[Field], pathPrefix: String = ""): List[String] = {
    fields.flatMap { field =>
      val fieldPath = if (pathPrefix.isEmpty) field.name else s"$pathPrefix.${field.name}"
      
      if (field.fields.nonEmpty) {
        if (isArrayField(field)) {
          // For array fields, collect paths both with and without "element" wrapper
          val withElementPaths = collectAllFieldPathsWithArrayHandling(field.fields, s"$fieldPath.element")
          val withoutElementPaths = collectAllFieldPathsWithArrayHandling(field.fields, fieldPath)
          
          // Include the parent path and all nested paths (both variants)
          fieldPath :: (withElementPaths ++ withoutElementPaths)
        } else {
          // For non-array fields, collect paths normally
          fieldPath :: collectAllFieldPathsWithArrayHandling(field.fields, fieldPath)
        }
      } else {
        // Leaf field
        List(fieldPath)
      }
    }
  }

  /**
   * Parses a comma-separated field list from options.
   */
  private def parseFieldList(optValue: Option[String]): List[String] = {
    optValue.map(_.split(",")).getOrElse(Array()).map(_.trim).filter(_.nonEmpty).toList
  }

  private def registerSparkFunctions(): Unit = {
    sparkSession.udf.register(GENERATE_REGEX_UDF, UDFHelperFunctions.regex(faker))
    sparkSession.udf.register(GENERATE_FAKER_EXPRESSION_UDF, UDFHelperFunctions.expression(faker))
    sparkSession.udf.register(GENERATE_RANDOM_ALPHANUMERIC_STRING_UDF, UDFHelperFunctions.alphaNumeric())
  }

  private def attachMetadata(df: DataFrame, structType: StructType): DataFrame = {
    // Helper function to recursively update metadata in nested structures
    def updateNestedMetadata(originalType: DataType, targetType: DataType): DataType = {
      (originalType, targetType) match {
        case (originalStruct: StructType, targetStruct: StructType) =>
          // For struct types, recursively update each field
          val updatedFields = originalStruct.fields.map { originalField =>
            targetStruct.find(_.name == originalField.name) match {
              case Some(targetField) =>
                // Recursively update the field's data type and attach metadata
                val updatedDataType = updateNestedMetadata(originalField.dataType, targetField.dataType)
                StructField(originalField.name, updatedDataType, originalField.nullable, targetField.metadata)
              case None =>
                // Keep original field if not found in target
                originalField
            }
          }
          StructType(updatedFields)

        case (ArrayType(originalElementType, nullable), ArrayType(targetElementType, _)) =>
          // For array types, recursively update the element type
          val updatedElementType = updateNestedMetadata(originalElementType, targetElementType)
          ArrayType(updatedElementType, nullable)

        case (_, _) =>
          // For primitive types, just return the original type (metadata will be handled at field level)
          originalType
      }
    }

    // Create a new schema with updated metadata
    val updatedSchema = StructType(
      df.schema.fields.map { originalField =>
        structType.find(_.name == originalField.name) match {
          case Some(targetField) =>
            // Update the field's data type with proper metadata propagation
            val updatedDataType = updateNestedMetadata(originalField.dataType, targetField.dataType)
            StructField(originalField.name, updatedDataType, originalField.nullable, targetField.metadata)
          case None =>
            // Keep original field if not found in target
            originalField
        }
      }
    )

    // Apply the updated schema to the DataFrame
    val sparkSession = df.sparkSession
    val rdd = df.rdd
    sparkSession.createDataFrame(rdd, updatedSchema)
  }
}

object UDFHelperFunctions extends Serializable {

  private val RANDOM = new Random()

  def regex(faker: Faker): UserDefinedFunction = udf((s: String) => faker.regexify(s)).asNondeterministic()

  def expression(faker: Faker): UserDefinedFunction = udf((s: String) => faker.expression(s)).asNondeterministic()

  def alphaNumeric(): UserDefinedFunction = udf((minLength: Int, maxLength: Int) => {
    val length = RANDOM.nextInt(maxLength + 1) + minLength
    RANDOM.alphanumeric.take(length).mkString("")
  }).asNondeterministic()
}
