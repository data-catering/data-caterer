package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_FIELD_NULLABLE, DEFAULT_PER_FIELD_COUNT_RECORDS, FOREIGN_KEY_DELIMITER, FOREIGN_KEY_DELIMITER_REGEX, FOREIGN_KEY_PLAN_FILE_DELIMITER_REGEX, IS_PRIMARY_KEY, IS_UNIQUE, MAXIMUM, MINIMUM, PRIMARY_KEY_POSITION, STATIC}
import io.github.datacatering.datacaterer.api.model.{Count, Field, ForeignKeyRelation, IntegerType, PerFieldCount, SinkOptions, Step, Task}
import io.github.datacatering.datacaterer.core.exception.{InvalidFieldConfigurationException, InvalidForeignKeyFormatException}
import io.github.datacatering.datacaterer.core.model.Constants.{COUNT_BASIC, COUNT_FIELDS, COUNT_GENERATED, COUNT_GENERATED_PER_FIELD, COUNT_NUM_RECORDS, COUNT_PER_FIELD, COUNT_TYPE}
import io.github.datacatering.datacaterer.core.model.ForeignKeyWithGenerateAndDelete
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{ArrayType, DataType, Metadata, MetadataBuilder, StructField, StructType}

import scala.language.implicitConversions


object ForeignKeyRelationHelper {
  def fromString(foreignKey: String): ForeignKeyRelation = {
    val strSpt = foreignKey.split(FOREIGN_KEY_DELIMITER_REGEX)
    val strSptPlanFile = foreignKey.split(FOREIGN_KEY_PLAN_FILE_DELIMITER_REGEX)

    (strSpt.length, strSptPlanFile.length) match {
      case (3, _) => ForeignKeyRelation(strSpt.head, strSpt(1), strSpt.last.split(",(?![^()]*\\))").toList)
      case (2, _) => ForeignKeyRelation(strSpt.head, strSpt.last)
      case (_, 3) => ForeignKeyRelation(strSptPlanFile.head, strSptPlanFile(1), strSptPlanFile.last.split(",(?![^()]*\\))").toList)
      case (_, 2) => ForeignKeyRelation(strSptPlanFile.head, strSptPlanFile.last)
      case _ => throw InvalidForeignKeyFormatException(foreignKey)
    }
  }

  def updateForeignKeyName(stepNameMapping: Map[String, String], foreignKey: ForeignKeyRelation): ForeignKeyRelation = {
    val fkDataSourceStep = foreignKey.toString.split(FOREIGN_KEY_DELIMITER_REGEX).take(2).mkString(FOREIGN_KEY_DELIMITER)
    stepNameMapping.get(fkDataSourceStep)
      .map(newName => {
        val sptNewName = newName.split(FOREIGN_KEY_DELIMITER_REGEX)
        val newDataSourceName = sptNewName.head
        val newStepName = sptNewName.last
        foreignKey.copy(dataSource = newDataSourceName, step = newStepName)
      })
      .getOrElse(foreignKey)
  }
}

object SchemaHelper {
  private val LOGGER = Logger.getLogger(getClass.getName)

  def fromStructType(structType: StructType): List[Field] = {
    structType.fields.map(FieldHelper.fromStructField).toList
  }

  /**
   * Merge the field definitions together, taking schema2 field definition as preference
   *
   * @param schema1 First schema all fields defined
   * @param schema2 Second schema which may have all or subset of fields defined where it will override if same
   *                options defined in schema1
   * @return Merged schema
   */
  def mergeSchemaInfo(schema1: List[Field], schema2: List[Field], hasMultipleSubDataSources: Boolean = false): List[Field] = {
    LOGGER.debug(s"mergeSchemaInfo: schema1.size=${schema1.size}, schema2.size=${schema2.size}, hasMultipleSubDataSources=$hasMultipleSubDataSources")
    LOGGER.debug(s"Schema1 fields: ${schema1.map(_.name).mkString(", ")}")
    LOGGER.debug(s"Schema2 fields: ${schema2.map(_.name).mkString(", ")}")

    val result = (schema1.isEmpty, schema2.isEmpty) match {
      case (false, true) =>
        LOGGER.debug("Using schema1 (schema2 is empty)")
        schema1
      case (true, false) =>
        LOGGER.debug("Using schema2 (schema1 is empty)")
        schema2
      case (true, true) =>
        LOGGER.debug("Both schemas are empty")
        List()
      case (false, false) =>
        LOGGER.debug("Both schemas non-empty, merging...")
        mergeBothSchemas(schema1, schema2, hasMultipleSubDataSources)
    }

    LOGGER.debug(s"mergeSchemaInfo final result: ${result.map(_.name).mkString(", ")} (count: ${result.size})")
    result
  }

  /**
   * Merge two non-empty schemas together
   */
  private def mergeBothSchemas(schema1: List[Field], schema2: List[Field], hasMultipleSubDataSources: Boolean): List[Field] = {
    val mergedFields = mergePrimaryFields(schema1, schema2)
    val userOnlyFields = getUserOnlyFields(schema2, schema1, hasMultipleSubDataSources)
    val metadataOnlyFields = getMetadataOnlyFields(schema1, schema2)

    combineFieldsWithoutDuplicates(mergedFields, userOnlyFields, metadataOnlyFields)
  }

  /**
   * Merge fields that exist in schema1 with their overrides from schema2
   */
  private def mergePrimaryFields(schema1: List[Field], schema2: List[Field]): List[Field] = {
    schema1.map(field => {
      val matchingFieldsInSchema2 = schema2.filter(_.name == field.name)

      if (matchingFieldsInSchema2.isEmpty) {
        LOGGER.debug(s"Field '${field.name}' from metadata not overridden by user (keeping metadata definition)")
        field
      } else {
        if (matchingFieldsInSchema2.size > 1) {
          LOGGER.warn(s"Multiple field definitions found for field '${field.name}'. Only taking the first definition. Found ${matchingFieldsInSchema2.size} definitions.")
        }
        val f2 = matchingFieldsInSchema2.head
        LOGGER.debug(s"Merging field '${field.name}': metadata type=${field.`type`.getOrElse("unknown")}, user type=${f2.`type`.getOrElse("unknown")}")
        mergeTwoFields(field, f2)
      }
    })
  }

  /**
   * Merge two individual fields together
   */
  private def mergeTwoFields(field1: Field, field2: Field): Field = {
    val mergedNestedFields = mergeNestedFields(field1, field2)
    val fieldType = mergeFieldType(field1, field2, mergedNestedFields)
    val fieldGenerator = mergeGenerator(field1, field2)
    val fieldNullable = mergeNullable(field1, field2)
    val fieldStatic = mergeStaticValue(field1, field2)

    LOGGER.debug(s"Merged field '${field1.name}': final type=${fieldType.getOrElse("unknown")}, options=${fieldGenerator.size} entries")
    Field(field1.name, fieldType, fieldGenerator, fieldNullable, fieldStatic, mergedNestedFields)
  }

  /**
   * Merge nested fields from two field definitions
   */
  private def mergeNestedFields(field1: Field, field2: Field): List[Field] = {
    (field1.fields.nonEmpty, field2.fields.nonEmpty) match {
      case (true, true) =>
        LOGGER.debug(s"Merging nested fields for '${field1.name}': metadata has ${field1.fields.size} nested, user has ${field2.fields.size} nested")
        mergeSchemaInfo(field1.fields, field2.fields)
      case (true, false) =>
        LOGGER.debug(s"Using metadata nested fields for '${field1.name}' (${field1.fields.size} fields)")
        field1.fields
      case (false, true) =>
        LOGGER.debug(s"Using user nested fields for '${field1.name}' (${field2.fields.size} fields)")
        field2.fields
      case (false, false) =>
        List()
    }
  }

  /**
   * Get fields that exist only in schema2 (user-defined fields)
   */
  private def getUserOnlyFields(schema2: List[Field], schema1: List[Field], hasMultipleSubDataSources: Boolean): List[Field] = {
    if (hasMultipleSubDataSources) {
      LOGGER.info(s"Multiple sub data sources detected, skipping user-only fields to avoid field duplication")
      List()
    } else {
      val additionalFields = schema2.filter(f2 => !schema1.exists(f1 => f1.name == f2.name))
      if (additionalFields.nonEmpty) {
        LOGGER.debug(s"Found ${additionalFields.size} user-defined fields not in metadata: [${additionalFields.map(_.name).mkString(", ")}]")
      }
      additionalFields
    }
  }

  /**
   * Get fields that exist only in schema1 (metadata-only fields not overridden)
   */
  private def getMetadataOnlyFields(schema1: List[Field], schema2: List[Field]): List[Field] = {
    val metadataOnlyFields = schema1.filter(f1 => !schema2.exists(f2 => f2.name == f1.name))
    if (metadataOnlyFields.nonEmpty) {
      LOGGER.debug(s"Found ${metadataOnlyFields.size} metadata fields not overridden by user: [${metadataOnlyFields.map(_.name).mkString(", ")}]")
    }
    metadataOnlyFields
  }

  /**
   * Combine merged, user-only, and metadata-only fields while avoiding duplicates
   */
  private def combineFieldsWithoutDuplicates(mergedFields: List[Field], userOnlyFields: List[Field], metadataOnlyFields: List[Field]): List[Field] = {
    val includedNames = mergedFields.map(_.name).toSet
    LOGGER.debug(s"Base merged fields (metadata + user overrides): [${mergedFields.map(_.name).mkString(", ")}] (count: ${mergedFields.size})")

    // Add user-only fields that aren't already included
    val fieldsToAddFromUser = userOnlyFields.filter(f => !includedNames.contains(f.name))
    if (fieldsToAddFromUser.nonEmpty) {
      LOGGER.debug(s"Added user-only fields: [${fieldsToAddFromUser.map(_.name).mkString(", ")}]")
    }

    // Add metadata-only fields that aren't already included
    val updatedIncludedNames = includedNames ++ fieldsToAddFromUser.map(_.name)
    val fieldsToAddFromMetadata = metadataOnlyFields.filter(f => !updatedIncludedNames.contains(f.name))
    if (fieldsToAddFromMetadata.nonEmpty) {
      LOGGER.debug(s"Added metadata-only fields: [${fieldsToAddFromMetadata.map(_.name).mkString(", ")}]")
    }

    mergedFields ++ fieldsToAddFromUser ++ fieldsToAddFromMetadata
  }

  private def mergeStaticValue(field: Field, f2: Field) = {
    (field.static, f2.static) match {
      case (Some(fStatic), Some(f2Static)) =>
        if (fStatic.equalsIgnoreCase(f2Static)) {
          field.static
        } else {
          LOGGER.warn(s"User has defined static value different to metadata source or from data source. " +
            s"Using user defined static value, field-name=${field.name}, user-static-value=$f2Static, data-static-value=$fStatic")
          f2.static
        }
      case (Some(_), None) => field.static
      case (None, Some(_)) => f2.static
      case _ => None
    }
  }

  private def mergeNullable(field: Field, f2: Field) = {
    (field.nullable, f2.nullable) match {
      case (false, _) => false
      case (true, false) => false
      case _ => DEFAULT_FIELD_NULLABLE
    }
  }

  private def mergeGenerator(field: Field, f2: Field) = {
    field.options ++ f2.options
  }

  private def mergeFieldType(field: Field, f2: Field, mergedNestedFields: List[Field] = List()) = {
    (field.`type`, f2.`type`) match {
      case (Some(fType), Some(f2Type)) =>
        if (fType.equalsIgnoreCase(f2Type)) {
          field.`type`
        } else {
          // Check if we're dealing with struct types that should be merged based on their nested fields
          val isStructType = fType.toLowerCase.startsWith("struct") || f2Type.toLowerCase.startsWith("struct")
          val isArrayOfStructType = (fType.toLowerCase.startsWith("array<struct") || f2Type.toLowerCase.startsWith("array<struct"))
          
          if (isStructType || isArrayOfStructType) {
            // For struct types, reconstruct the type from merged nested fields if available
            if (mergedNestedFields.nonEmpty) {
              // Reconstruct struct type from merged nested fields
              val fieldDefs = mergedNestedFields.map(f => {
                val fieldType = f.`type`.getOrElse("string")
                s"${f.name}: $fieldType"
              }).mkString(", ")
              
              val reconstructedType = if (isArrayOfStructType) {
                s"array<struct<$fieldDefs>>"
              } else {
                s"struct<$fieldDefs>"
              }
              
              Some(reconstructedType)
            } else {
              // Fallback to original logic when no merged fields available
              if (f2Type.toLowerCase.contains("struct<>") && fType.toLowerCase.startsWith("struct")) {
                // User defined empty struct with nested fields, use metadata source type as base
                field.`type`
              } else if (fType.toLowerCase.contains("struct<>") && f2Type.toLowerCase.startsWith("struct")) {
                // Metadata source has empty struct, user has complete type
                f2.`type`
              } else {
                // Both have complete struct definitions, use metadata source as base
                LOGGER.debug(s"Both field definitions have complete struct types. Using metadata source type as base for field merging, field-name=${field.name}, user-type=$f2Type, data-source-type=$fType")
                field.`type`
              }
            }
          } else {
            // For non-struct types, log warning and use metadata source type
            LOGGER.warn(s"User has defined data type different to metadata source or from data source. " +
              s"Using data source defined type, field-name=${field.name}, user-type=$f2Type, data-source-type=$fType")
            field.`type`
          }
        }
      case (Some(_), None) => field.`type`
      case (None, Some(_)) => f2.`type`
      case _ => field.`type`
    }
  }
}

object FieldHelper {

  def fromStructField(structField: StructField): Field = {
    val metadataOptions = MetadataUtil.metadataToMap(structField.metadata)
    val optStatic = if (structField.metadata.contains(STATIC)) Some(structField.metadata.getString(STATIC)) else None
    val fields = if (structField.dataType.typeName == "struct") {
      SchemaHelper.fromStructType(structField.dataType.asInstanceOf[StructType])
    } else if (structField.dataType.typeName == "array" && structField.dataType.asInstanceOf[ArrayType].elementType.typeName == "struct") {
      SchemaHelper.fromStructType(structField.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType])
    } else {
      List()
    }
    Field(structField.name, Some(structField.dataType.sql.toLowerCase), metadataOptions, structField.nullable, optStatic, fields)
  }
}

object PlanImplicits {

  implicit class ForeignKeyRelationOps(foreignKeyRelation: ForeignKeyRelation) {
    def dataFrameName = s"${foreignKeyRelation.dataSource}.${foreignKeyRelation.step}"
  }

  implicit class SinkOptionsOps(sinkOptions: SinkOptions) {
    def gatherForeignKeyRelations(source: ForeignKeyRelation): ForeignKeyWithGenerateAndDelete = {
      val generationFk = sinkOptions.foreignKeys.filter(f => f.source.equals(source)).flatMap(_.generate)
      val deleteFk = sinkOptions.foreignKeys.filter(f => f.source.equals(source)).flatMap(_.delete)
      ForeignKeyWithGenerateAndDelete(source, generationFk, deleteFk)
    }

    def foreignKeyStringWithDataSourceAndStep(fk: String): String = fk.split(FOREIGN_KEY_DELIMITER_REGEX).take(2).mkString(FOREIGN_KEY_DELIMITER)

    def foreignKeysWithoutFieldNames: List[(String, List[String])] = {
      sinkOptions.foreignKeys.map(foreignKey => {
        val sourceFk = foreignKeyStringWithDataSourceAndStep(foreignKey.source.toString)
        val generationFk = foreignKey.generate.map(g => foreignKeyStringWithDataSourceAndStep(g.toString))
        val deleteFk = foreignKey.delete.map(d => foreignKeyStringWithDataSourceAndStep(d.toString))
        (sourceFk, generationFk ++ deleteFk)
      })
    }

    def getAllForeignKeyRelations: List[(ForeignKeyRelation, String)] = {
      sinkOptions.foreignKeys.flatMap(fk => {
        val generationForeignKeys = fk.generate.map(_ -> "generation")
        val deleteForeignKeys = fk.delete.map(_ -> "delete")
        List((fk.source, "source")) ++ generationForeignKeys ++ deleteForeignKeys
      })
    }
  }

  implicit class TaskOps(task: Task) {
    def toTaskDetailString: String = {
      val enabledSteps = task.steps.filter(_.enabled)
      val stepSummary = enabledSteps.map(_.toStepDetailString).mkString(",")
      s"name=${task.name}, num-steps=${task.steps.size}, num-enabled-steps=${enabledSteps.size}, enabled-steps-summary=($stepSummary)"
    }
  }

  implicit class StepOps(step: Step) {
    def toStepDetailString: String = {
      s"name=${step.name}, type=${step.`type`}, options=${step.options}, step-num-records=(${step.count.numRecordsString._1}), schema-summary=(${step.fields.toString})"
    }

    def gatherPrimaryKeys: List[String] = {
      step.fields.filter(field => {
          if (field.options.nonEmpty) {
            val metadata = field.options
            metadata.contains(IS_PRIMARY_KEY) && metadata(IS_PRIMARY_KEY).toString.toBoolean
          } else false
        })
        .map(field => (field.name, field.options.getOrElse(PRIMARY_KEY_POSITION, "1").toString.toInt))
        .sortBy(_._2)
        .map(_._1)
    }

    def gatherUniqueFields: List[String] = {
      step.fields.filter(field => {
        field.options.get(IS_UNIQUE).exists(_.toString.toBoolean) &&
          !field.`type`.exists(t => t.contains(IntegerType.toString) || t.contains("array"))
      }).map(_.name)
    }
  }

  implicit class CountOps(count: Count) {
    private val LOGGER = Logger.getLogger(getClass.getName)

    private def hasRecordsAndPerFieldDefined: Boolean = count.records.isDefined && count.perField.isDefined

    private def hasPerFieldDefined: Boolean = count.perField.isDefined && count.perField.get.count.isDefined

    private def hasPerFieldCountDefinedNoOptions: Boolean = hasPerFieldDefined && count.perField.get.options.isEmpty

    private def hasPerFieldCountDefinedWithOptions: Boolean = hasPerFieldDefined && count.perField.get.options.nonEmpty

    def numRecordsString: (String, List[List[String]]) = {
      //per field count defined => 2 records per account_id
      if (hasRecordsAndPerFieldDefined && hasPerFieldCountDefinedNoOptions) {
        val records = (count.records.get * count.perField.get.count.get).toString
        val fields = count.perField.get.fieldNames.mkString(",")
        val str = s"per-field-count: fields=$fields, num-records=$records"
        val list = List(
          List(COUNT_TYPE, COUNT_PER_FIELD),
          List(COUNT_FIELDS, fields),
          List(COUNT_NUM_RECORDS, records)
        )
        (str, list)
      } else if (hasPerFieldCountDefinedWithOptions) {
        val records = (count.records.get * count.perField.get.count.get).toString
        val fields = count.perField.get.fieldNames.mkString(",")
        val str = s"per-field-count: fields=$fields, num-records-via-generator=$records"
        val list = List(
          List(COUNT_TYPE, COUNT_GENERATED_PER_FIELD),
          List(COUNT_FIELDS, fields),
          List(COUNT_NUM_RECORDS, records)
        )
        (str, list)
      } else if (count.records.isDefined) {
        val records = count.records.get.toString
        val str = s"basic-count: num-records=$records"
        val list = List(
          List(COUNT_TYPE, COUNT_BASIC),
          List(COUNT_NUM_RECORDS, records)
        )
        (str, list)
      } else if (count.options.nonEmpty) {
        val records = count.options.toString
        val str = s"generated-count: num-records=$records"
        val list = List(
          List(COUNT_TYPE, COUNT_GENERATED),
          List(COUNT_NUM_RECORDS, records)
        )
        (str, list)
      } else {
        //TODO: should throw error here?
        ("0", List())
      }
    }

    def numRecords: Long = {
      val countOptionsEmpty = count.options.isEmpty
      val countPerFieldOptionsEmpty = count.perField.map(_.options).getOrElse(Map.empty).isEmpty

      // If duration and pattern are configured, calculate records based on pattern type
      if (count.duration.isDefined && count.pattern.isDefined) {
        val durationSeconds = parseDurationToSeconds(count.duration.get)
        val pattern = count.pattern.get
        val calculatedRecords = calculateRecordsForPattern(pattern, durationSeconds)
        // If calculateRecordsForPattern returns a negative or zero value, it means the pattern is invalid
        // In that case, fall back to records field if available
        if (calculatedRecords > 0) {
          LOGGER.debug(s"Calculating records from pattern-based config: duration=${count.duration.get}, pattern=${pattern.`type`}, calculated-records=$calculatedRecords")
          return calculatedRecords
        }
      }

      // If duration is configured with rate (no pattern), calculate records from duration * rate
      if (count.duration.isDefined && count.rate.isDefined) {
        val durationSeconds = parseDurationToSeconds(count.duration.get)
        val rate = count.rate.get
        val calculatedRecords = (durationSeconds * rate).toLong
        LOGGER.debug(s"Calculating records from duration-based config: duration=${count.duration.get}, rate=$rate/sec, calculated-records=$calculatedRecords")
        return calculatedRecords
      }

      (count.records, countOptionsEmpty, count.perField, countPerFieldOptionsEmpty) match {
        case (_, false, Some(perCol), false) =>
          perCol.averageCountPerField * averageCount(count.options)
        case (Some(t), true, Some(perCol), false) =>
          perCol.averageCountPerField * t
        case (Some(t), true, Some(perCol), true) =>
          perCol.count.get * t
        case (Some(t), false, None, true) =>
          averageCount(count.options) * t
        case (None, false, None, true) =>
          averageCount(count.options)
        case (Some(t), true, None, true) =>
          t
        case _ => 1000L
      }
    }

    /**
     * Calculate expected number of records for a given load pattern and duration.
     * Returns -1 if the pattern is invalid/unknown and cannot be calculated.
     */
    private def calculateRecordsForPattern(pattern: io.github.datacatering.datacaterer.api.model.LoadPattern, durationSeconds: Long): Long = {
      pattern.`type`.toLowerCase match {
        case "ramp" =>
          // Linear ramp: average rate = (startRate + endRate) / 2
          val startRate = pattern.startRate.getOrElse(1)
          val endRate = pattern.endRate.getOrElse(startRate)
          val averageRate = (startRate + endRate) / 2.0
          (durationSeconds * averageRate).toLong

        case "wave" =>
          // Sinusoidal wave: over complete cycles, average = baseRate
          val baseRate = pattern.baseRate.getOrElse(1)
          (durationSeconds * baseRate).toLong

        case "stepped" =>
          // Sum records for each step
          pattern.steps.map { steps =>
            steps.map { step =>
              val stepDurationSeconds = parseDurationToSeconds(step.duration)
              stepDurationSeconds * step.rate
            }.sum
          }.getOrElse(durationSeconds) // Fallback to duration if no steps

        case "spike" =>
          // Base rate for most of duration, spike rate for spike duration
          val baseRate = pattern.baseRate.getOrElse(1)
          val spikeRate = pattern.spikeRate.getOrElse(baseRate)
          val spikeDurationFraction = pattern.spikeDuration.getOrElse(0.0)
          val spikeDurationSeconds = (durationSeconds * spikeDurationFraction).toLong
          val baseDurationSeconds = durationSeconds - spikeDurationSeconds
          (baseDurationSeconds * baseRate) + (spikeDurationSeconds * spikeRate)

        case "constant" =>
          // Constant rate: use baseRate if available
          val rate = pattern.baseRate.orElse(pattern.startRate).getOrElse(1)
          durationSeconds * rate

        case _ =>
          // Unknown pattern type: return -1 to signal fallback to records field
          -1
      }
    }

    /**
     * Parse duration string (e.g., "1s", "10s", "1m") to seconds
     */
    private def parseDurationToSeconds(duration: String): Long = {
      val durationPattern = """(\d+)([smh])""".r
      duration match {
        case durationPattern(value, unit) =>
          val longValue = value.toLong
          unit match {
            case "s" => longValue
            case "m" => longValue * 60
            case "h" => longValue * 3600
            case _ => throw new IllegalArgumentException(s"Unsupported duration unit: $unit")
          }
        case _ => throw new IllegalArgumentException(s"Invalid duration format: $duration. Expected format: <number><unit> (e.g., '10s', '1m')")
      }
    }
  }

  implicit class PerFieldCountOps(perFieldCount: PerFieldCount) {
    def averageCountPerField: Long = {
      if (perFieldCount.options.nonEmpty) {
        averageCount(perFieldCount.options)
      } else {
        perFieldCount.count.getOrElse(DEFAULT_PER_FIELD_COUNT_RECORDS)
      }
    }

    def maxCountPerField: Long = {
      perFieldCount.count.map(x => x)
        .getOrElse(
          perFieldCount.options.get(MAXIMUM).map(_.toString.toLong).getOrElse(0)
        )
    }
  }

  implicit class FieldOps(field: Field) {
    def toStructField: StructField = {
      if (field.static.isDefined) {
        val metadata = new MetadataBuilder().withMetadata(getMetadata).putString(STATIC, field.static.get).build()
        StructField(field.name, DataType.fromDDL(field.`type`.get), field.nullable, metadata)
      } else if (field.fields.nonEmpty) {
        val innerStructFields = StructType(field.fields.map(_.toStructField))
        StructField(
          field.name,
          if (field.`type`.isDefined && field.`type`.get.toLowerCase.startsWith("array")) ArrayType(innerStructFields, field.nullable) else innerStructFields,
          field.nullable,
          getMetadata
        )
      } else if (field.`type`.isDefined) {
        StructField(field.name, DataType.fromDDL(field.`type`.get), field.nullable, getMetadata)
      } else {
        throw InvalidFieldConfigurationException(this.field)
      }
    }

    def fieldToStringOptions: Field = {
      val stringFieldOptions = toStringValues(field.options)
      val mappedInnerFields = field.fields.map(_.fieldToStringOptions)
      field.copy(options = stringFieldOptions, fields = mappedInnerFields)
    }

    private def toStringValues(options: Map[String, Any]): Map[String, Any] = {
      options.map(x => {
        val value = x._2 match {
          case _: Int | _: Long | _: Double | _: Float | _: Boolean => x._2.toString
          case y: List[_] =>
            if (y.nonEmpty) {
              y.head match {
                case _: Int | _: Long | _: Double | _: Float | _: Boolean => y.map(y1 => y1.toString)
                case _ => y
              }
            } else {
              y
            }
          case y => y
        }
        (x._1, value)
      })
    }

    private def getMetadata: Metadata = {
      if (field.options.nonEmpty) {
        val cleanField = field.fieldToStringOptions
        Metadata.fromJson(ObjectMapperUtil.jsonObjectMapper.writeValueAsString(cleanField.options))
      } else {
        Metadata.empty
      }
    }
  }

  private def averageCount(generator: Map[String, Any]): Long = {
    if (generator.contains(MINIMUM) || generator.contains(MAXIMUM)) {
      val min = generator.get(MINIMUM).map(_.toString.toLong).getOrElse(1L)
      val max = generator.get(MAXIMUM).map(_.toString.toLong).getOrElse(10L)
      (max + min + 1) / 2
    } else 1L
  }
}

object DataFrameOmitUtil {
  import com.fasterxml.jackson.databind.JsonNode
  import com.fasterxml.jackson.databind.node.ObjectNode
  import io.github.datacatering.datacaterer.api.model.Constants.OMIT
  import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
  import org.apache.spark.sql.{Column, DataFrame}

  /**
   * Removes fields marked with omit=true from a DataFrame, including nested fields within structs and arrays.
   * This handles the complete removal of helper/intermediate fields that should not appear in final output.
   * Uses native Spark column operations for efficient processing.
   *
   * @param df The DataFrame to process
   * @return A new DataFrame with omitted fields removed
   */
  def removeOmitFields(df: DataFrame)(implicit sparkSession: org.apache.spark.sql.SparkSession): DataFrame = {
    val omittedPaths = collectOmittedPaths(df.schema)

    if (omittedPaths.isEmpty) {
      df
    } else {
      // Build column projections that exclude omitted fields
      val projectedColumns = buildColumnProjections(df.schema, omittedPaths)
      
      if (projectedColumns.isEmpty) {
        // All fields are omitted - return empty DataFrame with no columns
        df.limit(0).drop(df.columns: _*)
      } else {
        df.select(projectedColumns: _*)
      }
    }
  }
  
  /**
   * Build column projections that exclude omitted fields.
   * This recursively rebuilds structs and arrays without omitted fields using native Spark operations.
   *
   * @param schema The schema to process
   * @param omittedPaths Set of paths to omitted fields
   * @param pathPrefix Current path prefix for recursion
   * @return List of Column objects that project the schema without omitted fields
   */
  private def buildColumnProjections(
    schema: StructType, 
    omittedPaths: Set[String], 
    pathPrefix: String = ""
  ): Seq[Column] = {
    import org.apache.spark.sql.functions._
    
    schema.fields.flatMap { field =>
      val fieldPath = if (pathPrefix.isEmpty) field.name else s"$pathPrefix.${field.name}"
      
      // Skip if this field is omitted
      if (omittedPaths.contains(fieldPath)) {
        None
      } else {
        field.dataType match {
          case structType: StructType =>
            // Recursively build the struct without omitted nested fields
            val nestedColumns = buildNestedStructColumns(structType, omittedPaths, fieldPath)
            if (nestedColumns.isEmpty) {
              None // All nested fields are omitted
            } else {
              val structCols = nestedColumns.map { case (name, colExpr) => colExpr.alias(name) }
              Some(struct(structCols: _*).alias(field.name))
            }
            
          case ArrayType(elementType: StructType, containsNull) =>
            // For arrays of structs, use SQL TRANSFORM to rebuild array elements without omitted fields
            // Note: omitted paths for array elements don't include ".element" - they're at fieldPath.fieldName
            val nestedColumns = buildNestedStructColumns(elementType, omittedPaths, fieldPath)
            if (nestedColumns.isEmpty) {
              None // All nested fields are omitted
            } else {
              // Build SQL expression for the transform
              val transformSql = buildArrayTransformSql(field.name, nestedColumns)
              Some(expr(transformSql).alias(field.name))
            }
            
          case _ =>
            // Simple field - just project it
            Some(col(if (pathPrefix.isEmpty) field.name else fieldPath).alias(field.name))
        }
      }
    }
  }
  
  /**
   * Build columns for a nested struct, excluding omitted fields.
   * Returns field name -> Column mappings for the struct fields.
   */
  private def buildNestedStructColumns(
    structType: StructType,
    omittedPaths: Set[String],
    basePath: String
  ): Seq[(String, Column)] = {
    import org.apache.spark.sql.functions._
    
    structType.fields.flatMap { field =>
      val fieldPath = s"$basePath.${field.name}"
      
      if (omittedPaths.contains(fieldPath)) {
        None
      } else {
        field.dataType match {
          case nestedStruct: StructType =>
            // Recursively process deeper nested structs
            val deeperColumns = buildNestedStructColumns(nestedStruct, omittedPaths, fieldPath)
            if (deeperColumns.isEmpty) {
              None
            } else {
              // Build the struct from the nested columns
              val nestedStructCols = deeperColumns.map { case (name, colExpr) => colExpr.alias(name) }
              Some((field.name, struct(nestedStructCols: _*)))
            }
            
          case ArrayType(elementType: StructType, _) =>
            // Arrays within structs - need to use SQL transform
            // Note: omitted paths for array elements don't include ".element"
            val arrayElementFields = buildNestedStructColumns(elementType, omittedPaths, fieldPath)
            if (arrayElementFields.isEmpty) {
              None
            } else {
              // Build SQL expression for transform
              val transformSql = buildArrayTransformSql(fieldPath, arrayElementFields)
              Some((field.name, expr(transformSql)))
            }
            
          case _ =>
            Some((field.name, col(fieldPath)))
        }
      }
    }
  }
  
  /**
   * Build an SQL TRANSFORM expression for arrays of structs.
   * This approach uses SQL strings which is more reliable for nested array transformations.
   */
  private def buildArrayTransformSql(arrayPath: String, elementFields: Seq[(String, Column)]): String = {
    // Build the struct field expressions for the lambda
    val structFields = elementFields.map { case (name, _) =>
      s"x.$name AS $name"
    }.mkString(", ")
    
    s"TRANSFORM($arrayPath, x -> struct($structFields))"
  }

  /**
   * Collects all field paths that are marked with omit=true, including nested fields.
   *
   * @param schema The StructType to scan
   * @param pathPrefix The current path prefix (used for recursion)
   * @return Set of full paths to omitted fields
   */
  def collectOmittedPaths(schema: StructType, pathPrefix: String = ""): Set[String] = {
    schema.fields.flatMap { field =>
      val fieldPath = if (pathPrefix.isEmpty) field.name else s"$pathPrefix.${field.name}"
      val isOmitted = field.metadata.contains(OMIT) && field.metadata.getString(OMIT).equalsIgnoreCase("true")

      val currentPath = if (isOmitted) Set(fieldPath) else Set.empty[String]

      val nestedPaths = field.dataType match {
        case st: StructType => collectOmittedPaths(st, fieldPath)
        case ArrayType(st: StructType, _) => collectOmittedPaths(st, fieldPath)
        case _ => Set.empty[String]
      }

      currentPath ++ nestedPaths
    }.toSet
  }

  /**
   * Removes omitted fields from a JSON node (mutates the node).
   * Used when DataFrame conversion via JSON is necessary for nested field removal.
   *
   * @param node The JSON node to process
   * @param omittedPaths Set of paths to omitted fields
   * @param currentPath The current path in the JSON tree
   */
  def removeOmittedFromJson(node: JsonNode, omittedPaths: Set[String], currentPath: String = ""): Unit = {
    if (node.isObject) {
      val objectNode = node.asInstanceOf[ObjectNode]
      val it = objectNode.fields()
      val fieldsToRemove = scala.collection.mutable.ListBuffer[String]()

      while (it.hasNext) {
        val entry = it.next()
        val fieldName = entry.getKey
        val fieldPath = if (currentPath.isEmpty) fieldName else s"$currentPath.$fieldName"

        if (omittedPaths.contains(fieldPath)) {
          fieldsToRemove += fieldName
        } else {
          removeOmittedFromJson(entry.getValue, omittedPaths, fieldPath)
        }
      }

      fieldsToRemove.foreach(objectNode.remove)
    } else if (node.isArray) {
      val arrayIt = node.elements()
      while (arrayIt.hasNext) {
        removeOmittedFromJson(arrayIt.next(), omittedPaths, currentPath)
      }
    }
  }

  /**
   * Removes omitted fields from a Spark schema.
   * Returns a new schema with omitted fields filtered out.
   *
   * @param schema The original schema
   * @return A new schema without omitted fields
   */
  def removeOmittedFieldsFromSchema(schema: StructType): StructType = {
    def cleanDataType(dataType: DataType): DataType = {
      dataType match {
        case st: StructType =>
          val cleanedFields = st.fields.filter { field =>
            !(field.metadata.contains(OMIT) && field.metadata.getString(OMIT).equalsIgnoreCase("true"))
          }.map(field => field.copy(dataType = cleanDataType(field.dataType)))
          StructType(cleanedFields)

        case ArrayType(elementType, nullable) =>
          ArrayType(cleanDataType(elementType), nullable)

        case other => other
      }
    }

    val cleanedFields = schema.fields.filter { field =>
      !(field.metadata.contains(OMIT) && field.metadata.getString(OMIT).equalsIgnoreCase("true"))
    }.map(field => field.copy(dataType = cleanDataType(field.dataType)))

    StructType(cleanedFields)
  }
}
