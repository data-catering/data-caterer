package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_FIELD_NULLABLE, FOREIGN_KEY_DELIMITER, FOREIGN_KEY_DELIMITER_REGEX, FOREIGN_KEY_PLAN_FILE_DELIMITER_REGEX, IS_PRIMARY_KEY, IS_UNIQUE, MAXIMUM, MINIMUM, PRIMARY_KEY_POSITION, STATIC}
import io.github.datacatering.datacaterer.api.model.{Count, Field, ForeignKeyRelation, PerFieldCount, SinkOptions, Step, Task}
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
    if (schema1.nonEmpty && schema2.isEmpty) {
      schema1
    } else if (schema1.isEmpty && schema2.nonEmpty) {
      schema2
    } else if (schema1.nonEmpty && schema2.nonEmpty) {
      val mergedFields = schema1.map(field => {
        val filterInSchema2 = schema2.filter(f2 => f2.name == field.name)
        val optFieldToMerge = if (filterInSchema2.nonEmpty) {
          if (filterInSchema2.size > 1) {
            LOGGER.warn(s"Multiple field definitions found. Only taking the first definition, field-name=${field.name}")
          }
          Some(filterInSchema2.head)
        } else {
          None
        }
        optFieldToMerge.map(f2 => {
          val fieldSchema = if (field.fields.nonEmpty && f2.fields.nonEmpty) {
            mergeSchemaInfo(field.fields, f2.fields)
          } else if (field.fields.nonEmpty && f2.fields.isEmpty) {
            field.fields
          } else if (field.fields.isEmpty && f2.fields.nonEmpty) {
            f2.fields
          } else {
            List()
          }

          val fieldType = mergeFieldType(field, f2)
          val fieldGenerator = mergeGenerator(field, f2)
          val fieldNullable = mergeNullable(field, f2)
          val fieldStatic = mergeStaticValue(field, f2)
          Field(field.name, fieldType, fieldGenerator, fieldNullable, fieldStatic, fieldSchema)
        }).getOrElse(field)
      })

      val fieldsInSchema2NotInSchema1 = if (hasMultipleSubDataSources) {
        LOGGER.debug(s"Multiple sub data sources created, not adding fields that are manually defined")
        List()
      } else {
        schema2.filter(f2 => !schema1.exists(f1 => f1.name == f2.name))
      }
      mergedFields ++ fieldsInSchema2NotInSchema1
    } else {
      List()
    }
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

  private def mergeFieldType(field: Field, f2: Field) = {
    (field.`type`, f2.`type`) match {
      case (Some(fType), Some(f2Type)) =>
        if (fType.equalsIgnoreCase(f2Type)) {
          field.`type`
        } else {
          LOGGER.warn(s"User has defined data type different to metadata source or from data source. " +
            s"Using data source defined type, field-name=${field.name}, user-type=$f2Type, data-source-type=$fType")
          field.`type`
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
        field.options.get(IS_UNIQUE).exists(_.toString.toBoolean)
      }).map(_.name)
    }
  }

  implicit class CountOps(count: Count) {
    def numRecordsString: (String, List[List[String]]) = {
      if (count.records.isDefined && count.perField.isDefined && count.perField.get.count.isDefined && count.perField.get.options.isEmpty) {
        val records = (count.records.get * count.perField.get.count.get).toString
        val fields = count.perField.get.fieldNames.mkString(",")
        val str = s"per-field-count: fields=$fields, num-records=$records"
        val list = List(
          List(COUNT_TYPE, COUNT_PER_FIELD),
          List(COUNT_FIELDS, fields),
          List(COUNT_NUM_RECORDS, records)
        )
        (str, list)
      } else if (count.perField.isDefined && count.perField.get.options.nonEmpty) {
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
      (count.records, count.options.isEmpty, count.perField, count.perField.map(_.options).getOrElse(Map()).isEmpty) match {
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
  }

  implicit class PerFieldCountOps(perFieldCount: PerFieldCount) {
    def averageCountPerField: Long = {
      perFieldCount.count.getOrElse(averageCount(perFieldCount.options))
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
