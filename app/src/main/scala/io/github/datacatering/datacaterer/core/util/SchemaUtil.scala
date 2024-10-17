package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_FIELD_NULLABLE, FOREIGN_KEY_DELIMITER, FOREIGN_KEY_DELIMITER_REGEX, FOREIGN_KEY_PLAN_FILE_DELIMITER, FOREIGN_KEY_PLAN_FILE_DELIMITER_REGEX, IS_PRIMARY_KEY, IS_UNIQUE, MAXIMUM, MINIMUM, ONE_OF_GENERATOR, PRIMARY_KEY_POSITION, RANDOM_GENERATOR, REGEX_GENERATOR, STATIC}
import io.github.datacatering.datacaterer.api.model.{Count, Field, ForeignKeyRelation, Generator, PerColumnCount, Schema, SinkOptions, Step, Task}
import io.github.datacatering.datacaterer.core.exception.{InvalidFieldConfigurationException, InvalidForeignKeyFormatException, NoSchemaDefinedException}
import io.github.datacatering.datacaterer.core.model.Constants.{COUNT_BASIC, COUNT_COLUMNS, COUNT_GENERATED, COUNT_GENERATED_PER_COLUMN, COUNT_NUM_RECORDS, COUNT_PER_COLUMN, COUNT_TYPE}
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

  def updateForeignKeyName(stepNameMapping: Map[String, String], foreignKey: String): String = {
    val fkDataSourceStep = foreignKey.split(FOREIGN_KEY_DELIMITER_REGEX).take(2).mkString(FOREIGN_KEY_DELIMITER)
    stepNameMapping.get(fkDataSourceStep)
      .map(newName => foreignKey.replace(fkDataSourceStep, newName))
      .getOrElse(foreignKey)
  }
}

object SchemaHelper {
  private val LOGGER = Logger.getLogger(getClass.getName)

  def fromStructType(structType: StructType): Schema = {
    val fields = structType.fields.map(FieldHelper.fromStructField).toList
    Schema(Some(fields))
  }

  /**
   * Merge the field definitions together, taking schema2 field definition as preference
   *
   * @param schema1 First schema all fields defined
   * @param schema2 Second schema which may have all or subset of fields defined where it will override if same
   *                options defined in schema1
   * @return Merged schema
   */
  def mergeSchemaInfo(schema1: Schema, schema2: Schema, hasMultipleSubDataSources: Boolean = false): Schema = {
    (schema1.fields, schema2.fields) match {
      case (Some(fields1), Some(fields2)) if fields1.nonEmpty && fields2.isEmpty => schema1
      case (Some(fields1), Some(fields2)) if fields1.isEmpty && fields2.nonEmpty => schema2
      case (Some(fields1), Some(fields2)) =>
        val mergedFields = fields1.map(field => {
          val filterInSchema2 = fields2.filter(f2 => f2.name == field.name)
          val optFieldToMerge = if (filterInSchema2.nonEmpty) {
            if (filterInSchema2.size > 1) {
              LOGGER.warn(s"Multiple field definitions found. Only taking the first definition, field-name=${field.name}")
            }
            Some(filterInSchema2.head)
          } else {
            None
          }
          optFieldToMerge.map(f2 => {
            val fieldSchema = (field.schema, f2.schema) match {
              case (Some(fSchema), Some(f2Schema)) => Some(mergeSchemaInfo(fSchema, f2Schema))
              case (Some(fSchema), None) => Some(fSchema)
              case (None, Some(_)) =>
                LOGGER.warn(s"Schema from metadata source or from data source has no nested schema for field but has nested schema defined by user. " +
                  s"Ignoring user defined nested schema, field-name=${field.name}")
                None
              case _ => None
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
          fields2.filter(f2 => !fields1.exists(f1 => f1.name == f2.name))
        }
        Schema(Some(mergedFields ++ fieldsInSchema2NotInSchema1))
      case (Some(_), None) => schema1
      case (None, Some(_)) => schema2
      case _ => throw NoSchemaDefinedException()
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
    (field.generator, f2.generator) match {
      case (Some(fGen), Some(f2Gen)) =>
        val genType = if (fGen.`type`.equalsIgnoreCase(f2Gen.`type`)) fGen.`type` else f2Gen.`type`
        val options = fGen.options ++ f2Gen.options
        Some(Generator(genType, options))
      case (Some(_), None) => field.generator
      case (None, Some(_)) => f2.generator
      case _ => None
    }
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
    val generatorType = if (structField.metadata.contains(ONE_OF_GENERATOR)) {
      ONE_OF_GENERATOR
    } else if (structField.metadata.contains(REGEX_GENERATOR)) {
      REGEX_GENERATOR
    } else {
      RANDOM_GENERATOR
    }
    val generator = Generator(generatorType, metadataOptions)
    val optStatic = if (structField.metadata.contains(STATIC)) Some(structField.metadata.getString(STATIC)) else None
    val optSchema = if (structField.dataType.typeName == "struct") {
      Some(SchemaHelper.fromStructType(structField.dataType.asInstanceOf[StructType]))
    } else if (structField.dataType.typeName == "array" && structField.dataType.asInstanceOf[ArrayType].elementType.typeName == "struct") {
      Some(SchemaHelper.fromStructType(structField.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]))
    } else {
      None
    }
    Field(structField.name, Some(structField.dataType.sql.toLowerCase), Some(generator), structField.nullable, optStatic, optSchema)
  }
}

object PlanImplicits {

  implicit class ForeignKeyRelationOps(foreignKeyRelation: ForeignKeyRelation) {
    def dataFrameName = s"${foreignKeyRelation.dataSource}.${foreignKeyRelation.step}"
  }

  implicit class SinkOptionsOps(sinkOptions: SinkOptions) {
    def gatherForeignKeyRelations(key: String): ForeignKeyWithGenerateAndDelete = {
      val source = ForeignKeyRelationHelper.fromString(key)
      val generationFk = sinkOptions.foreignKeys.filter(f => f._1.equalsIgnoreCase(key)).flatMap(_._2)
      val deleteFk = sinkOptions.foreignKeys.filter(f => f._1.equalsIgnoreCase(key)).flatMap(_._3)
      val generationForeignKeys = generationFk.map(ForeignKeyRelationHelper.fromString)
      val deleteForeignKeys = deleteFk.map(ForeignKeyRelationHelper.fromString)
      ForeignKeyWithGenerateAndDelete(source, generationForeignKeys, deleteForeignKeys)
    }

    def foreignKeyStringWithDataSourceAndStep(fk: String): String = fk.split(FOREIGN_KEY_DELIMITER_REGEX).take(2).mkString(FOREIGN_KEY_DELIMITER)

    def foreignKeysWithoutColumnNames: List[(String, List[String])] = {
      sinkOptions.foreignKeys.map(foreignKey => {
        val sourceFk = foreignKeyStringWithDataSourceAndStep(foreignKey._1)
        val generationFk = foreignKey._2.map(foreignKeyStringWithDataSourceAndStep)
        val deleteFk = foreignKey._3.map(foreignKeyStringWithDataSourceAndStep)
        (sourceFk, generationFk ++ deleteFk)
      })
    }

    def getAllForeignKeyRelations: List[(ForeignKeyRelation, String)] = {
      sinkOptions.foreignKeys.flatMap(fk => {
        val sourceFk = ForeignKeyRelationHelper.fromString(fk._1)
        val generationForeignKeys = fk._2.map(f => (ForeignKeyRelationHelper.fromString(f), "generation"))
        val deleteForeignKeys = fk._3.map(f => (ForeignKeyRelationHelper.fromString(f), "delete"))
        List((sourceFk, "source")) ++ generationForeignKeys ++ deleteForeignKeys
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
      s"name=${step.name}, type=${step.`type`}, options=${step.options}, step-num-records=(${step.count.numRecordsString._1}), schema-summary=(${step.schema.toString})"
    }

    def gatherPrimaryKeys: List[String] = {
      if (step.schema.fields.isDefined) {
        val fields = step.schema.fields.get
        fields.filter(field => {
          if (field.generator.isDefined) {
            val metadata = field.generator.get.options
            metadata.contains(IS_PRIMARY_KEY) && metadata(IS_PRIMARY_KEY).toString.toBoolean
          } else false
        })
          .map(field => (field.name, field.generator.get.options.getOrElse(PRIMARY_KEY_POSITION, "1").toString.toInt))
          .sortBy(_._2)
          .map(_._1)
      } else List()
    }

    def gatherUniqueFields: List[String] = {
      step.schema.fields.map(fields => {
        fields.filter(field => {
          field.generator
            .flatMap(gen => gen.options.get(IS_UNIQUE).map(_.toString.toBoolean))
            .getOrElse(false)
        }).map(_.name)
      }).getOrElse(List())
    }
  }

  implicit class CountOps(count: Count) {
    def numRecordsString: (String, List[List[String]]) = {
      if (count.records.isDefined && count.perColumn.isDefined && count.perColumn.get.count.isDefined && count.perColumn.get.generator.isEmpty) {
        val records = (count.records.get * count.perColumn.get.count.get).toString
        val columns = count.perColumn.get.columnNames.mkString(",")
        val str = s"per-column-count: columns=$columns, num-records=$records"
        val list = List(
          List(COUNT_TYPE, COUNT_PER_COLUMN),
          List(COUNT_COLUMNS, columns),
          List(COUNT_NUM_RECORDS, records)
        )
        (str, list)
      } else if (count.perColumn.isDefined && count.perColumn.get.generator.isDefined) {
        val records = (count.records.get * count.perColumn.get.count.get).toString
        val columns = count.perColumn.get.columnNames.mkString(",")
        val str = s"per-column-count: columns=$columns, num-records-via-generator=$records"
        val list = List(
          List(COUNT_TYPE, COUNT_GENERATED_PER_COLUMN),
          List(COUNT_COLUMNS, columns),
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
      } else if (count.generator.isDefined) {
        val records = count.generator.toString
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
      (count.records, count.generator, count.perColumn, count.perColumn.flatMap(_.generator)) match {
        case (Some(t), None, Some(perCol), Some(_)) =>
          perCol.averageCountPerColumn * t
        case (Some(t), None, Some(perCol), None) =>
          perCol.count.get * t
        case (Some(t), Some(gen), None, None) =>
          gen.averageCount * t
        case (None, Some(gen), None, None) =>
          gen.averageCount
        case (Some(t), None, None, None) =>
          t
        case _ => 1000L
      }
    }
  }

  implicit class PerColumnCountOps(perColumnCount: PerColumnCount) {
    def averageCountPerColumn: Long = {
      perColumnCount.generator.map(_.averageCount).getOrElse(perColumnCount.count.map(identity).getOrElse(1L))
    }

    def maxCountPerColumn: Long = {
      perColumnCount.count.map(x => x)
        .getOrElse(
          perColumnCount.generator.flatMap(g =>
            g.options.get(MAXIMUM).map(_.toString.toLong)
          ).getOrElse(0)
        )
    }
  }

  implicit class SchemaOps(schema: Schema) {
    def toStructType: StructType = {
      if (schema.fields.isDefined) {
        val structFields = schema.fields.get.map(_.toStructField)
        StructType(structFields)
      } else {
        StructType(Seq())
      }
    }
  }

  implicit class FieldOps(field: Field) {
    def toStructField: StructField = {
      if (field.static.isDefined) {
        val metadata = new MetadataBuilder().withMetadata(getMetadata).putString(STATIC, field.static.get).build()
        StructField(field.name, DataType.fromDDL(field.`type`.get), field.nullable, metadata)
      } else if (field.schema.isDefined) {
        val innerStructFields = field.schema.get.toStructType
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

    private def getMetadata: Metadata = {
      if (field.generator.isDefined) {
        Metadata.fromJson(ObjectMapperUtil.jsonObjectMapper.writeValueAsString(field.generator.get.options))
      } else {
        Metadata.empty
      }
    }
  }

  implicit class GeneratorOps(generator: Generator) {
    def averageCount: Long = {
      if (generator.`type`.equalsIgnoreCase(RANDOM_GENERATOR)) {
        val min = generator.options.get(MINIMUM).map(_.toString.toLong).getOrElse(1L)
        val max = generator.options.get(MAXIMUM).map(_.toString.toLong).getOrElse(10L)
        (max + min + 1) / 2
      } else 1L
    }
  }
}
