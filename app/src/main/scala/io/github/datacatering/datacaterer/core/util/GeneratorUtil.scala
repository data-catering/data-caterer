package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants.{EXPRESSION, HTTP, JMS, ONE_OF_GENERATOR, REGEX_GENERATOR, SQL_GENERATOR}
import io.github.datacatering.datacaterer.api.model.{Step, TaskSummary}
import io.github.datacatering.datacaterer.core.generator.provider.{DataGenerator, OneOfDataGenerator, RandomDataGenerator, RegexDataGenerator}
import io.github.datacatering.datacaterer.core.generator.provider.FastDataGenerator.{FastRegexDataGenerator, FastStringDataGenerator}
import io.github.datacatering.datacaterer.core.model.Constants.{BATCH, REAL_TIME}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{LongType, StructField, StructType, DataType, ArrayType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.MetadataBuilder

object GeneratorUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def getDataGenerator(structField: StructField, faker: Faker): DataGenerator[_] = {
    getDataGenerator(structField, faker, enableFastGeneration = false)
  }

  def getDataGenerator(generatorOpts: Map[String, Any], structField: StructField, faker: Faker): DataGenerator[_] = {
    getDataGenerator(generatorOpts, structField, faker, enableFastGeneration = false)
  }

  /**
   * Get data generator with fast mode support.
   * When fast mode is enabled, returns generators that use pure SQL instead of UDF calls for better performance.
   */
  def getDataGenerator(structField: StructField, faker: Faker, enableFastGeneration: Boolean): DataGenerator[_] = {
    val hasSql = structField.metadata.contains(SQL_GENERATOR)
    val hasExpression = structField.metadata.contains(EXPRESSION)
    val hasRegex = structField.metadata.contains(REGEX_GENERATOR)
    val hasOneOf = structField.metadata.contains(ONE_OF_GENERATOR)

    if (hasOneOf) {
      OneOfDataGenerator.getGenerator(structField, faker)
    } else if (hasRegex) {
      if (enableFastGeneration) {
        new FastRegexDataGenerator(structField, faker)
      } else {
        RegexDataGenerator.getGenerator(structField, faker)
      }
    } else if (hasSql || hasExpression) {
      if (enableFastGeneration && hasExpression) {
        new FastStringDataGenerator(structField, faker)
      } else {
        RandomDataGenerator.getGeneratorForStructField(structField, faker)
      }
    } else {
      // For string fields in fast mode, use fast generator to avoid potential UDF calls
      if (enableFastGeneration && structField.dataType.typeName == "string") {
        new FastStringDataGenerator(structField, faker)
      } else {
        RandomDataGenerator.getGeneratorForStructField(structField, faker)
      }
    }
  }

  /**
   * Get data generator with fast mode support and generator options.
   */
  def getDataGenerator(generatorOpts: Map[String, Any], structField: StructField, faker: Faker, enableFastGeneration: Boolean): DataGenerator[_] = {
    if (generatorOpts.contains(ONE_OF_GENERATOR)) {
      OneOfDataGenerator.getGenerator(structField, faker)
    } else if (generatorOpts.contains(REGEX_GENERATOR)) {
      if (enableFastGeneration) {
        new FastRegexDataGenerator(structField, faker)
      } else {
        RegexDataGenerator.getGenerator(structField, faker)
      }
    } else {
      // For string fields in fast mode, use fast generator
      if (enableFastGeneration && structField.dataType.typeName == "string") {
        new FastStringDataGenerator(structField, faker)
      } else {
        RandomDataGenerator.getGeneratorForStructField(structField, faker)
      }
    }
  }

  def zipWithIndex(df: DataFrame, colName: String): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(ln._1.toSeq ++ Seq(ln._2))
      ),
      StructType(
        df.schema.fields ++ Array(StructField(colName, LongType, false))
      )
    )
  }

  def getDataSourceName(taskSummary: TaskSummary, step: Step): String = {
    s"${taskSummary.dataSourceName}.${step.name}"
  }

  def applySqlExpressions(df: DataFrame, foreignKeyFields: List[String] = List(), isIgnoreForeignColExists: Boolean = true): DataFrame = {
    // Recursively find all SQL expressions in the schema
    def findSqlExpressions(field: StructField, path: String = ""): List[(String, String)] = {
      val currentPath = if (path.isEmpty) field.name else s"$path.${field.name}"
      
      field.dataType match {
        case StructType(fields) =>
          // Check if this struct field itself has SQL
          val selfSql = if (field.metadata.contains(SQL_GENERATOR)) {
            val sqlExpr = field.metadata.getString(SQL_GENERATOR)
            if (isIgnoreForeignColExists || foreignKeyFields.exists(col => sqlExpr.contains(col))) {
              List((currentPath, sqlExpr))
            } else {
              List()
            }
          } else {
            List()
          }
          
          // Recursively find SQL expressions in nested fields
          val nestedSql = fields.flatMap(f => findSqlExpressions(f, currentPath)).toList
          selfSql ++ nestedSql
          
        case ArrayType(StructType(arrayFields), _) =>
          // Check if this array field itself has SQL
          val selfSql = if (field.metadata.contains(SQL_GENERATOR)) {
            val sqlExpr = field.metadata.getString(SQL_GENERATOR)
            if (isIgnoreForeignColExists || foreignKeyFields.exists(col => sqlExpr.contains(col))) {
              List((currentPath, sqlExpr))
            } else {
              List()
            }
          } else {
            List()
          }
          
          // Recursively find SQL expressions in array element fields
          val nestedSql = arrayFields.flatMap(f => findSqlExpressions(f, s"$currentPath.element")).toList
          selfSql ++ nestedSql
          
        case _ =>
          // Check if this field has SQL
          if (field.metadata.contains(SQL_GENERATOR)) {
            val sqlExpr = field.metadata.getString(SQL_GENERATOR)
            if (isIgnoreForeignColExists || foreignKeyFields.exists(col => sqlExpr.contains(col))) {
              List((currentPath, sqlExpr))
            } else {
              List()
            }
          } else {
            List()
          }
      }
    }
    
    // Find all SQL expressions in the schema
    val sqlExpressions = df.schema.fields.flatMap(findSqlExpressions(_)).toList
    
    if (sqlExpressions.isEmpty) {
      return df
    }
    
    // Step 1: Add temporary columns for all SQL expressions
    val sqlExpressionsWithoutForeignKeys = sqlExpressions.filter {
      case (_, sqlExpr) => isIgnoreForeignColExists || foreignKeyFields.exists(col => sqlExpr.contains(col))
    }
    
    var resultDf = df
    sqlExpressionsWithoutForeignKeys.foreach { case (path, sqlExpr) =>
      val tempColName = s"_temp_${path.replace(".", "_")}"
      resultDf = resultDf.withColumn(tempColName, expr(sqlExpr))
    }
    
    // Step 2: Build new columns using the temporary values
    val newColumns = resultDf.schema.fields.map { field =>
      buildColumnWithSqlResolution(field, resultDf, sqlExpressions)
    }
    
    // Step 3: Select the new columns
    val finalDf = resultDf.select(newColumns: _*)
    
    // Step 4: Remove omitted fields from the final DataFrame
    val tempColumns = sqlExpressionsWithoutForeignKeys.map { case (path, _) => s"_temp_${path.replace(".", "_")}" }
    finalDf.drop(tempColumns: _*)
  }
  
  // Helper function to build columns with SQL resolution
  private def buildColumnWithSqlResolution(field: StructField, df: DataFrame, sqlExpressions: List[(String, String)]): org.apache.spark.sql.Column = {
    
    def buildNestedColumn(structField: StructField, pathPrefix: String, baseColumnName: String): String = {
      val currentPath = if (pathPrefix.isEmpty) structField.name else s"$pathPrefix.${structField.name}"
      
      // Check if this field has a SQL expression
      sqlExpressions.find(_._1 == currentPath) match {
        case Some((_, sqlExpr)) =>
          // Use the temporary column value
          val tempColName = s"_temp_${currentPath.replace(".", "_")}"
          s"`$tempColName`"
          
        case None =>
          structField.dataType match {
            case StructType(nestedFields) =>
              // Check if any nested field has SQL expressions
              val hasNestedSql = nestedFields.exists(f => sqlExpressions.exists(_._1.startsWith(s"$currentPath.${f.name}")))
              
              if (hasNestedSql) {
                // Build nested structure with potential SQL expressions
                val fieldExprs = nestedFields.map { f =>
                  val nestedExpr = buildNestedColumn(f, currentPath, baseColumnName)
                  s"'${f.name}', $nestedExpr"
                }
                s"NAMED_STRUCT(${fieldExprs.mkString(", ")})"
              } else {
                // Use original column reference
                if (baseColumnName == "x") {
                  // In array context
                  s"x.${structField.name}"
                } else {
                  // Calculate field path relative to base column
                  val fieldPath = if (pathPrefix.length > baseColumnName.length && pathPrefix.startsWith(baseColumnName)) {
                    val suffixPath = pathPrefix.substring(baseColumnName.length + 1) // +1 for the dot
                    s"$baseColumnName.$suffixPath.${structField.name}"
                  } else {
                    s"$baseColumnName.${structField.name}"
                  }
                  fieldPath
                }
              }
              
            case ArrayType(StructType(arrayFields), _) =>
              // Check if any array element field has SQL expressions
              val hasArraySql = arrayFields.exists(f => sqlExpressions.exists(_._1.startsWith(s"$currentPath.element.${f.name}")))
              
              if (hasArraySql) {
                // Build TRANSFORM for array elements with SQL expressions
                val elementExprs = arrayFields.map { f =>
                  val nestedExpr = buildNestedColumn(f, s"$currentPath.element", "x")
                  s"'${f.name}', $nestedExpr"
                }
                
                // Calculate array field path
                if (baseColumnName == "x") {
                  // In nested array context
                  s"TRANSFORM(x.${structField.name}, x -> NAMED_STRUCT(${elementExprs.mkString(", ")}))"
                } else {
                  // Top-level array reference - use the full path to the array
                  val fullArrayPath = if (pathPrefix.isEmpty) {
                    // At top level, use just the field name
                    structField.name
                  } else if (pathPrefix == baseColumnName) {
                    // When pathPrefix equals baseColumnName, use baseColumnName.fieldName
                    s"$baseColumnName.${structField.name}"
                  } else {
                    // In nested context, use the full path
                    s"$pathPrefix.${structField.name}"
                  }
                  s"TRANSFORM($fullArrayPath, x -> NAMED_STRUCT(${elementExprs.mkString(", ")}))"
                }
              } else {
                // Use original array reference
                if (baseColumnName == "x") {
                  s"x.${structField.name}"
                } else {
                  val fieldPath = if (pathPrefix.length > baseColumnName.length && pathPrefix.startsWith(baseColumnName)) {
                    val suffixPath = pathPrefix.substring(baseColumnName.length + 1) // +1 for the dot
                    s"$baseColumnName.$suffixPath.${structField.name}"
                  } else {
                    s"$baseColumnName.${structField.name}"
                  }
                  fieldPath
                }
              }
              
            case _ =>
              // Primitive field - use original reference
              if (baseColumnName == "x") {
                s"x.${structField.name}"
              } else {
                val fieldPath = if (pathPrefix.length > baseColumnName.length && pathPrefix.startsWith(baseColumnName)) {
                  val suffixPath = pathPrefix.substring(baseColumnName.length + 1) // +1 for the dot
                  s"$baseColumnName.$suffixPath.${structField.name}"
                } else {
                  s"$baseColumnName.${structField.name}"
                }
                fieldPath
              }
          }
      }
    }
    
    // Check if the top-level field has a SQL expression
    sqlExpressions.find(_._1 == field.name) match {
      case Some((_, sqlExpr)) =>
        // Use the temporary column
        val tempColName = s"_temp_${field.name.replace(".", "_")}"
        col(tempColName).as(field.name)
        
      case None =>
        field.dataType match {
          case StructType(nestedFields) =>
            // Check if any nested field has SQL expressions
            val hasNestedSql = nestedFields.exists(f => sqlExpressions.exists(_._1.startsWith(s"${field.name}.${f.name}")))
            
            if (hasNestedSql) {
              // Build nested structure with SQL resolution
              val fieldExprs = nestedFields.map { f =>
                val nestedExpr = buildNestedColumn(f, field.name, field.name)
                s"'${f.name}', $nestedExpr"
              }
              
              try {
                val exprStr = s"NAMED_STRUCT(${fieldExprs.mkString(", ")})"
                expr(exprStr).as(field.name)
              } catch {
                case e: Exception =>
                  println(s"Failed to create expression for ${field.name}: ${e.getMessage}")
                  println(s"Expression was: ${s"NAMED_STRUCT(${fieldExprs.mkString(", ")})"}")
                  col(field.name)
              }
            } else {
              col(field.name)
            }
            
          case ArrayType(StructType(arrayFields), _) =>
            // Check if any array element field has SQL expressions
            val hasArraySql = arrayFields.exists(f => sqlExpressions.exists(_._1.startsWith(s"${field.name}.element.${f.name}")))
            
            if (hasArraySql) {
              // Build TRANSFORM for array elements with SQL expressions
              val elementExprs = arrayFields.map { f =>
                val nestedExpr = buildNestedColumn(f, s"${field.name}.element", "x")
                s"'${f.name}', $nestedExpr"
              }
              
              val exprStr = s"TRANSFORM(${field.name}, x -> NAMED_STRUCT(${elementExprs.mkString(", ")}))"
              try {
                expr(exprStr).as(field.name)
              } catch {
                case e: Exception =>
                  println(s"Failed to create expression for ${field.name}: ${e.getMessage}")
                  println(s"Expression was: $exprStr")
                  col(field.name)
              }
            } else {
              col(field.name)
            }
            
          case _ =>
            col(field.name)
        }
    }
  }

  def determineSaveTiming(dataSourceName: String, format: String, stepName: String): String = {
    format match {
      case HTTP | JMS =>
        LOGGER.debug(s"Given the step type is either HTTP or JMS, data will be generated in real-time mode. " +
          s"It will be based on requests per second defined at plan level, data-source-name=$dataSourceName, step-name=$stepName, format=$format")
        REAL_TIME
      case _ =>
        LOGGER.debug(s"Will generate data in batch mode for step, data-source-name=$dataSourceName, step-name=$stepName, format=$format")
        BATCH
    }
  }

}
