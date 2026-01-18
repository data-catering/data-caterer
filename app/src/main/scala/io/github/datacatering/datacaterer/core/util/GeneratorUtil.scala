package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants.{EXPRESSION, HTTP, JMS, KAFKA, ONE_OF_GENERATOR, REGEX_GENERATOR, ROWS_PER_SECOND, SQL_GENERATOR}
import io.github.datacatering.datacaterer.api.model.{Step, TaskSummary}
import io.github.datacatering.datacaterer.core.generator.provider.FastDataGenerator.{FastRegexDataGenerator, FastStringDataGenerator}
import io.github.datacatering.datacaterer.core.generator.provider.{DataGenerator, OneOfDataGenerator, RandomDataGenerator}
import io.github.datacatering.datacaterer.core.model.Constants.{BATCH, REAL_TIME}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

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

    getDataGenerator(structField, faker, enableFastGeneration, hasSql, hasExpression, hasRegex, hasOneOf)
  }

  /**
   * Get data generator with fast mode support and generator options.
   */
  def getDataGenerator(generatorOpts: Map[String, Any], structField: StructField, faker: Faker, enableFastGeneration: Boolean): DataGenerator[_] = {
    val hasOneOfGenerator = generatorOpts.contains(ONE_OF_GENERATOR)
    val hasRegex = generatorOpts.contains(REGEX_GENERATOR)
    val hasExpression = generatorOpts.contains(EXPRESSION)
    val hasSql = generatorOpts.contains(SQL_GENERATOR)

    getDataGenerator(structField, faker, enableFastGeneration, hasSql, hasExpression, hasRegex, hasOneOfGenerator)
  }

  private def getDataGenerator(structField: StructField, faker: Faker, enableFastGeneration: Boolean, hasSql: Boolean, hasExpression: Boolean, hasRegex: Boolean, hasOneOf: Boolean) = {
    if (hasOneOf) {
      OneOfDataGenerator.getGenerator(structField, faker)
    } else if (hasRegex) {
      // Always use FastRegexDataGenerator for regex patterns (it falls back to UDF for unsupported patterns)
      new FastRegexDataGenerator(structField, faker)
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
    val dsName = if (taskSummary.dataSourceName.isEmpty) {
      throw new IllegalStateException("Task summary cannot have empty dataSourceName")
    } else {
      taskSummary.dataSourceName
    }
    s"$dsName.${step.name}"
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
    
    // Separate array element expressions from regular expressions
    val (_, regularExpressions) = sqlExpressions.partition(_._1.contains(".element."))
    
    // Step 1: Add temporary columns for non-array element SQL expressions only
    val sqlExpressionsWithoutForeignKeys = regularExpressions.filter {
      case (_, sqlExpr) => isIgnoreForeignColExists || foreignKeyFields.exists(col => sqlExpr.contains(col))
    }
    
    var resultDf = df

    // Identify identity expressions (expr equals original path), e.g., __index_inc
    def isIdentity(path: String, exprStr: String): Boolean = {
      val p = path.replace("`", "").trim
      val e = exprStr.replace("`", "").trim
      e.equalsIgnoreCase(p)
    }

    val identityPaths: Set[String] = sqlExpressionsWithoutForeignKeys.collect {
      case (path, exprStr) if isIdentity(path, exprStr) => path
    }.toSet

    // Build map of regular (non-array) non-identity sql paths -> temp column names
    val tempNameByPath: Map[String, String] = sqlExpressionsWithoutForeignKeys.collect {
      case (path, _) if !identityPaths.contains(path) =>
        path -> s"_temp_${path.replace(".", "_")}"
    }.toMap

    def rewriteWithTempRefs(raw: String, currentPath: String): String = {
      // Replace references to other regular SQL-generated columns with their temp columns
      // Sort keys by length to avoid partial replacements
      val keys = tempNameByPath.keys.filterNot(_ == currentPath).toList.sortBy(-_.length)
      keys.foldLeft(raw) { case (acc, path) =>
        val temp = tempNameByPath(path)
        // Match backticked or plain path as a whole token using simpler boundaries
        val pattern = ("(?i)\\b" + java.util.regex.Pattern.quote(path) + "\\b").r
        val patternTick = ("(?i)`" + java.util.regex.Pattern.quote(path) + "`").r
        val replacedTicks = patternTick.replaceAllIn(acc, s"`$temp`")
        pattern.replaceAllIn(replacedTicks, s"`$temp`")
      }
    }
    def lookupDataType(path: String): Option[DataType] = {
      val parts = path.split("\\.").toList
      def loop(fields: Array[StructField], remaining: List[String]): Option[DataType] = remaining match {
        case Nil => None
        case name :: Nil => fields.find(_.name == name).map(_.dataType)
        case name :: tail =>
          fields.find(_.name == name).flatMap { f =>
            f.dataType match {
              case StructType(nested) => loop(nested.toArray, tail)
              case _ => None
            }
          }
      }
      loop(df.schema.fields, parts)
    }

    // Phase 2 Optimization: Batch SQL expression resolution
    // Build expressions in dependency order to handle lateral column aliases
    // Spark doesn't support lateral aliases in same selectExpr, so we batch by dependency level

    val tempColumnInfo = sqlExpressionsWithoutForeignKeys.flatMap { case (path, sqlExpr) =>
      val tempColName = s"_temp_${path.replace(".", "_")}"
      val isIdentityExpr = isIdentity(path, sqlExpr)
      if (!isIdentityExpr) {
        val rewired = rewriteWithTempRefs(sqlExpr, path)
        val finalExpr = lookupDataType(path) match {
          case Some(dt) if dt.typeName == "string" => s"CAST(($rewired) AS STRING)"
          case _ => rewired
        }
        LOGGER.debug(s"Adding temp SQL column [$tempColName] for path=[$path], expr=[$finalExpr]")
        Some((tempColName, finalExpr, rewired))
      } else {
        LOGGER.debug(s"Skipping temp SQL column for identity expr path=[$path], expr=[$sqlExpr]")
        None
      }
    }

    // Group expressions by whether they reference other temp columns
    // Level 0: No temp column references (can be done in one batch)
    // Level 1+: References temp columns from previous level (need sequential application)
    def getDependencyLevel(expr: String, tempNames: Set[String]): Int = {
      val referencesTemp = tempNames.exists(name => expr.contains(name))
      if (referencesTemp) 1 else 0
    }

    val tempNames = tempColumnInfo.map(_._1).toSet
    val level0 = tempColumnInfo.filter { case (_, _, rewired) => getDependencyLevel(rewired, tempNames) == 0 }
    val level1 = tempColumnInfo.filter { case (_, _, rewired) => getDependencyLevel(rewired, tempNames) > 0 }

    // Apply level 0 expressions in batch
    if (level0.nonEmpty) {
      val existingColumns = resultDf.columns.map(c => s"`$c`")
      val level0Exprs = level0.map { case (name, expr, _) => s"($expr) AS `$name`" }
      resultDf = resultDf.selectExpr(existingColumns ++ level0Exprs: _*)
    }

    // Apply level 1 expressions one at a time (they may have lateral dependencies)
    level1.foreach { case (tempColName, finalExpr, _) =>
      resultDf = resultDf.withColumn(tempColName, expr(finalExpr))
    }
    
    // Step 2: Build new columns using the temporary values for regular expressions
    // and inline processing for array element expressions
    // Build columns only for original schema fields, not for temporary helper columns
    val newColumns = df.schema.fields.map { field =>
      buildColumnWithSqlResolution(field, resultDf, sqlExpressions, identityPaths, tempNameByPath)
    }
    
    // Step 3: Select the new columns
    val finalDf = resultDf.select(newColumns: _*)
    
    // Step 4: Remove omitted fields from the final DataFrame
    val tempColumns = sqlExpressionsWithoutForeignKeys.map { case (path, _) => s"_temp_${path.replace(".", "_")}" }
    finalDf.drop(tempColumns: _*)
  }
  
  /**
   * Transform SQL expression to work within array element context
   * This function replaces array field references with element references
   */
  private def transformSqlExpressionForArrayElement(sqlExpr: String, arrayPath: String): String = {
    // Get the array field name from the path (e.g., "transaction_history" from "transaction_history.element")
    val arrayFieldName = arrayPath.split("\\.element").head
    
    // Create a regex pattern to match array field references
    // This matches patterns like "transaction_history.field_name"
    val arrayFieldPattern = s"\\b$arrayFieldName\\.(\\w+)\\b".r
    
    // Replace array field references with element references
    val transformedExpr = arrayFieldPattern.replaceAllIn(sqlExpr, m => {
      val fieldName = m.group(1)
      s"x.$fieldName"
    })
    
    LOGGER.debug(s"Transformed SQL expression for array element: '$sqlExpr' -> '$transformedExpr'")
    transformedExpr
  }

  /**
   * Expand SQL expression for an array element by inlining references to other calculated fields
   * within the same array element. This avoids relying on ordering inside NAMED_STRUCT which
   * is not supported by Spark SQL during TRANSFORM lambdas.
   *
   * Example:
   *  - Given array "orders" with expressions:
   *      orders.element.tax = orders.amount * 0.1
   *      orders.element.total = orders.amount + orders.tax
   *    Expands `orders.total` to `x.amount + (x.amount * 0.1)`
   */
  private def expandArrayElementSqlExpression(
                                               rawExpr: String,
                                               arrayPath: String,
                                               elementExpressionsByArray: Map[String, Map[String, String]],
                                               lambdaVarByArrayName: Map[String, String]
                                             ): String = {
    // Example arrayPath: "organizations.element.departments.element"
    // We want arrayRootPathKey: "organizations.departments"
    val arrayRootPathKey = {
      val lastIdx = arrayPath.lastIndexOf(".element")
      val upToLast = if (lastIdx >= 0) arrayPath.substring(0, lastIdx) else arrayPath
      upToLast.replaceAll("\\.element", "")
    }
    val elementExprs: Map[String, String] = elementExpressionsByArray.getOrElse(arrayRootPathKey, Map.empty)
    // Determine the correct lambda variable for this array by matching the innermost array name in scope
    val currentLambdaVar = {
      val keysByLength = lambdaVarByArrayName.keys.toList.sortBy(-_.length)
      keysByLength.find(k => arrayRootPathKey == k || arrayRootPathKey.endsWith(s".$k")).flatMap(lambdaVarByArrayName.get).getOrElse("x")
    }

    // Build a memoized recursive expander to inline same-element references
    import scala.collection.mutable
    val memo: mutable.Map[String, String] = mutable.Map.empty

    def sanitizeFieldPath(path: String): String = path.stripPrefix(".")

    // Recursively expand an element-level field path (relative to element, e.g. "amount", "nested.tax")
    def expandField(fieldPathRel: String, visited: Set[String]): String = {
      val clean = sanitizeFieldPath(fieldPathRel)
      if (memo.contains(clean)) return memo(clean)
      if (visited.contains(clean)) {
        // Cycle detected; fallback to x.<field> to avoid infinite recursion
        val fallback = s"$currentLambdaVar.$clean"
        memo.update(clean, fallback)
        return fallback
      }
      elementExprs.get(clean) match {
        case Some(depExprRaw) =>
          // First transform any array references to lambda element scope
          val depExprWithX = transformSqlExpressionForArrayElement(depExprRaw, s"$arrayRootPathKey.element")
          // Now inline any references to other element-level calculated fields
          val inlined = inlineSameElementReferences(depExprWithX, visited + clean)
          memo.update(clean, inlined)
          inlined
        case None =>
          val baseRef = s"$currentLambdaVar.$clean"
          memo.update(clean, baseRef)
          baseRef
      }
    }

    // Replace occurrences of same-element references (either `array.field` or `x.field`) with their expansions
    def inlineSameElementReferences(exprIn: String, visited: Set[String]): String = {
      // Matches same-array path prefix (supports dotted array roots) like `organizations.departments.field`
      val arrayRefPattern = ("(?i)(`?" + java.util.regex.Pattern.quote(arrayRootPathKey) + "`?\\.)([a-zA-Z0-9_`\\.]+)").r

      // First replace arrayName.* references
      val replacedArrayRefs = arrayRefPattern.replaceAllIn(exprIn, m => {
        val relPathRaw = m.group(2)
        val relPath = relPathRaw.replace("`", "")
        val expansion = expandField(relPath, visited)
        // Ensure proper grouping
        s"($expansion)"
      })

      // Replace references to other arrays in scope, retarget to nearest scoped lambda
      val afterScopeSwap = {
        // Find all occurrences of a dotted prefix followed by more path, then rewrite selectively
        val prefixAndRest = "(?i)(`?[a-zA-Z0-9_]+`?(?:\\.`?[a-zA-Z0-9_]+`?)*)\\.([a-zA-Z0-9_`\\.]+)".r
        prefixAndRest.replaceAllIn(replacedArrayRefs, m => {
          val prefixFull = m.group(1).replace("`", "")
          val rest = m.group(2).replace("`", "")
          val prefixParts = prefixFull.split("\\.").toList

          // Choose the deepest part that is an array in scope
          val chosenOpt = prefixParts.reverse.find(p => lambdaVarByArrayName.contains(p))
          chosenOpt match {
          case Some(chosenArray) =>
              val lambda = lambdaVarByArrayName.getOrElse(chosenArray, currentLambdaVar)
              val restParts = rest.split("\\.").toList
              // Build the path segments AFTER the chosen array within prefixFull
              val idxChosen = prefixParts.lastIndexOf(chosenArray)
              val afterChosenInPrefix = if (idxChosen >= 0 && idxChosen < prefixParts.length - 1) prefixParts.drop(idxChosen + 1) else Nil
              val fullRelAfterChosen = afterChosenInPrefix ++ restParts
              // If the first segment after chosen is itself an array in scope, retarget to that lambda
              fullRelAfterChosen match {
                case head :: tail if lambdaVarByArrayName.contains(head) =>
                  val innerLambda = lambdaVarByArrayName(head)
                  val tailStr = tail.mkString(".")
                  if (tailStr.nonEmpty) s"$innerLambda.$tailStr" else innerLambda
                case _ => s"$lambda.${fullRelAfterChosen.mkString(".")}"
              }
            case None => m.matched // leave unchanged
          }
        })
      }

      // Cleanup: remove accidental prefixes like "organizations.y.*" => "y.*" for all arrays/lambdas in scope
      val cleaned = lambdaVarByArrayName.foldLeft(afterScopeSwap) { case (accExpr, (arrayName, lambdaVar)) =>
        val prefixDotPattern = ("(?i)(`?" + java.util.regex.Pattern.quote(arrayName) + "`?\\.)" + java.util.regex.Pattern.quote(lambdaVar) + "\\.").r
        val prefixWordBoundaryPattern = ("(?i)(`?" + java.util.regex.Pattern.quote(arrayName) + "`?\\.)" + java.util.regex.Pattern.quote(lambdaVar) + "\\b").r
        val step1 = prefixDotPattern.replaceAllIn(accExpr, _ => s"$lambdaVar.")
        prefixWordBoundaryPattern.replaceAllIn(step1, _ => s"$lambdaVar")
      }
      cleaned
    }

    val finalExpr = inlineSameElementReferences(rawExpr, Set.empty)
    LOGGER.debug(s"Inline array element SQL for [$arrayRootPathKey] raw=[$rawExpr] -> [$finalExpr], lambdas=$lambdaVarByArrayName")
    finalExpr
  }
  
  // Helper function to build columns with SQL resolution
  private def buildColumnWithSqlResolution(field: StructField, df: DataFrame, sqlExpressions: List[(String, String)], identityPaths: Set[String] = Set.empty, tempNameByPath: Map[String, String] = Map.empty): org.apache.spark.sql.Column = {
    
    // Allocate readable distinct lambda variable names by depth
    def allocateLambda(depth: Int): String = depth match {
      case 0 => "x"
      case 1 => "y"
      case 2 => "z"
      case 3 => "w"
      case n => s"v$n"
    }

    def buildNestedColumn(structField: StructField, pathPrefix: String, baseColumnName: String, lambdaVarByArrayName: Map[String, String]): String = {
      val currentPath = if (pathPrefix.isEmpty) structField.name else s"$pathPrefix.${structField.name}"
      
      // Check if this field has a SQL expression
      sqlExpressions.find(_._1 == currentPath) match {
        case Some((_, sqlExpr)) =>
          // Check if this is an array element expression
          if (currentPath.contains(".element.")) {
            // For array elements, inline same-element calculated field references
            val arrayPath = pathPrefix // e.g., "orders.element"
            val elementExprsByArray: Map[String, Map[String, String]] = buildElementSqlMapByArray(sqlExpressions)
            expandArrayElementSqlExpression(sqlExpr, arrayPath, elementExprsByArray, lambdaVarByArrayName)
          } else {
            // For identity expressions (expr equals original path), keep original path
            if (identityPaths.contains(currentPath)) {
              currentPath
            } else {
              // Use the temporary column value for regular expressions if it exists; otherwise, fall back to original path.
              val tempColName = tempNameByPath.getOrElse(currentPath, s"_temp_${currentPath.replace(".", "_")}")
              if (df.columns.contains(tempColName)) s"`$tempColName`" else currentPath
            }
          }
          
        case None =>
          structField.dataType match {
            case StructType(nestedFields) =>
              // Check if any nested field has SQL expressions
              val hasNestedSql = nestedFields.exists(f => sqlExpressions.exists(_._1.startsWith(s"$currentPath.${f.name}")))
              
              if (hasNestedSql) {
                // Build nested structure with potential SQL expressions
                val fieldExprs = nestedFields.map { f =>
                  val nestedExpr = buildNestedColumn(f, currentPath, baseColumnName, lambdaVarByArrayName)
                  s"'${f.name}', $nestedExpr"
                }
                s"NAMED_STRUCT(${fieldExprs.mkString(", ")})"
              } else {
                // Use original struct reference (no nested SQL) without duplicating the field name
                if (baseColumnName.nonEmpty) s"$baseColumnName.${structField.name}" else currentPath
              }
              
            case ArrayType(StructType(arrayFields), _) =>
              // Check if any array element field has SQL expressions
              val hasArraySql = arrayFields.exists(f => sqlExpressions.exists(_._1.startsWith(s"$currentPath.element.${f.name}")))
              
              if (hasArraySql) {
                // Build TRANSFORM for array elements with SQL expressions
                val newLambda = allocateLambda(lambdaVarByArrayName.size)
                val updatedScope = lambdaVarByArrayName + (structField.name -> newLambda)
                val elementExprs = arrayFields.map { f =>
                  val nestedExpr = buildNestedColumn(f, s"$currentPath.element", newLambda, updatedScope)
                  s"'${f.name}', $nestedExpr"
                }
                
                // Calculate array field path
                if (baseColumnName.nonEmpty) {
                  // In nested array context
                  s"TRANSFORM($baseColumnName.${structField.name}, $newLambda -> NAMED_STRUCT(${elementExprs.mkString(", ")}))"
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
                  s"TRANSFORM($fullArrayPath, $newLambda -> NAMED_STRUCT(${elementExprs.mkString(", ")}))"
                }
              } else {
                // Use original array reference
                if (baseColumnName.nonEmpty) s"$baseColumnName.${structField.name}" else currentPath
              }
              
            case _ =>
              // Primitive field - use original reference
              if (baseColumnName.nonEmpty) s"$baseColumnName.${structField.name}" else currentPath
          }
      }
    }
    
    // Check if the top-level field has a SQL expression
    sqlExpressions.find(_._1 == field.name) match {
      case Some((_, _)) =>
        // Always keep the original index column
        if (field.name == "__index_inc") {
          col(field.name)
        } else if (identityPaths.contains(field.name)) {
          // For identity expressions, keep the original column
          col(field.name)
        } else {
          // Use the temporary column if it exists; otherwise, keep the original column
          val tempColName = tempNameByPath.getOrElse(field.name, s"_temp_${field.name.replace(".", "_")}")
          if (df.columns.contains(tempColName)) col(tempColName).as(field.name) else col(field.name)
        }
        
      case None =>
        field.dataType match {
          case StructType(nestedFields) =>
            // Check if any nested field has SQL expressions
            val hasNestedSql = nestedFields.exists(f => sqlExpressions.exists(_._1.startsWith(s"${field.name}.${f.name}")))
            
            if (hasNestedSql) {
              // Build nested structure with SQL resolution
              val fieldExprs = nestedFields.map { f =>
                val nestedExpr = buildNestedColumn(f, field.name, "", Map.empty)
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
              val rootLambda = allocateLambda(0)
              val scope = Map(field.name -> rootLambda)
              val elementExprs = arrayFields.map { f =>
                val nestedExpr = buildNestedColumn(f, s"${field.name}.element", rootLambda, scope)
                s"'${f.name}', $nestedExpr"
              }
              
              val exprStr = s"TRANSFORM(${field.name}, $rootLambda -> NAMED_STRUCT(${elementExprs.mkString(", ")}))"
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

  // Build a map from array field name -> (element-relative field path -> SQL expression)
  private def buildElementSqlMapByArray(sqlExpressions: List[(String, String)]): Map[String, Map[String, String]] = {
    // Collect paths that are element-level, e.g., "orders.element.total"
    val elementLevel = sqlExpressions.collect {
      case (path, expr) if path.contains(".element.") => (path, expr)
    }

    elementLevel.groupBy { case (path, _) => path.split("\\.element").head }
      .map { case (arrayField, entries) =>
        val relMap: Map[String, String] = entries.map { case (path, expr) =>
          val rel = path.substring(path.indexOf(".element.") + ".element.".length) // relative to element
          rel -> expr
        }.toMap
        arrayField -> relMap
      }
  }

  /**
   * Determines whether to use batch or real-time mode for saving data.
   *
   * Real-time mode is used when:
   * - Format is HTTP or JMS (always real-time)
   * - Format is Kafka and rowsPerSecond is configured (rate-limited streaming)
   *
   * Batch mode is used for all other cases including Kafka without rate limiting.
   */
  def determineSaveTiming(dataSourceName: String, format: String, stepName: String): String = {
    determineSaveTiming(dataSourceName, format, stepName, Map.empty)
  }

  /**
   * Determines whether to use batch or real-time mode for saving data.
   *
   * Real-time mode is used when:
   * - Format is HTTP or JMS (always real-time)
   * - Format is Kafka and rowsPerSecond is configured (rate-limited streaming)
   *
   * Batch mode is used for all other cases including Kafka without rate limiting.
   */
  def determineSaveTiming(dataSourceName: String, format: String, stepName: String, stepOptions: Map[String, String]): String = {
    val hasRateLimit = stepOptions.contains(ROWS_PER_SECOND)

    format match {
      case HTTP | JMS =>
        LOGGER.info(s"Using real-time mode for sink (HTTP/JMS always uses real-time), " +
          s"data-source-name=$dataSourceName, step-name=$stepName, format=$format")
        REAL_TIME
      case KAFKA if hasRateLimit =>
        val rate = stepOptions.getOrElse(ROWS_PER_SECOND, "unknown")
        LOGGER.info(s"Using real-time mode for Kafka sink with rate limiting, " +
          s"data-source-name=$dataSourceName, step-name=$stepName, format=$format, rowsPerSecond=$rate")
        REAL_TIME
      case KAFKA =>
        LOGGER.info(s"Using batch mode for Kafka sink (no rate limiting configured, set 'rowsPerSecond' to enable rate limiting), " +
          s"data-source-name=$dataSourceName, step-name=$stepName, format=$format")
        BATCH
      case _ =>
        LOGGER.debug(s"Using batch mode for sink, data-source-name=$dataSourceName, step-name=$stepName, format=$format")
        BATCH
    }
  }

  /**
   * Parse duration string to seconds.
   * Supports formats like: "30s", "5m", "1h", "2h30m15s"
   */
  def parseDurationToSeconds(duration: String): Double = {
    val pattern = """(\d+)([smh])""".r
    val matches = pattern.findAllMatchIn(duration.toLowerCase)

    matches.foldLeft(0.0) { (total, m) =>
      val value = m.group(1).toDouble
      val unit = m.group(2)
      val seconds = unit match {
        case "s" => value
        case "m" => value * 60
        case "h" => value * 3600
        case _ => 0.0
      }
      total + seconds
    }
  }

  def parseDurationToMillis(duration: String): Long = {
    (parseDurationToSeconds(duration) * 1000).toLong
  }
}
