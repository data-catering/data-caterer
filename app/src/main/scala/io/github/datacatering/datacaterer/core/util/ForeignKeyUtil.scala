package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants.OMIT
import io.github.datacatering.datacaterer.api.model.{ForeignKey, Plan}
import io.github.datacatering.datacaterer.core.exception.MissingDataSourceFromForeignKeyException
import io.github.datacatering.datacaterer.core.model.{ForeignKeyRelationship, ForeignKeyWithGenerateAndDelete}
import io.github.datacatering.datacaterer.core.util.ForeignKeyRelationHelper.updateForeignKeyName
import io.github.datacatering.datacaterer.core.util.GeneratorUtil.applySqlExpressions
import io.github.datacatering.datacaterer.core.util.PlanImplicits.{ForeignKeyRelationOps, SinkOptionsOps}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, expr, struct}
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, Metadata, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, Column}

import scala.annotation.tailrec
import scala.collection.mutable

object ForeignKeyUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Apply same values from source data frame fields to target foreign key fields
   *
   * @param plan                     where foreign key definitions are defined
   * @param generatedDataForeachTask list of <dataSourceName>.<stepName> => generated data as dataframe
   * @return map of <dataSourceName>.<stepName> => dataframe
   */
  def getDataFramesWithForeignKeys(plan: Plan, generatedDataForeachTask: List[(String, DataFrame)]): List[(String, DataFrame)] = {
    val generatedDataForeachTaskMap = generatedDataForeachTask.toMap
    val enabledSources = plan.tasks.filter(_.enabled).map(_.dataSourceName)
    val sinkOptions = plan.sinkOptions.get
    val foreignKeyRelations = sinkOptions.foreignKeys
      .map(fk => sinkOptions.gatherForeignKeyRelations(fk.source))
    val enabledForeignKeys = foreignKeyRelations
      .filter(fkr => isValidForeignKeyRelation(generatedDataForeachTaskMap, enabledSources, fkr))
    var taskDfs = generatedDataForeachTask

    val foreignKeyAppliedDfs = enabledForeignKeys.flatMap(foreignKeyDetails => {
      val sourceDfName = foreignKeyDetails.source.dataFrameName
      LOGGER.debug(s"Getting source dataframe, source=$sourceDfName")
      val optSourceDf = taskDfs.find(task => task._1.equalsIgnoreCase(sourceDfName))
      if (optSourceDf.isEmpty) {
        throw MissingDataSourceFromForeignKeyException(sourceDfName)
      }
      val sourceDf = optSourceDf.get._2

      val sourceDfsWithForeignKey = foreignKeyDetails.generationLinks.map(target => {
        val targetDfName = target.dataFrameName
        LOGGER.debug(s"Getting target dataframe, source=$targetDfName")
        val optTargetDf = taskDfs.find(task => task._1.equalsIgnoreCase(targetDfName))
        if (optTargetDf.isEmpty) {
          throw MissingDataSourceFromForeignKeyException(targetDfName)
        }

        val targetDf = optTargetDf.get._2
        if (target.fields.forall(field => hasDfContainField(field, targetDf.schema.fields))) {
          LOGGER.info(s"Applying foreign key values to target data source, source-data=${foreignKeyDetails.source.dataSource}, target-data=${target.dataSource}")
          val dfWithForeignKeys = applyForeignKeysToTargetDf(sourceDf, targetDf, foreignKeyDetails.source.fields, target.fields)
          if (!dfWithForeignKeys.storageLevel.useMemory) dfWithForeignKeys.cache()
          (targetDfName, dfWithForeignKeys)
        } else {
          LOGGER.warn(s"Foreign key data source does not contain all foreign key(s) defined in plan, defaulting to base generated data, " +
            s"target-foreign-key-fields=${target.fields.mkString(",")}, target-columns=${targetDf.columns.mkString(",")}")
          (targetDfName, targetDf)
        }
      })
      taskDfs ++= sourceDfsWithForeignKey.toMap
      sourceDfsWithForeignKey
    })

    val insertOrder = getInsertOrder(foreignKeyRelations.map(f => (f.source.dataFrameName, f.generationLinks.map(_.dataFrameName))))
    val insertOrderDfs = insertOrder
      .map(s => {
        foreignKeyAppliedDfs.find(f => f._1.equalsIgnoreCase(s))
          .getOrElse(s -> taskDfs.find(t => t._1.equalsIgnoreCase(s)).get._2)
      })
    val nonForeignKeyTasks = taskDfs.filter(t => !insertOrderDfs.exists(_._1.equalsIgnoreCase(t._1)))
    insertOrderDfs ++ nonForeignKeyTasks
  }

  def isValidForeignKeyRelation(generatedDataForeachTask: Map[String, DataFrame], enabledSources: List[String],
                                fkr: ForeignKeyWithGenerateAndDelete) = {
    val isMainForeignKeySourceEnabled = enabledSources.contains(fkr.source.dataSource)
    val subForeignKeySources = fkr.generationLinks.map(_.dataSource)
    val isSubForeignKeySourceEnabled = subForeignKeySources.forall(enabledSources.contains)
    val disabledSubSources = subForeignKeySources.filter(s => !enabledSources.contains(s))
    if (!generatedDataForeachTask.contains(fkr.source.dataFrameName)) {
      throw MissingDataSourceFromForeignKeyException(fkr.source.dataFrameName)
    }
    val mainDfFields = generatedDataForeachTask(fkr.source.dataFrameName).schema.fields
    val fieldExistsMain = fkr.source.fields.forall(c => hasDfContainField(c, mainDfFields))

    if (!isMainForeignKeySourceEnabled) {
      LOGGER.warn(s"Foreign key data source is not enabled. Data source needs to be enabled for foreign key relationship " +
        s"to exist from generated data, data-source-name=${fkr.source.dataSource}")
    }
    if (!isSubForeignKeySourceEnabled) {
      LOGGER.warn(s"Sub data sources within foreign key relationship are not enabled, disabled-task=${disabledSubSources.mkString(",")}")
    }
    if (!fieldExistsMain) {
      LOGGER.warn(s"Main field for foreign key references is not created, data-source-name=${fkr.source.dataSource}, field=${fkr.source.fields}")
    }
    isMainForeignKeySourceEnabled && isSubForeignKeySourceEnabled && fieldExistsMain
  }

  def hasDfContainField(field: String, fields: Array[StructField]): Boolean = {
    if (field.contains(".")) {
      val spt = field.split("\\.")
      fields.find(_.name == spt.head)
        .exists(field => checkNestedFields(spt, field.dataType))
    } else {
      fields.exists(_.name == field)
    }
  }

  @tailrec
  private def checkNestedFields(spt: Array[String], dataType: DataType): Boolean = {
    val tailColName = spt.tail
    dataType match {
      case StructType(nestedFields) =>
        hasDfContainField(tailColName.mkString("."), nestedFields)
      case ArrayType(elementType, _) =>
        checkNestedFields(spt, elementType)
      case _ => false
    }
  }

  private def applyForeignKeysToTargetDf(sourceDf: DataFrame, targetDf: DataFrame, sourceFields: List[String], targetFields: List[String]): DataFrame = {
    if (!sourceDf.storageLevel.useMemory) sourceDf.cache() //TODO do we checkpoint instead of cache? checkpoint based on total number of records?
    if (!targetDf.storageLevel.useMemory) targetDf.cache()
    
    // Separate nested and flat fields
    val nestedFields = targetFields.zip(sourceFields).filter(_._1.contains("."))
    val flatFields = targetFields.zip(sourceFields).filter(!_._1.contains("."))
    
    LOGGER.debug(s"Applying foreign key values to target DF, source=${sourceFields.mkString(",")}, target=${targetFields.mkString(",")}, nested-fields=${nestedFields.size}, flat-fields=${flatFields.size}")
    
    // If we have both flat and nested fields in the same foreign key relationship,
    // we need to use sampling for ALL fields to ensure consistency
    val hasMixedFields = flatFields.nonEmpty && nestedFields.nonEmpty
    
    val resultDf = if (flatFields.nonEmpty && !hasMixedFields) {
      // Pure flat fields case - use original join approach
      val flatSourceFields = flatFields.map(_._2)
      val flatTargetFields = flatFields.map(_._1)
      
      val sourceColRename = flatSourceFields.map(c => {
        if (c.contains(".")) {
          val lastCol = c.split("\\.").last
          (lastCol, s"_src_$lastCol")
        } else {
          (c, s"_src_$c")
        }
      }).toMap
      val distinctSourceKeys = zipWithIndex(
        sourceDf.selectExpr(flatSourceFields: _*).distinct()
          .withColumnsRenamed(sourceColRename)
      )
      val distinctTargetKeys = zipWithIndex(targetDf.selectExpr(flatTargetFields: _*).distinct())

      LOGGER.debug(s"Attempting to join source DF keys with target DF, source=${flatSourceFields.mkString(",")}, target=${flatTargetFields.mkString(",")}")
      val joinDf = distinctSourceKeys.join(distinctTargetKeys, Seq("_join_foreign_key"))
        .drop("_join_foreign_key")
      val targetColRename = flatTargetFields.zip(flatSourceFields).map(c => {
        if (c._2.contains(".")) {
          val lastCol = c._2.split("\\.").last
          (c._1, col(s"_src_$lastCol"))
        } else {
          (c._1, col(s"_src_${c._2}"))
        }
      }).toMap
      targetDf.join(joinDf, flatTargetFields)
        .withColumns(targetColRename)
        .drop(sourceColRename.values.toList: _*)
    } else {
      targetDf
    }
    
    // Apply sampling approach for nested fields OR mixed fields
    val finalResult = if (nestedFields.nonEmpty || hasMixedFields) {
      LOGGER.debug(s"Processing nested fields using sampling approach, nested fields: ${nestedFields.size}")
      
      // Get all source fields for this foreign key relationship
      val allSourceFields = sourceFields.distinct
      val sourceValues = sourceDf.selectExpr(allSourceFields: _*).distinct().collect()
      
      // Add a column with a consistent random index for all foreign key fields
      val dfWithRandomIndex = resultDf.withColumn("_fk_random_index", expr(s"cast(floor(rand() * ${sourceValues.length}) + 1 as int)"))
      
      // Apply all fields using the same random index to ensure consistent relationships
      val allFields = if (hasMixedFields) targetFields.zip(sourceFields) else nestedFields
      val dfWithUpdatedFields = allFields.foldLeft(dfWithRandomIndex) { case (df, (targetField, sourceField)) =>
        // Create arrays of all possible values for this source field
        val fieldValues = sourceValues.map(row => {
          val value = row.getAs[Any](sourceField)
          // Convert to string for SQL expression, properly escaping quotes
          value match {
            case s: String => s"'${s.replace("'", "''").replace("\\", "\\\\")}'"
            case other => s"'$other'"
          }
        }).mkString(",")
        
        // Create SQL expression that uses the same random index for all fields
        // This ensures that all fields in the same foreign key relationship get values from the same row
        val sqlExpr = s"element_at(array($fieldValues), _fk_random_index)"
        
        if (targetField.contains(".")) {
          updateNestedField(df, targetField, sqlExpr)
        } else {
          // Handle flat fields with sampling approach when mixed with nested fields
          df.withColumn(targetField, expr(sqlExpr))
        }
      }
      
      // Remove the temporary random index column
      dfWithUpdatedFields.drop("_fk_random_index")
    } else {
      resultDf
    }

    LOGGER.debug(s"Applied source DF keys with target DF, source=${sourceFields.mkString(",")}, target=${targetFields.mkString(",")}")
    if (!finalResult.storageLevel.useMemory) finalResult.cache()
    //need to add back original metadata as it will use the metadata from the sourceDf and override the targetDf metadata
    val dfMetadata = combineMetadata(sourceDf, sourceFields, targetDf, targetFields, finalResult)
    
    // Store the original schema to ensure we only keep original fields in the final output
    val originalSchema = targetDf.schema
    
    // Only apply SQL expressions for flat fields that were handled via join (not sampling)
    val flatFieldsToProcess = if (hasMixedFields) List() else targetFields.filter(!_.contains("."))
    val dfWithSqlExpressions = applySqlExpressions(dfMetadata, flatFieldsToProcess, false)
    
    // Ensure only original schema fields are kept in the final output
    val originalFieldNames = originalSchema.fieldNames.toSet
    val finalFieldNames = dfWithSqlExpressions.schema.fieldNames.filter(originalFieldNames.contains)
    
    if (finalFieldNames.length != dfWithSqlExpressions.schema.fieldNames.length) {
      LOGGER.debug(s"Removing flattened fields from final output: " +
        s"original-fields=${originalFieldNames.mkString(",")}, " +
        s"current-fields=${dfWithSqlExpressions.schema.fieldNames.mkString(",")}")
      dfWithSqlExpressions.select(finalFieldNames.map(col): _*)
    } else {
      dfWithSqlExpressions
    }
  }
  
  /**
   * Update a nested field in a DataFrame properly
   */
  private def updateNestedField(df: DataFrame, fieldPath: String, sqlExpr: String): DataFrame = {
    val parts = fieldPath.split("\\.")
    LOGGER.debug(s"Updating nested field: path=$fieldPath, depth=${parts.length}")
    
    try {
      if (parts.length == 2) {
        val structField = parts(0)
        val nestedField = parts(1)
        
        // Get the current struct column
        val currentStruct = col(structField)
        
        // Create a new struct with the updated nested field
        val existingFields = df.schema.fields.find(_.name == structField).get.dataType.asInstanceOf[StructType].fieldNames
        
        val updatedFields = existingFields.map { fieldName =>
          if (fieldName == nestedField) {
            expr(sqlExpr).alias(fieldName)
          } else {
            col(s"$structField.$fieldName").alias(fieldName)
          }
        }
        
        df.withColumn(structField, struct(updatedFields: _*))
      } else if (parts.length > 2) {
        // Handle deeper nesting (depth 3+)
        LOGGER.debug(s"Handling deep nested field update: path=$fieldPath")
        updateDeepNestedField(df, parts, sqlExpr)
      } else {
        // Single field path - shouldn't happen in nested context
        LOGGER.warn(s"Single field path in nested context: $fieldPath")
        df.withColumn(fieldPath, expr(sqlExpr))
      }
    } catch {
      case e: Exception =>
        LOGGER.error(s"Error updating nested field $fieldPath with SQL expression $sqlExpr", e)
        throw e
    }
  }
  
  /**
   * Update a deeply nested field in a DataFrame (depth 3+)
   */
  private def updateDeepNestedField(df: DataFrame, pathParts: Array[String], sqlExpr: String): DataFrame = {
    if (pathParts.length < 3) {
      throw new IllegalArgumentException(s"updateDeepNestedField requires at least 3 path parts, got ${pathParts.length}")
    }
    
    val topLevelField = pathParts(0)
    val remainingPath = pathParts.tail
    
    try {
      // Get the schema of the top-level field
      val topLevelSchema = df.schema.fields.find(_.name == topLevelField).get.dataType.asInstanceOf[StructType]
      
      // Build the complete path structure
      val updatedStruct = buildCompleteNestedStruct(topLevelField, remainingPath, topLevelSchema, sqlExpr)
      
      df.withColumn(topLevelField, updatedStruct)
    } catch {
      case e: Exception =>
        LOGGER.error(s"Error in updateDeepNestedField for path ${pathParts.mkString(".")} with SQL expression $sqlExpr", e)
        throw e
    }
  }
  
  /**
   * Build a complete nested struct with a field update at arbitrary depth
   */
  private def buildCompleteNestedStruct(basePath: String, remainingPath: Array[String], schema: StructType, sqlExpr: String): Column = {
    if (remainingPath.length == 1) {
      // We're at the target field - build the struct with the updated field
      val targetField = remainingPath(0)
      
      val updatedFields = schema.fieldNames.map { fieldName =>
        if (fieldName == targetField) {
          expr(sqlExpr).alias(fieldName)
        } else {
          col(s"$basePath.$fieldName").alias(fieldName)
        }
      }
      struct(updatedFields: _*)
    } else {
      // We need to go deeper - build the intermediate struct
      val currentField = remainingPath(0)
      
      val nestedSchema = schema.fields.find(_.name == currentField).get.dataType.asInstanceOf[StructType]
      
      val nestedStruct = buildCompleteNestedStruct(s"$basePath.$currentField", remainingPath.tail, nestedSchema, sqlExpr)
      
      val updatedFields = schema.fieldNames.map { fieldName =>
        if (fieldName == currentField) {
          nestedStruct.alias(fieldName)
        } else {
          col(s"$basePath.$fieldName").alias(fieldName)
        }
      }
      struct(updatedFields: _*)
    }
  }

  //TODO: Need some way to understand potential relationships between fields of different data sources (i.e. correlations, word2vec) https://spark.apache.org/docs/latest/ml-features

  /**
   * Can have logic like this:
   * 1. Using field metadata, find fields in other data sources that have similar metadata based on data profiling
   * 2. Assign a score to how similar two fields are across data sources
   * 3. Get those pairs that are greater than a threshold score
   * 4. Group all foreign keys together
   * 4.1. Unsure how to determine what is the primary source of the foreign key (the one that has the most references to it?)
   *
   * @param dataSourceForeignKeys Foreign key relationships for each data source
   * @return Map of data source fields to respective foreign key fields (which may be in other data sources)
   */
  def getAllForeignKeyRelationships(
                                     dataSourceForeignKeys: List[Dataset[ForeignKeyRelationship]],
                                     optPlanRun: Option[PlanRun],
                                     stepNameMapping: Map[String, String]
                                   ): List[ForeignKey] = {
    //given all the foreign key relations in each data source, detect if there are any links between data sources, then pass that into plan
    //the step name may be updated if it has come from a metadata source, need to update foreign key definitions as well with new step name

    val generatedForeignKeys = dataSourceForeignKeys.flatMap(_.collect())
      .groupBy(_.key)
      .map(x => ForeignKey(x._1, x._2.map(_.foreignKey), List()))
      .toList
    val userForeignKeys = optPlanRun.flatMap(planRun => planRun._plan.sinkOptions.map(_.foreignKeys))
      .getOrElse(List())
      .map(userFk => {
        val fkMapped = updateForeignKeyName(stepNameMapping, userFk.source)
        val subFkNamesMapped = userFk.generate.map(subFk => updateForeignKeyName(stepNameMapping, subFk))
        ForeignKey(fkMapped, subFkNamesMapped, List())
      })

    val mergedForeignKeys = generatedForeignKeys.map(genFk => {
      userForeignKeys.find(userFk => userFk.source == genFk.source)
        .map(matchUserFk => {
          //generated foreign key takes precedence due to constraints from underlying data source need to be adhered
          ForeignKey(matchUserFk.source, matchUserFk.generate ++ genFk.generate, List())
        })
        .getOrElse(genFk)
    })
    val allForeignKeys = mergedForeignKeys ++ userForeignKeys.filter(userFk => !generatedForeignKeys.exists(_.source == userFk.source))
    allForeignKeys
  }

  //get insert order
  def getInsertOrder(foreignKeys: List[(String, List[String])]): List[String] = {
    // Step 1: Build graph (adjacency list) & track in-degrees
    val adjList = mutable.Map[String, List[String]]().withDefaultValue(List())
    val inDegree = mutable.Map[String, Int]().withDefaultValue(0)
    val allTables = mutable.Set[String]()

    foreignKeys.foreach { case (parent, children) =>
      allTables += parent
      children.foreach { child =>
        adjList.update(parent, adjList(parent) :+ child) // Preserve child order
        inDegree.update(child, inDegree(child) + 1)
        allTables += child
      }
    }

    // Step 2: Identify root nodes (in-degree == 0)
    val queue = mutable.Queue[String]()
    allTables.foreach { table =>
      if (inDegree(table) == 0) queue.enqueue(table)
    }

    // Step 3: Topological sort with child order preserved
    val result = mutable.ListBuffer[String]()
    while (queue.nonEmpty) {
      val table = queue.dequeue()
      result += table

      // Process children in defined order
      adjList(table).foreach { child =>
        inDegree.update(child, inDegree(child) - 1)
        if (inDegree(child) == 0) queue.enqueue(child)
      }
    }

    result.toList
  }

  def getDeleteOrder(foreignKeys: List[(String, List[String])]): List[String] = {
    //given map of foreign key relationships, need to order the foreign keys by leaf nodes first, parents after
    //could be nested foreign keys
    //e.g. key1 -> key2
    //key2 -> key3
    //resulting order of deleting should be key3, key2, key1
    val fkMap = foreignKeys.toMap
    var visited = Set[String]()

    def getForeignKeyOrder(currKey: String): List[String] = {
      if (!visited.contains(currKey)) {
        visited = visited ++ Set(currKey)

        if (fkMap.contains(currKey)) {
          val children = foreignKeys.find(f => f._1 == currKey).map(_._2).getOrElse(List())
          val nested = children.flatMap(c => {
            if (!visited.contains(c)) {
              val nestedChildren = getForeignKeyOrder(c)
              visited = visited ++ Set(c)
              nestedChildren
            } else {
              List()
            }
          })
          nested ++ List(currKey)
        } else {
          List(currKey)
        }
      } else {
        List()
      }
    }

    foreignKeys.flatMap(x => getForeignKeyOrder(x._1))
  }

  private def zipWithIndex(df: DataFrame): DataFrame = {
    if (!df.storageLevel.useMemory) df.cache()
    val allColumns = df.columns ++ Array("ROW_NUMBER() OVER (ORDER BY 1) AS _join_foreign_key")
    df.selectExpr(allColumns: _*)
  }

  private def combineMetadata(sourceDf: DataFrame, sourceCols: List[String], targetDf: DataFrame, targetCols: List[String], df: DataFrame): DataFrame = {
    val sourceColsMetadata = sourceCols.map(c => {
      val baseMetadata = getMetadata(c, sourceDf.schema.fields)
      new MetadataBuilder().withMetadata(baseMetadata).remove(OMIT).build()
    })
    val targetColsMetadata = targetCols.map(c => (c, getMetadata(c, targetDf.schema.fields)))
    val newMetadata = sourceColsMetadata.zip(targetColsMetadata).map(meta => (meta._2._1, new MetadataBuilder().withMetadata(meta._2._2).withMetadata(meta._1).build()))
    //also should apply any further sql statements
    newMetadata.foldLeft(df)((metaDf, meta) => metaDf.withMetadata(meta._1, meta._2))
  }

  private def getMetadata(field: String, fields: Array[StructField]): Metadata = {
    val optMetadata = if (field.contains(".")) {
      val spt = field.split("\\.")
      val optField = fields.find(_.name == spt.head)
      optField.map(field => checkNestedForMetadata(spt, field.dataType))
    } else {
      fields.find(_.name == field).map(_.metadata)
    }
    if (optMetadata.isEmpty) {
      LOGGER.warn(s"Unable to find metadata for field, defaulting to empty metadata, field-name=$field")
      Metadata.empty
    } else optMetadata.get
  }

  @tailrec
  private def checkNestedForMetadata(spt: Array[String], dataType: DataType): Metadata = {
    dataType match {
      case StructType(nestedFields) => getMetadata(spt.tail.mkString("."), nestedFields)
      case ArrayType(elementType, _) => checkNestedForMetadata(spt, elementType)
      case _ => Metadata.empty
    }
  }
}
