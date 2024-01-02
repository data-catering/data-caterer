package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants.OMIT
import io.github.datacatering.datacaterer.api.model.{ForeignKeyRelation, Plan}
import ForeignKeyRelationHelper.updateForeignKeyName
import GeneratorUtil.applySqlExpressions
import io.github.datacatering.datacaterer.core.model.ForeignKeyRelationship
import io.github.datacatering.datacaterer.core.util.PlanImplicits.{ForeignKeyRelationOps, SinkOptionsOps}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, Metadata, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.annotation.tailrec
import scala.collection.mutable

object ForeignKeyUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Apply same values from source data frame columns to target foreign key columns
   *
   * @param plan                     where foreign key definitions are defined
   * @param generatedDataForeachTask map of <dataSourceName>.<stepName> => generated data as dataframe
   * @return map of <dataSourceName>.<stepName> => dataframe
   */
  def getDataFramesWithForeignKeys(plan: Plan, generatedDataForeachTask: Map[String, DataFrame]): List[(String, DataFrame)] = {
    val enabledSources = plan.tasks.filter(_.enabled).map(_.dataSourceName)
    val sinkOptions = plan.sinkOptions.get
    val foreignKeyRelations = sinkOptions.foreignKeys
      .map(fk => sinkOptions.gatherForeignKeyRelations(fk._1))
    val enabledForeignKeys = foreignKeyRelations
      .filter(fkr => isValidForeignKeyRelation(generatedDataForeachTask, enabledSources, fkr))
    var taskDfs = generatedDataForeachTask

    val foreignKeyAppliedDfs = enabledForeignKeys.flatMap(foreignKeyDetails => {
      val sourceDfName = foreignKeyDetails._1.dataFrameName
      LOGGER.debug(s"Getting source dataframe, source=$sourceDfName")
      if (!taskDfs.contains(sourceDfName)) {
        throw new RuntimeException(s"Cannot create target foreign key as one of the data sources not created. " +
          s"Please ensure there exists a data source with name (<plan dataSourceName>.<task step name>): $sourceDfName")
      }
      val sourceDf = taskDfs(sourceDfName)

      val sourceDfsWithForeignKey = foreignKeyDetails._2.map(target => {
        val targetDfName = target.dataFrameName
        LOGGER.debug(s"Getting target dataframe, source=$targetDfName")
        val targetDf = taskDfs(targetDfName)
        if (target.columns.forall(targetDf.columns.contains)) {
          val dfWithForeignKeys = applyForeignKeysToTargetDf(sourceDf, targetDf, foreignKeyDetails._1.columns, target.columns)
          if (!dfWithForeignKeys.storageLevel.useMemory) dfWithForeignKeys.cache()
          (targetDfName, dfWithForeignKeys)
        } else {
          LOGGER.warn("Foreign key data source does not contain foreign key defined in plan, defaulting to base generated data")
          (targetDfName, targetDf)
        }
      })
      taskDfs ++= sourceDfsWithForeignKey.toMap
      sourceDfsWithForeignKey
    })

    val insertOrder = getInsertOrder(foreignKeyRelations.map(f => (f._1.dataFrameName, f._2.map(_.dataFrameName))))
    val insertOrderDfs = insertOrder
      .filter(i => foreignKeyAppliedDfs.exists(f => f._1.equalsIgnoreCase(i)))
      .map(s => (s, foreignKeyAppliedDfs.filter(f => f._1.equalsIgnoreCase(s)).head._2))
    taskDfs.toList.filter(t => !insertOrderDfs.exists(_._1.equalsIgnoreCase(t._1))) ++ insertOrderDfs
  }

  private def isValidForeignKeyRelation(generatedDataForeachTask: Map[String, DataFrame], enabledSources: List[String], fkr: (ForeignKeyRelation, List[ForeignKeyRelation])) = {
    val isMainForeignKeySourceEnabled = enabledSources.contains(fkr._1.dataSource)
    val subForeignKeySources = fkr._2.map(_.dataSource)
    val isSubForeignKeySourceEnabled = subForeignKeySources.forall(enabledSources.contains)
    val disabledSubSources = subForeignKeySources.filter(s => !enabledSources.contains(s))
    val mainDfFields = generatedDataForeachTask(fkr._1.dataFrameName).schema.fields
    val columnExistsMain = fkr._1.columns.forall(c => hasDfContainColumn(c, mainDfFields))

    if (!isMainForeignKeySourceEnabled) {
      LOGGER.warn(s"Foreign key data source is not enabled. Data source needs to be enabled for foreign key relationship " +
        s"to exist from generated data, data-source-name=${fkr._1.dataSource}")
    }
    if (!isSubForeignKeySourceEnabled) {
      LOGGER.warn(s"Sub data sources within foreign key relationship are not enabled, disabled-task=${disabledSubSources.mkString(",")}")
    }
    if (!columnExistsMain) {
      LOGGER.warn(s"Main column for foreign key references is not created, data-source-name=${fkr._1.dataSource}, column=${fkr._1.columns}")
    }
    isMainForeignKeySourceEnabled && isSubForeignKeySourceEnabled && columnExistsMain
  }

  def hasDfContainColumn(column: String, fields: Array[StructField]): Boolean = {
    if (column.contains(".")) {
      val spt = column.split("\\.")
      fields.find(_.name == spt.head)
        .exists(field => checkNestedFields(spt, field.dataType))
    } else {
      fields.exists(_.name == column)
    }
  }

  @tailrec
  private def checkNestedFields(spt: Array[String], dataType: DataType): Boolean = {
    val tailColName = spt.tail
    dataType match {
      case StructType(nestedFields) =>
        hasDfContainColumn(tailColName.mkString("."), nestedFields)
      case ArrayType(elementType, _) =>
        checkNestedFields(spt, elementType)
      case _ => false
    }
  }

  private def applyForeignKeysToTargetDf(sourceDf: DataFrame, targetDf: DataFrame, sourceColumns: List[String], targetColumns: List[String]): DataFrame = {
    if (!sourceDf.storageLevel.useMemory) sourceDf.cache() //TODO do we checkpoint instead of cache? checkpoint based on total number of records?
    if (!targetDf.storageLevel.useMemory) targetDf.cache()
    val sourceColRename = sourceColumns.map(c => {
      if (c.contains(".")) {
        val lastCol = c.split("\\.").last
        (lastCol, s"_src_$lastCol")
      } else {
        (c, s"_src_$c")
      }
    }).toMap
    val distinctSourceKeys = zipWithIndex(
      sourceDf.selectExpr(sourceColumns: _*).distinct()
        .withColumnsRenamed(sourceColRename)
    )
    val distinctTargetKeys = zipWithIndex(targetDf.selectExpr(targetColumns: _*).distinct())

    LOGGER.debug(s"Attempting to join source DF keys with target DF, source=${sourceColumns.mkString(",")}, target=${targetColumns.mkString(",")}")
    val joinDf = distinctSourceKeys.join(distinctTargetKeys, Seq("_join_foreign_key"))
      .drop("_join_foreign_key")
    val targetColRename = targetColumns.zip(sourceColumns).map(c => {
      if (c._2.contains(".")) {
        val lastCol = c._2.split("\\.").last
        (c._1, col(s"_src_$lastCol"))
      } else {
        (c._1, col(s"_src_${c._2}"))
      }
    }).toMap
    val res = targetDf.join(joinDf, targetColumns)
      .withColumns(targetColRename)
      .drop(sourceColRename.values.toList: _*)

    LOGGER.debug(s"Applied source DF keys with target DF, source=${sourceColumns.mkString(",")}, target=${targetColumns.mkString(",")}")
    if (!res.storageLevel.useMemory) res.cache()
    //need to add back original metadata as it will use the metadata from the sourceDf and override the targetDf metadata
    val dfMetadata = combineMetadata(sourceDf, sourceColumns, targetDf, targetColumns, res)
    applySqlExpressions(dfMetadata, targetColumns, false)
  }

  /**
   * Consolidate all the foreign key relationships into a list of foreign keys to a list of their relationships.
   * Foreign key relationships string follows the pattern of <dataSourceName>.<stepName>.<column>
   *
   * @param dataSourceForeignKeys Foreign key relationships for each data source
   * @return Map of data source columns to respective foreign key columns (which may be in other data sources)
   */
  def getAllForeignKeyRelationships(
                                     dataSourceForeignKeys: List[Dataset[ForeignKeyRelationship]],
                                     optPlanRun: Option[PlanRun],
                                     stepNameMapping: Map[String, String]
                                   ): List[(String, List[String])] = {
    val generatedForeignKeys = dataSourceForeignKeys.flatMap(_.collect())
      .groupBy(_.key)
      .map(x => (x._1.toString, x._2.map(_.foreignKey.toString)))
      .toList
    val userForeignKeys = optPlanRun.flatMap(planRun => planRun._plan.sinkOptions.map(_.foreignKeys))
      .getOrElse(List())
      .map(userFk => {
        val fkMapped = updateForeignKeyName(stepNameMapping, userFk._1)
        val subFkNamesMapped = userFk._2.map(subFk => updateForeignKeyName(stepNameMapping, subFk))
        (fkMapped, subFkNamesMapped)
      })

    val mergedForeignKeys = generatedForeignKeys.map(genFk => {
      userForeignKeys.find(userFk => userFk._1 == genFk._1)
        .map(matchUserFk => {
          //generated foreign key takes precedence due to constraints from underlying data source need to be adhered
          (matchUserFk._1, matchUserFk._2 ++ genFk._2)
        })
        .getOrElse(genFk)
    })
    val allForeignKeys = mergedForeignKeys ++ userForeignKeys.filter(userFk => !generatedForeignKeys.exists(_._1 == userFk._1))
    allForeignKeys
  }

  //get delete order
  def getInsertOrder(foreignKeys: List[(String, List[String])]): List[String] = {
    val result = mutable.ListBuffer.empty[String]
    val visited = mutable.Set.empty[String]

    def visit(table: String): Unit = {
      if (!visited.contains(table)) {
        visited.add(table)
        foreignKeys.find(f => f._1 == table).map(_._2).getOrElse(List.empty).foreach(visit)
        result.prepend(table)
      }
    }

    foreignKeys.map(_._1).foreach(visit)
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
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(ln._1.toSeq ++ Seq(ln._2))
      ),
      StructType(
        df.schema.fields ++ Array(StructField("_join_foreign_key", LongType, false))
      )
    )
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

  private def getMetadata(column: String, fields: Array[StructField]): Metadata = {
    val optMetadata = if (column.contains(".")) {
      val spt = column.split("\\.")
      val optField = fields.find(_.name == spt.head)
      optField.map(field => checkNestedForMetadata(spt, field.dataType))
    } else {
      fields.find(_.name == column).map(_.metadata)
    }
    if (optMetadata.isEmpty) {
      LOGGER.warn(s"Unable to find metadata for column, defaulting to empty metadata, column-name=$column")
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
