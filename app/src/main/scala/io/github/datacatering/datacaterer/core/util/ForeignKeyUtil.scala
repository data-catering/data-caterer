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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, Metadata, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.annotation.tailrec
import scala.collection.mutable

object ForeignKeyUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Apply same values from source data frame fields to target foreign key fields
   *
   * @param plan                     where foreign key definitions are defined
   * @param generatedDataForeachTask map of <dataSourceName>.<stepName> => generated data as dataframe
   * @return map of <dataSourceName>.<stepName> => dataframe
   */
  def getDataFramesWithForeignKeys(plan: Plan, generatedDataForeachTask: Map[String, DataFrame]): List[(String, DataFrame)] = {
    val enabledSources = plan.tasks.filter(_.enabled).map(_.dataSourceName)
    val sinkOptions = plan.sinkOptions.get
    val foreignKeyRelations = sinkOptions.foreignKeys
      .map(fk => sinkOptions.gatherForeignKeyRelations(fk.source))
    val enabledForeignKeys = foreignKeyRelations
      .filter(fkr => isValidForeignKeyRelation(generatedDataForeachTask, enabledSources, fkr))
    var taskDfs = generatedDataForeachTask

    val foreignKeyAppliedDfs = enabledForeignKeys.flatMap(foreignKeyDetails => {
      val sourceDfName = foreignKeyDetails.source.dataFrameName
      LOGGER.debug(s"Getting source dataframe, source=$sourceDfName")
      if (!taskDfs.contains(sourceDfName)) {
        throw MissingDataSourceFromForeignKeyException(sourceDfName)
      }
      val sourceDf = taskDfs(sourceDfName)

      val sourceDfsWithForeignKey = foreignKeyDetails.generationLinks.map(target => {
        val targetDfName = target.dataFrameName
        LOGGER.debug(s"Getting target dataframe, source=$targetDfName")
        val targetDf = taskDfs(targetDfName)
        if (target.fields.forall(targetDf.columns.contains)) {
          LOGGER.info(s"Applying foreign key values to target data source, source-data=${foreignKeyDetails.source.dataSource}, target-data=${target.dataSource}")
          val dfWithForeignKeys = applyForeignKeysToTargetDf(sourceDf, targetDf, foreignKeyDetails.source.fields, target.fields)
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

    val insertOrder = getInsertOrder(foreignKeyRelations.map(f => (f.source.dataFrameName, f.generationLinks.map(_.dataFrameName))))
    val insertOrderDfs = insertOrder
      .filter(i => foreignKeyAppliedDfs.exists(f => f._1.equalsIgnoreCase(i)))
      .map(s => (s, foreignKeyAppliedDfs.filter(f => f._1.equalsIgnoreCase(s)).head._2))
    taskDfs.toList.filter(t => !insertOrderDfs.exists(_._1.equalsIgnoreCase(t._1))) ++ insertOrderDfs
  }

  private def isValidForeignKeyRelation(generatedDataForeachTask: Map[String, DataFrame], enabledSources: List[String],
                                        fkr: ForeignKeyWithGenerateAndDelete) = {
    val isMainForeignKeySourceEnabled = enabledSources.contains(fkr.source.dataSource)
    val subForeignKeySources = fkr.generationLinks.map(_.dataSource)
    val isSubForeignKeySourceEnabled = subForeignKeySources.forall(enabledSources.contains)
    val disabledSubSources = subForeignKeySources.filter(s => !enabledSources.contains(s))
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
    val sourceColRename = sourceFields.map(c => {
      if (c.contains(".")) {
        val lastCol = c.split("\\.").last
        (lastCol, s"_src_$lastCol")
      } else {
        (c, s"_src_$c")
      }
    }).toMap
    val distinctSourceKeys = zipWithIndex(
      sourceDf.selectExpr(sourceFields: _*).distinct()
        .withColumnsRenamed(sourceColRename)
    )
    val distinctTargetKeys = zipWithIndex(targetDf.selectExpr(targetFields: _*).distinct())

    LOGGER.debug(s"Attempting to join source DF keys with target DF, source=${sourceFields.mkString(",")}, target=${targetFields.mkString(",")}")
    val joinDf = distinctSourceKeys.join(distinctTargetKeys, Seq("_join_foreign_key"))
      .drop("_join_foreign_key")
    val targetColRename = targetFields.zip(sourceFields).map(c => {
      if (c._2.contains(".")) {
        val lastCol = c._2.split("\\.").last
        (c._1, col(s"_src_$lastCol"))
      } else {
        (c._1, col(s"_src_${c._2}"))
      }
    }).toMap
    val res = targetDf.join(joinDf, targetFields)
      .withColumns(targetColRename)
      .drop(sourceColRename.values.toList: _*)

    LOGGER.debug(s"Applied source DF keys with target DF, source=${sourceFields.mkString(",")}, target=${targetFields.mkString(",")}")
    if (!res.storageLevel.useMemory) res.cache()
    //need to add back original metadata as it will use the metadata from the sourceDf and override the targetDf metadata
    val dfMetadata = combineMetadata(sourceDf, sourceFields, targetDf, targetFields, res)
    applySqlExpressions(dfMetadata, targetFields, false)
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
