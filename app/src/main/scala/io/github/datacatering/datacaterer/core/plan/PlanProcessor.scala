package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants.{DATA_CATERER_INTERFACE_JAVA, DATA_CATERER_INTERFACE_SCALA, DATA_CATERER_INTERFACE_YAML, PLAN_CLASS}
import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, Plan, Task, ValidationConfiguration}
import io.github.datacatering.datacaterer.core.activity.PlanRunPrePlanProcessor
import io.github.datacatering.datacaterer.core.config.ConfigParser
import io.github.datacatering.datacaterer.core.exception.PlanRunClassNotFoundException
import io.github.datacatering.datacaterer.core.generator.DataGeneratorProcessor
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.DataSourceMetadataFactory
import io.github.datacatering.datacaterer.core.model.Constants.METADATA_CONNECTION_OPTIONS
import io.github.datacatering.datacaterer.core.model.PlanRunResults
import io.github.datacatering.datacaterer.core.parser.PlanParser
import io.github.datacatering.datacaterer.core.util.SparkProvider
import org.apache.spark.sql.SparkSession

import scala.util.{Success, Try}

object PlanProcessor {

  def determineAndExecutePlan(
                               optPlanRun: Option[PlanRun] = None,
                               interface: String = DATA_CATERER_INTERFACE_SCALA
                             ): PlanRunResults = {
    val optPlanClass = getPlanClass
    optPlanClass.map(Class.forName)
      .map(cls => {
        cls.getDeclaredConstructor().newInstance()
        val tryScalaPlan = Try(cls.getDeclaredConstructor().newInstance().asInstanceOf[PlanRun])
        val tryJavaPlan = Try(cls.getDeclaredConstructor().newInstance().asInstanceOf[io.github.datacatering.datacaterer.javaapi.api.PlanRun])
        (tryScalaPlan, tryJavaPlan) match {
          case (Success(value), _) => (value, DATA_CATERER_INTERFACE_SCALA)
          case (_, Success(value)) => (value.getPlan, DATA_CATERER_INTERFACE_JAVA)
          case _ => throw PlanRunClassNotFoundException(optPlanClass.get)
        }
      })
      .map(p => executePlan(p._1, p._2))
      .getOrElse(
        optPlanRun.map(p => executePlan(p, interface))
          .getOrElse(executePlan(interface))
      )
  }

  def determineAndExecutePlanJava(planRun: io.github.datacatering.datacaterer.javaapi.api.PlanRun): PlanRunResults =
    determineAndExecutePlan(Some(planRun.getPlan))

  private def executePlan(planRun: PlanRun, interface: String): PlanRunResults = {
    val dataCatererConfiguration = planRun._configuration
    executePlanWithConfig(dataCatererConfiguration, Some(planRun), interface)
  }

  private def executePlan(interface: String): PlanRunResults = {
    val dataCatererConfiguration = ConfigParser.toDataCatererConfiguration
    executePlanWithConfig(dataCatererConfiguration, None, interface)
  }

  private def executePlanWithConfig(
                                     dataCatererConfiguration: DataCatererConfiguration,
                                     optPlan: Option[PlanRun],
                                     interface: String
                                   ): PlanRunResults = {
    val connectionConf = optPlan.map(_._connectionTaskBuilders.flatMap(_.connectionConfigWithTaskBuilder.options).toMap).getOrElse(Map())
    implicit val sparkSession: SparkSession = new SparkProvider(dataCatererConfiguration.master, dataCatererConfiguration.runtimeConfig ++ connectionConf).getSparkSession

    val (planRun, resolvedInterface) = if (optPlan.isDefined) {
      (optPlan.get, interface)
    } else {
      val (parsedPlan, enabledTasks, validations) = PlanParser.getPlanTasksFromYaml(dataCatererConfiguration)
      (new YamlPlanRun(parsedPlan, enabledTasks, validations, dataCatererConfiguration), DATA_CATERER_INTERFACE_YAML)
    }
    applyPrePlanProcessors(planRun, dataCatererConfiguration, resolvedInterface)

    val optPlanWithTasks = new DataSourceMetadataFactory(dataCatererConfiguration).extractAllDataSourceMetadata(planRun)
    val dataGeneratorProcessor = new DataGeneratorProcessor(dataCatererConfiguration)

    (optPlanWithTasks, planRun) match {
      case (Some((genPlan, genTasks, genValidation)), _) => dataGeneratorProcessor.generateData(genPlan, genTasks, Some(genValidation))
      case (_, plan) => dataGeneratorProcessor.generateData(plan._plan, plan._tasks, Some(plan._validations))
    }
  }

  private def getPlanClass: Option[String] = {
    val envPlanClass = System.getenv(PLAN_CLASS)
    val propPlanClass = System.getProperty(PLAN_CLASS)
    (envPlanClass, propPlanClass) match {
      case (env, _) if env != null && env.nonEmpty => Some(env)
      case (_, prop) if prop != null && prop.nonEmpty => Some(prop)
      case _ => None
    }
  }

  private def applyPrePlanProcessors(planRun: PlanRun, dataCatererConfiguration: DataCatererConfiguration, interface: String): Unit = {
    val prePlanProcessors = List(new PlanRunPrePlanProcessor(dataCatererConfiguration))

    prePlanProcessors.foreach(prePlanProcessor => {
      if (prePlanProcessor.enabled) prePlanProcessor.apply(planRun._plan.copy(interface = Some(interface)), planRun._tasks, planRun._validations)
    })
  }
}

class YamlPlanRun(
                   yamlPlan: Plan,
                   yamlTasks: List[Task],
                   validations: Option[List[ValidationConfiguration]],
                   dataCatererConfiguration: DataCatererConfiguration
                 ) extends PlanRun {
  _plan = yamlPlan
  _tasks = yamlTasks
  _validations = validations.getOrElse(List())
  //get any metadata configuration from tasks for data sources and add to configuration
  private val tasksWithMetadataOptions = yamlTasks.filter(t => t.steps.nonEmpty)
    .map(t => {
      val dataSourceName = yamlPlan.tasks.find(ts => ts.name.equalsIgnoreCase(t.name)).get.dataSourceName
      dataSourceName -> t.steps.flatMap(s => s.options.filter(o => METADATA_CONNECTION_OPTIONS.contains(o._1))).toMap
    }).toMap
  private val updatedConnectionConfig = dataCatererConfiguration.connectionConfigByName
    .map(c => c._1 -> (tasksWithMetadataOptions.getOrElse(c._1, Map()) ++ c._2))
  _configuration = dataCatererConfiguration.copy(connectionConfigByName = updatedConnectionConfig)
}
