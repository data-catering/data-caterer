package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants.PLAN_CLASS
import io.github.datacatering.datacaterer.api.model.DataCatererConfiguration
import io.github.datacatering.datacaterer.core.config.ConfigParser
import io.github.datacatering.datacaterer.core.generator.DataGeneratorProcessor
import io.github.datacatering.datacaterer.core.model.PlanRunResults
import io.github.datacatering.datacaterer.core.util.SparkProvider
import io.github.datacatering.datacaterer.javaapi.api
import org.apache.spark.sql.SparkSession

import scala.util.{Success, Try}

object PlanProcessor {

  def determineAndExecutePlan(optPlanRun: Option[PlanRun] = None): PlanRunResults = {
    val optPlanClass = getPlanClass
    optPlanClass.map(Class.forName)
      .map(cls => {
        cls.getDeclaredConstructor().newInstance()
        val tryScalaPlan = Try(cls.getDeclaredConstructor().newInstance().asInstanceOf[PlanRun])
        val tryJavaPlan = Try(cls.getDeclaredConstructor().newInstance().asInstanceOf[api.PlanRun])
        (tryScalaPlan, tryJavaPlan) match {
          case (Success(value), _) => value
          case (_, Success(value)) => value.getPlan
          case _ => throw new RuntimeException(s"Failed to load class as either Java or Scala PlanRun, class=${optPlanClass.get}")
        }
      })
      .map(executePlan)
      .getOrElse(
        optPlanRun.map(executePlan)
          .getOrElse(executePlan)
      )
  }

  def determineAndExecutePlanJava(planRun: api.PlanRun): PlanRunResults =
    determineAndExecutePlan(Some(planRun.getPlan))

  private def executePlan(planRun: PlanRun): PlanRunResults = {
    val dataCatererConfiguration = planRun._configuration
    executePlanWithConfig(dataCatererConfiguration, Some(planRun))
  }

  private def executePlan: PlanRunResults = {
    val dataCatererConfiguration = ConfigParser.toDataCatererConfiguration
    executePlanWithConfig(dataCatererConfiguration, None)
  }

  private def executePlanWithConfig(dataCatererConfiguration: DataCatererConfiguration, optPlan: Option[PlanRun]): PlanRunResults = {
    implicit val sparkSession: SparkSession = new SparkProvider(dataCatererConfiguration.master, dataCatererConfiguration.runtimeConfig).getSparkSession

    val dataGeneratorProcessor = new DataGeneratorProcessor(dataCatererConfiguration)
    optPlan match {
      case Some(plan) => dataGeneratorProcessor.generateData(plan._plan, plan._tasks, Some(plan._validations))
      case _ => dataGeneratorProcessor.generateData()
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
}
