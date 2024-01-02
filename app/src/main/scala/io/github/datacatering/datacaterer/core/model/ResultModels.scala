package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.Constants.DEFAULT_DATA_SOURCE_NAME
import io.github.datacatering.datacaterer.api.model.{Field, Step, Task}

import java.time.{Duration, LocalDateTime}

case class DataSourceResultSummary(
                                    name: String,
                                    numRecords: Long,
                                    isSuccess: Boolean,
                                    dataSourceResults: List[DataSourceResult]
                                  )

case class DataSourceResult(
                             name: String = DEFAULT_DATA_SOURCE_NAME,
                             task: Task = Task(),
                             step: Step = Step(),
                             sinkResult: SinkResult = SinkResult(),
                             batchNum: Int = 0
                           )

case class TaskResultSummary(
                              task: Task,
                              numRecords: Long,
                              isSuccess: Boolean,
                              stepResults: List[StepResultSummary]
                            )

case class StepResultSummary(
                              step: Step,
                              numRecords: Long,
                              isSuccess: Boolean,
                              dataSourceResults: List[DataSourceResult]
                            )

case class SinkResult(
                       name: String = DEFAULT_DATA_SOURCE_NAME,
                       format: String = "json",
                       saveMode: String = "append",
                       options: Map[String, String] = Map(),
                       count: Long = -1,
                       isSuccess: Boolean = true,
                       sample: Array[String] = Array(),
                       startTime: LocalDateTime = LocalDateTime.now(),
                       endTime: LocalDateTime = LocalDateTime.now(),
                       generatedMetadata: Array[Field] = Array(),
                       exception: Option[Throwable] = None
                     ) {

  def durationInSeconds: Long = Duration.between(startTime, endTime).toSeconds
}
