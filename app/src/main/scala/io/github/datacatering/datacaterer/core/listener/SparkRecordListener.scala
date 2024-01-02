package io.github.datacatering.datacaterer.core.listener

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

import scala.collection.mutable.ListBuffer

class SparkRecordListener(enableCount: Boolean = true) extends SparkListener {

  var outputRows: ListBuffer[SparkTaskRecordSummary] = ListBuffer()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (taskEnd.taskType.equalsIgnoreCase("resulttask") && enableCount) {
      synchronized {

        outputRows.append(SparkTaskRecordSummary(
          taskEnd.taskInfo.finishTime,
          taskEnd.taskMetrics.outputMetrics.recordsWritten,
          taskEnd.taskExecutorMetrics.getMetricValue("OnHeapExecutionMemory")
        ))
      }
    }
  }
}

case class SparkTaskRecordSummary(finishTime: Long, numRecords: Long, onHeapExecutionMemory: Long)
