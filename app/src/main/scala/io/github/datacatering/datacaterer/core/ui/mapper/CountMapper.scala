package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.{CountBuilder, GeneratorBuilder}
import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_COUNT_RECORDS, DEFAULT_PER_COLUMN_COUNT_RECORDS, DISTRIBUTION_EXPONENTIAL, DISTRIBUTION_NORMAL}
import io.github.datacatering.datacaterer.core.ui.model.DataSourceRequest

object CountMapper {

  def countMapping(dataSourceRequest: DataSourceRequest): CountBuilder = {
    dataSourceRequest.count.map(recordCountRequest => {
      val baseRecordCount = (recordCountRequest.records, recordCountRequest.recordsMin, recordCountRequest.recordsMax) match {
        case (Some(records), None, None) => CountBuilder().records(records)
        case (None, Some(min), Some(max)) => CountBuilder().generator(GeneratorBuilder().min(min).max(max))
        case _ => CountBuilder().records(DEFAULT_COUNT_RECORDS)
      }

      val perColumnNames = recordCountRequest.perColumnNames.getOrElse(List())
      if (perColumnNames.nonEmpty) {
        (recordCountRequest.perColumnRecords, recordCountRequest.perColumnRecordsMin, recordCountRequest.perColumnRecordsMax,
          recordCountRequest.perColumnRecordsDistribution, recordCountRequest.perColumnRecordsDistributionRateParam) match {
          case (Some(records), None, None, None, None) => baseRecordCount.recordsPerColumn(records, perColumnNames: _*)
          case (None, Some(min), Some(max), None, None) => baseRecordCount.recordsPerColumnGenerator(GeneratorBuilder().min(min).max(max), perColumnNames: _*)
          case (None, Some(min), Some(max), Some(DISTRIBUTION_EXPONENTIAL), Some(rate)) => baseRecordCount.recordsPerColumnExponentialDistribution(min, max, rate.toDouble, perColumnNames: _*)
          case (None, None, None, Some(DISTRIBUTION_EXPONENTIAL), Some(rate)) => baseRecordCount.recordsPerColumnExponentialDistribution(rate.toDouble, perColumnNames: _*)
          case (None, Some(min), Some(max), Some(DISTRIBUTION_NORMAL), None) => baseRecordCount.recordsPerColumnNormalDistribution(min, max, perColumnNames: _*)
          case _ => baseRecordCount.recordsPerColumn(DEFAULT_PER_COLUMN_COUNT_RECORDS, perColumnNames: _*)
        }
      } else {
        baseRecordCount
      }
    }).getOrElse(CountBuilder())
  }

}
