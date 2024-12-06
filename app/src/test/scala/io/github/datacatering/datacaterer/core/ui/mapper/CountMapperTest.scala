package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.model.Constants.{DISTRIBUTION, DISTRIBUTION_EXPONENTIAL, DISTRIBUTION_NORMAL, DISTRIBUTION_RATE_PARAMETER, MAXIMUM, MINIMUM}
import io.github.datacatering.datacaterer.core.ui.model.{DataSourceRequest, RecordCountRequest}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CountMapperTest extends AnyFunSuite {

  test("Can convert UI count configuration with base record count") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", count = Some(RecordCountRequest(Some(10))))
    val res = CountMapper.countMapping(dataSourceRequest).count
    assert(res.records.contains(10))
  }

  test("Can convert UI count configuration with base record min and max") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", count = Some(RecordCountRequest(None, Some(1), Some(10))))
    val res = CountMapper.countMapping(dataSourceRequest).count
    assert(res.generator.isDefined)
    assert(res.generator.get.options.get(MINIMUM).contains("1"))
    assert(res.generator.get.options.get(MAXIMUM).contains("10"))
  }

  test("Can convert UI count configuration with per column count") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", count = Some(RecordCountRequest(perColumnNames = Some(List("account_id")), perColumnRecords = Some(10))))
    val res = CountMapper.countMapping(dataSourceRequest).count
    assert(res.perColumn.isDefined)
    assertResult(1)(res.perColumn.get.columnNames.size)
    assert(res.perColumn.get.columnNames.contains("account_id"))
    assert(res.perColumn.get.count.contains(10))
  }

  test("Can convert UI count configuration with per column min and max") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", count = Some(RecordCountRequest(perColumnNames = Some(List("account_id")), perColumnRecordsMin = Some(10), perColumnRecordsMax = Some(20))))
    val res = CountMapper.countMapping(dataSourceRequest).count
    assert(res.perColumn.isDefined)
    assertResult(1)(res.perColumn.get.columnNames.size)
    assert(res.perColumn.get.columnNames.contains("account_id"))
    assert(res.perColumn.get.generator.isDefined)
    assert(res.perColumn.get.generator.get.options.get(MINIMUM).contains("10"))
    assert(res.perColumn.get.generator.get.options.get(MAXIMUM).contains("20"))
  }

  test("Can convert UI count configuration with per column distribution") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", count = Some(RecordCountRequest(perColumnNames = Some(List("account_id")), perColumnRecordsDistribution = Some(DISTRIBUTION_EXPONENTIAL), perColumnRecordsDistributionRateParam = Some("0.5"))))
    val res = CountMapper.countMapping(dataSourceRequest).count
    assert(res.perColumn.isDefined)
    assertResult(1)(res.perColumn.get.columnNames.size)
    assert(res.perColumn.get.columnNames.contains("account_id"))
    assert(res.perColumn.get.generator.isDefined)
    assert(res.perColumn.get.generator.get.options.get(DISTRIBUTION).contains(DISTRIBUTION_EXPONENTIAL))
    assert(res.perColumn.get.generator.get.options.get(DISTRIBUTION_RATE_PARAMETER).contains("0.5"))
  }

  test("Can convert UI count configuration with per column distribution with min and max") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", count = Some(RecordCountRequest(perColumnNames = Some(List("account_id")), perColumnRecordsDistribution = Some(DISTRIBUTION_EXPONENTIAL), perColumnRecordsDistributionRateParam = Some("0.5"), perColumnRecordsMin = Some(1), perColumnRecordsMax = Some(3))))
    val res = CountMapper.countMapping(dataSourceRequest).count
    assert(res.perColumn.isDefined)
    assertResult(1)(res.perColumn.get.columnNames.size)
    assert(res.perColumn.get.columnNames.contains("account_id"))
    assert(res.perColumn.get.generator.isDefined)
    assert(res.perColumn.get.generator.get.options.get(DISTRIBUTION).contains(DISTRIBUTION_EXPONENTIAL))
    assert(res.perColumn.get.generator.get.options.get(DISTRIBUTION_RATE_PARAMETER).contains("0.5"))
    assert(res.perColumn.get.generator.get.options.get(MINIMUM).contains("1"))
    assert(res.perColumn.get.generator.get.options.get(MAXIMUM).contains("3"))
  }

  test("Can convert UI count configuration with per column normal distribution with min and max") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", count = Some(RecordCountRequest(perColumnNames = Some(List("account_id")), perColumnRecordsDistribution = Some(DISTRIBUTION_NORMAL), perColumnRecordsMin = Some(1), perColumnRecordsMax = Some(3))))
    val res = CountMapper.countMapping(dataSourceRequest).count
    assert(res.perColumn.isDefined)
    assertResult(1)(res.perColumn.get.columnNames.size)
    assert(res.perColumn.get.columnNames.contains("account_id"))
    assert(res.perColumn.get.generator.isDefined)
    assert(res.perColumn.get.generator.get.options.get(DISTRIBUTION).contains(DISTRIBUTION_NORMAL))
    assert(res.perColumn.get.generator.get.options.get(MINIMUM).contains("1"))
    assert(res.perColumn.get.generator.get.options.get(MAXIMUM).contains("3"))
  }

}
