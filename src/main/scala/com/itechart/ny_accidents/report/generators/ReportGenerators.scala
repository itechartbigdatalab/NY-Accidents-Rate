package com.itechart.ny_accidents.report.generators

import com.google.inject.Inject
import com.itechart.ny_accidents.constants.GeneralConstants._
import com.itechart.ny_accidents.constants.ReportsDatabase._
import com.itechart.ny_accidents.entity.MergedData
import com.itechart.ny_accidents.report.Reports
import com.itechart.ny_accidents.service.metric.{DayPeriodMetricService, DistrictMetricService, PopulationMetricService, TimeMetricService, WeatherMetricService}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame}

trait ReportGenerator{
  def apply(data: RDD[MergedData], report: Reports): Seq[Seq[String]] = {
    val calculated = calculateReport(data)
    generateReport(calculated, report)
  }

  def apply(data: RDD[MergedData], report: Reports, databaseCreationDate: Column): DataFrame = {
    val calculated = calculateReport(data)
    generateDataFrame(calculated, report, databaseCreationDate)
  }

  def tableName: String
  protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product]
  protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]]
  protected def generateDataFrame(data: RDD[_ <: Product], report: Reports, creationDate: Column): DataFrame
}

class DayOfWeekReportGenerator extends ReportGenerator {
  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    TimeMetricService.countDayOfWeek(data)
  }

  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, DAY_OF_WEEK_REPORT_HEADER)
  }

  override protected def generateDataFrame(data: RDD[_ <: Product], report: Reports, creationDate: Column): DataFrame = {
    report.generateDataFrameReportForTupleRDD(data, DAY_OF_WEEK_REPORT_SCHEMA, creationDate)
  }

  override def tableName: String = DAY_OF_WEEK_REPORT_TABLE_NAME
}

class HourOfDayReportGenerator extends ReportGenerator {
  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    TimeMetricService.countHours(data)
  }

  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, HOUR_OF_DAY_REPORT_HEADER)
  }

  override protected def generateDataFrame(data: RDD[_ <: Product], report: Reports, creationDate: Column): DataFrame = {
    report.generateDataFrameReportForTupleRDD(data, HOUR_OF_DAY_REPORT_SCHEMA, creationDate)
  }

  override def tableName: String = HOUR_OF_DAY_REPORT_TABLE_NAME
}

class PeriodReportGenerator extends ReportGenerator {
  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    WeatherMetricService.definePeriod(data)
  }

  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, DAY_OF_WEEK_REPORT_HEADER)
  }

  override protected def generateDataFrame(data: RDD[_ <: Product], report: Reports, creationDate: Column): DataFrame = {
    report.generateDataFrameReportForTupleRDD(data, DAY_OF_WEEK_REPORT_SCHEMA, creationDate)
  }

  override def tableName: String = DAY_PERIOD_REPORT_TABLE_NAME
}

class WeatherReportGenerator extends ReportGenerator {
  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    WeatherMetricService.getPhenomenonPercentage(data)
  }

  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, PHENOMENON_REPORT_HEADER)
  }

  override protected def generateDataFrame(data: RDD[_ <: Product], report: Reports, creationDate: Column): DataFrame = {
    report.generateDataFrameReportForTupleRDD(data, PHENOMENON_REPORT_SCHEMA, creationDate)
  }

  override def tableName: String = PHENOMENON_REPORT_TABLE_NAME
}

class BoroughReportGenerator extends ReportGenerator {
  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    DistrictMetricService.getBoroughPercentage(data)
  }

  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, BOROUGH_REPORT_HEADER)
  }

  override protected def generateDataFrame(data: RDD[_ <: Product], report: Reports, creationDate: Column): DataFrame = {
    report.generateDataFrameReportForTupleRDD(data, BOROUGH_REPORT_SCHEMA, creationDate)
  }

  override def tableName: String = BOROUGH_REPORT_TABLE_NAME
}

class DistrictReportGenerator extends ReportGenerator {
  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    DistrictMetricService.getDistrictsPercentage(data)
  }

  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, DISTRICT_REPORT_HEADER)
  }

  override protected def generateDataFrame(data: RDD[_ <: Product], report: Reports, creationDate: Column): DataFrame = {
    report.generateDataFrameReportForTupleRDD(data, DISTRICT_REPORT_SCHEMA, creationDate)
  }

  override def tableName: String = DISTRICT_REPORT_TABLE_NAME
}

class PopulationToNumberOfAccidentsReportGenerator @Inject()(populationService: PopulationMetricService) extends ReportGenerator {
  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    populationService.getPopulationToNumberOfAccidentsRatio(data)
  }

  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, POPULATION_TO_ACCIDENTS_REPORT_HEADER)
  }

  override protected def generateDataFrame(data: RDD[_ <: Product], report: Reports, creationDate: Column): DataFrame = {
    report.generateDataFrameReportForTupleRDD(data, POPULATION_TO_ACCIDENTS_REPORT_SCHEMA, creationDate)
  }

  override def tableName: String = POPULATION_TO_ACCIDENTS_REPORT_TABLE_NAME
}

class AccidentCountDuringPhenomenonPerHourReportGenerator extends ReportGenerator {
  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT_HEADER)
  }

  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    WeatherMetricService.calculateAccidentCountDuringPhenomenonPerHour(data)
  }

  override protected def generateDataFrame(data: RDD[_ <: Product], report: Reports, creationDate: Column): DataFrame = {
    report.generateDataFrameReportForTupleRDD(data, ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT_SCHEMA, creationDate)
  }

  override def tableName: String = ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT_TABLE_NAME
}

class FrequencyReportGenerator extends ReportGenerator {
  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    DayPeriodMetricService.getFrequency(data)
  }

  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, FREQUENCY_REPORT_HEADER)
  }

  override protected def generateDataFrame(data: RDD[_ <: Product], report: Reports, creationDate: Column): DataFrame = {
    report.generateDataFrameReportForTupleRDD(data, FREQUENCY_REPORT_SCHEMA, creationDate)
  }

  override def tableName: String = FREQUENCY_REPORT_TABLE_NAME
}
