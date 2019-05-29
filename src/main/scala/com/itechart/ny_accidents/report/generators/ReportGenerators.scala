package com.itechart.ny_accidents.report.generators

import com.google.inject.Inject
import com.itechart.ny_accidents.constants.GeneralConstants._
import com.itechart.ny_accidents.constants.ReportsDatabase._
import com.itechart.ny_accidents.entity.MergedData
import com.itechart.ny_accidents.report.Reports
import com.itechart.ny_accidents.service.metric.{DistrictMetricService, PopulationMetricService, TimeMetricService, WeatherMetricService}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.StructType

trait ReportGenerator{
  def apply(data: RDD[MergedData], report: Reports): Seq[Seq[String]] = {
    val calculated = calculateReport(data)
    generateReport(calculated, report)
  }

  def apply(data: RDD[MergedData], report: Reports, databaseCreationDate: Column): DataFrame = {
    val calculated = calculateReport(data)
    generateDataFrame(calculated, report, databaseCreationDate)
  }

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
}