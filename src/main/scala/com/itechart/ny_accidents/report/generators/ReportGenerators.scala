package com.itechart.ny_accidents.report.generators

import com.google.inject.Inject
import com.itechart.ny_accidents.constants.GeneralConstants._
import com.itechart.ny_accidents.entity.MergedData
import com.itechart.ny_accidents.report.Reports
import com.itechart.ny_accidents.service.metric.{DistrictMetricService, PopulationMetricService, TimeMetricService, WeatherMetricService}
import org.apache.spark.rdd.RDD

trait ReportGenerator{
  def apply(data: RDD[MergedData], report: Reports): Seq[Seq[String]] = {
    val calculated = calculateReport(data)
    generateReport(calculated, report)
  }

  protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product]
  protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]]
}

class DayOfWeekReportGenerator extends ReportGenerator {
  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    TimeMetricService.countDayOfWeek(data)
  }

  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, DAY_OF_WEEK_REPORT_HEADER)
  }
}

class HourOfDayReportGenerator extends ReportGenerator {
  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    TimeMetricService.countHours(data)
  }

  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, HOUR_OF_DAY_REPORT_HEADER)
  }
}

class PeriodReportGenerator extends ReportGenerator {
  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    WeatherMetricService.definePeriod(data)
  }

  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, DAY_OF_WEEK_REPORT_HEADER)
  }
}

class WeatherReportGenerator extends ReportGenerator {
  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    WeatherMetricService.getPhenomenonPercentage(data)
  }

  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, PHENOMENON_REPORT_HEADER)
  }
}

class BoroughReportGenerator extends ReportGenerator {
  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    DistrictMetricService.getBoroughPercentage(data)
  }

  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, BOROUGH_REPORT_HEADER)
  }
}

class DistrictReportGenerator extends ReportGenerator {
  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    DistrictMetricService.getDistrictsPercentage(data)
  }

  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, DISTRICT_REPORT_HEADER)
  }
}

class PopulationToNumberOfAccidentsReportGenerator @Inject()(populationService: PopulationMetricService) extends ReportGenerator {
  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    populationService.getPopulationToNumberOfAccidentsRatio(data)
  }

  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, DISTRICT_REPORT_HEADER)
  }
}

class AccidentCountDuringPhenomenonPerHourReportGenerator extends ReportGenerator {
  override protected def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT_HEADER)
  }

  override protected def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    WeatherMetricService.calculateAccidentCountDuringPhenomenonPerHour(data)
  }
}