package com.itechart.ny_accidents.report.generators

import com.google.inject.Inject
import com.itechart.ny_accidents.constants.GeneralConstants.{ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT_HEADER, BOROUGH_REPORT_HEADER, DAY_OF_WEEK_REPORT_HEADER, DISTRICT_REPORT_HEADER, HOUR_OF_DAY_REPORT_HEADER, PHENOMENON_REPORT_HEADER}
import com.itechart.ny_accidents.entity.MergedData
import com.itechart.ny_accidents.report.Reports
import com.itechart.ny_accidents.service.metric.{DistrictMetricService, PopulationMetricService, TimeMetricService, WeatherMetricService}
import com.itechart.ny_accidents.spark.Spark
import org.apache.spark.rdd.RDD

trait ReportGenerator{
  def calculateReport(data: RDD[MergedData]): RDD[_ <: Product]
  def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]]
}

class DayOfWeekReportGenerator extends ReportGenerator {
  override def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    TimeMetricService.countDayOfWeek(data)
  }

  override def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, DAY_OF_WEEK_REPORT_HEADER)
  }
}

class HourOfDayReportGenerator extends ReportGenerator {
  override def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    TimeMetricService.countHours(data)
  }

  override def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, HOUR_OF_DAY_REPORT_HEADER)
  }
}

class PeriodReportGenerator extends ReportGenerator {
  override def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    WeatherMetricService.definePeriod(data)
  }

  override def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, DAY_OF_WEEK_REPORT_HEADER)
  }
}

class WeatherReportGenerator extends ReportGenerator {
  override def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    WeatherMetricService.getPhenomenonPercentage(data)
  }

  override def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, PHENOMENON_REPORT_HEADER)
  }
}

class BoroughReportGenerator extends ReportGenerator {
  override def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    DistrictMetricService.getBoroughPercentage(data)
  }

  override def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, BOROUGH_REPORT_HEADER)
  }
}

class DistrictReportGenerator extends ReportGenerator {
  override def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    DistrictMetricService.getDistrictsPercentage(data)
  }

  override def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, DISTRICT_REPORT_HEADER)
  }
}

class PopulationToNumberOfAccidentsReportGenerator @Inject()(populationService: PopulationMetricService) extends ReportGenerator {
  override def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    populationService.getPopulationToNumberOfAccidentsRatio(data)
  }

  override def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, DISTRICT_REPORT_HEADER)
  }
}

class AccidentCountDuringPhenomenonPerHourReportGenerator extends ReportGenerator {
  def calculateReport(data: RDD[MergedData], weatherPhenomenon: RDD[(String, Int, Double)]): RDD[_ <: Product] = {
    WeatherMetricService.calculateAccidentCountDuringPhenomenonPerHour(data, weatherPhenomenon)
  }

  override def generateReport(data: RDD[_ <: Product], report: Reports): Seq[Seq[String]] = {
    report.generateReportForTupleRDD(data, ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT_HEADER)
  }

  override def calculateReport(data: RDD[MergedData]): RDD[_ <: Product] = {
    Spark.sc.parallelize(Seq())
  }
}