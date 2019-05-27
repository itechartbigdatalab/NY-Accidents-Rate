package com.itechart.ny_accidents.report

import com.google.inject.Inject
import com.itechart.ny_accidents.Application.logger
import com.itechart.ny_accidents.constants.GeneralConstants._
import com.itechart.ny_accidents.constants.Injector
import com.itechart.ny_accidents.entity.MergedData
import com.itechart.ny_accidents.service.metric.{DistrictMetricService, PopulationMetricService, TimeMetricService, WeatherMetricService}
import org.apache.spark.rdd.RDD

class ReportsGenerator @Inject()(populationService: PopulationMetricService) {
  def calculateMetrics(data: RDD[MergedData]):
  (RDD[(String, Int, Double)],
    RDD[(Int, Int, Double)],
    RDD[(String, Int, Double)],
    RDD[(String, Int, Double)],
    RDD[(String, Int, Double)],
    RDD[(String, Int, Double)],
    RDD[(String, Double, Double, Int)],
    RDD[(String, Int, Double, Double)]) = {

    val dayOfWeek = TimeMetricService.countDayOfWeek(data)
    logger.info("Day of week calculated")
    val hourOfDay = TimeMetricService.countHours(data)
    logger.info("Hour of day calculated")
    val period = WeatherMetricService.definePeriod(data)
    logger.info("Period calculated")
    val weatherPhenomenon = WeatherMetricService.getPhenomenonPercentage(data)
    logger.info("Weather phenomenon calculated")
    val boroughPercentage = DistrictMetricService.getBoroughPercentage(data)
    logger.info("Borough percentage calculated")
    val districtsPercentage = DistrictMetricService.getDistrictsPercentage(data)
    logger.info("Districts percentage calculated")
    val populationToNumberOfAccidents = populationService
      .getPopulationToNumberOfAccidentsRatio(data)
    logger.info("Population to number of accidents calculated")
    val accidentCountDuringPhenomenonPerHour =
      WeatherMetricService.calculateAccidentCountDuringPhenomenonPerHour(data, weatherPhenomenon)
    logger.info("Accidents count per hour for each phenomenon metric calculated")

    (
      dayOfWeek,
      hourOfDay,
      period,
      weatherPhenomenon,
      boroughPercentage,
      districtsPercentage,
      populationToNumberOfAccidents,
      accidentCountDuringPhenomenonPerHour
    )
  }

  def generateStringReports(dayOfWeekData: RDD[(String, Int, Double)],
                            hourOfDayData: RDD[(Int, Int, Double)],
                            periodData: RDD[(String, Int, Double)],
                            weatherPhenomenonData: RDD[(String, Int, Double)],
                            boroughPercentageData: RDD[(String, Int, Double)],
                            districtsPercentageData: RDD[(String, Int, Double)],
                            populationToNumberOfAccidentsData: RDD[(String, Double, Double, Int)],
                            accidentCountDuringPhenomenonPerHourData: RDD[(String, Int, Double, Double)]):
  Seq[(String, Seq[Seq[String]])] = {

    val report = Injector.injector.getInstance(classOf[Reports])
    val dayOfWeekReport = ("day_of_week.csv", report.generateReportForTupleRDD(dayOfWeekData, DAY_OF_WEEK_REPORT_HEADER))
    val hourOfDayReport = ("hour_of_day.csv", report.generateReportForTupleRDD(hourOfDayData, HOUR_OF_DAY_REPORT_HEADER))
    val periodReport = ("period.csv", report.generateReportForTupleRDD(periodData, DAY_OF_WEEK_REPORT_HEADER))
    val weatherReport = ("weather.csv", report.generateReportForTupleRDD(weatherPhenomenonData, PHENOMENON_REPORT_HEADER))
    val boroughReport = ("borough.csv", report.generateReportForTupleRDD(boroughPercentageData, BOROUGH_REPORT_HEADER))
    val districtsReport = ("districts.csv", report.generateReportForTupleRDD(districtsPercentageData, DISTRICT_REPORT_HEADER))
    val populationToNumberOfAccidentsReport = ("population_to_accidents.csv", report.
      generateReportForTupleRDD(populationToNumberOfAccidentsData, POPULATION_TO_ACCIDENTS_REPORT_HEADER))
    val accidentCountDuringPhenomenonPerHourReport = ("accidents_by_phenomenon.csv", report.
      generateReportForTupleRDD(accidentCountDuringPhenomenonPerHourData, ACCIDENTS_DURING_PHENOMENON_COUNT_REPORT_HEADER))

    Seq(
      dayOfWeekReport,
      hourOfDayReport,
      periodReport,
      weatherReport,
      boroughReport,
      districtsReport,
      populationToNumberOfAccidentsReport,
      accidentCountDuringPhenomenonPerHourReport
    )
  }
}
