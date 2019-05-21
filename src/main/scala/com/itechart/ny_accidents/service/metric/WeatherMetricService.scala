package com.itechart.ny_accidents.service.metric

import com.itechart.ny_accidents.entity.{ReportMergedData, WeatherForAccident}
import com.itechart.ny_accidents.service.DayPeriodService
import org.apache.spark.rdd.RDD

class WeatherMetricService extends PercentageMetricService {

  def getPhenomenonPercentage(data: RDD[ReportMergedData]): RDD[(String, Double)] = {
    val filteredData = data.filter(_.weather.isDefined).map(_.weather.get)
    val length = filteredData.count()
    val groupedData = filteredData.groupBy(_.phenomenon)

    // TODO Need rewrite
    calculatePercentage[WeatherForAccident, String](groupedData, length).map(metric => {
      metric._1.isEmpty match {
        case true => ("Clear", metric._2)
        case false => metric
      }
    })
  }

  val definePeriod: RDD[ReportMergedData] => RDD[(String, Double)] = accidentsWithWeather => {
    val filteredData = accidentsWithWeather
      .filter(_.accident.dateTime.isDefined)
      .map(_.accident.dateTime.get)
    val length = filteredData.count
    val groupedData = filteredData
      .map(DayPeriodService.defineLighting)
      .groupBy(identity)
    calculatePercentage(groupedData, length)
  }

}