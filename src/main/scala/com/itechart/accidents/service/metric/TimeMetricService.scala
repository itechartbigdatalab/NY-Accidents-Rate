package com.itechart.accidents.service.metric

import java.time.format.TextStyle
import java.time.{DayOfWeek, LocalDateTime}
import java.util.Locale

import com.itechart.accidents.entity.MergedData
import org.apache.spark.rdd.RDD


object TimeMetricService extends PercentageMetricService {

  def countHours(accidentsWithWeather: RDD[MergedData]): RDD[(Int, Int, Double)] = {
    val filteredData = accidentsWithWeather
      .filter(_.accident.localDateTime.isDefined)
      .map(_.accident.localDateTime.get)
    val length = filteredData.count
    val groupedData = filteredData
      .map(_.getHour)
      .groupBy(identity)
    calculatePercentage(groupedData, length)
  }

  def countDayOfWeek(accidentsWithWeather: RDD[MergedData]): RDD[(String, Int, Double)] = {
    val filteredData: RDD[LocalDateTime] = accidentsWithWeather
      .filter(_.accident.localDateTime.isDefined)
      .map(_.accident.localDateTime.get)
    val length = filteredData.count
    val groupedData = filteredData
      .map(_.getDayOfWeek.getValue)
      .groupBy(identity)
      .sortBy(_._1)
      .map(d => (d._1 + ") " + DayOfWeek.of(d._1).getDisplayName(TextStyle.SHORT, Locale.ENGLISH), d._2))
    calculatePercentage(groupedData, length)
  }


}
