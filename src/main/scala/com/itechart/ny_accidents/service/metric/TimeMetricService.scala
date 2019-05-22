package com.itechart.ny_accidents.service.metric

import java.time.DayOfWeek
import java.time.format.TextStyle
import java.util.{Calendar, Locale}

import com.itechart.ny_accidents.entity.{MergedData, ReportMergedData}
import org.apache.spark.rdd.RDD


object TimeMetricService extends PercentageMetricService {

  def countHours(accidentsWithWeather: RDD[MergedData]): RDD[(Int, Double)] = {
    val filteredData = accidentsWithWeather
      .filter(_.accident.dateTime.isDefined)
      .map(_.accident.dateTime.get)
    val length = filteredData.count
    val groupedData = filteredData
      .map(dateTime => {
        val calendar = Calendar.getInstance()
        calendar.setTime(dateTime)
        calendar.get(Calendar.HOUR_OF_DAY)
      })
      .groupBy(identity)
    calculatePercentage(groupedData, length)
  }

  def countDayOfWeek(accidentsWithWeather: RDD[MergedData]): RDD[(String, Double)] = {
    val filteredData = accidentsWithWeather
      .filter(_.accident.dateTime.isDefined)
      .map(_.accident.dateTime.get)
    val length = filteredData.count
    val groupedData = filteredData
      .map(dateTime => {
        val calendar = Calendar.getInstance()
        calendar.setTime(dateTime)
        calendar.get(Calendar.DAY_OF_WEEK)
      })
      .groupBy(identity)
      .map(d => (DayOfWeek.of(d._1).getDisplayName(TextStyle.SHORT, Locale.ENGLISH), d._2))
    calculatePercentage(groupedData, length)
  }



}
