package com.itechart.ny_accidents.service.metric

import java.time.DayOfWeek
import java.time.format.TextStyle
import java.util.{Calendar, Locale}

import com.itechart.ny_accidents.entity.MergedData
import com.itechart.ny_accidents.service.WeatherMappingService
import org.apache.spark.rdd.RDD

object WeatherMetric {

  // TODO rewrite to normal code
  val weatherMappingService = new WeatherMappingService

  def countHours(accidentsWithWeather: RDD[MergedData]): RDD[(Int, Int)] = {
    countTimeMetric(accidentsWithWeather, Calendar.HOUR_OF_DAY)
  }

  def countDayOfWeek(accidentsWithWeather: RDD[MergedData]): RDD[(String, Int)] = {
    countTimeMetric(accidentsWithWeather, Calendar.DAY_OF_WEEK)
      .map(d => (DayOfWeek.of(d._1).getDisplayName(TextStyle.SHORT, Locale.ENGLISH), d._2))
  }

  private def countTimeMetric(accidentsWithWeather: RDD[MergedData], calendarColumn: Int): RDD[(Int, Int)] = {
    accidentsWithWeather
      .filter(_.accident.dateTime.isDefined)
      .map(_.accident.dateTime.get)
      .map(dateTime => {
        val calendar = Calendar.getInstance()
        calendar.setTime(dateTime)
        calendar.get(calendarColumn)
      })
      .groupBy(identity)
      .mapValues(_.size)
  }

  def definePeriod(accidentsWithWeather: RDD[MergedData]): RDD[(String, Int)] = {
    accidentsWithWeather
      .filter(_.accident.dateTime.isDefined)
      .map(_.accident.dateTime.get)
      .map(weatherMappingService.defineLighting)
      .groupBy(identity)
      .mapValues(_.size)
  }

}
