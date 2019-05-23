package com.itechart.ny_accidents.service.metric

import java.util.Calendar

import com.itechart.ny_accidents.constants.GeneralConstants._
import com.itechart.ny_accidents.entity.MergedData
import com.itechart.ny_accidents.service.DayPeriodService
import org.apache.spark.rdd.RDD

object DayPeriodMetricService {
  private val sunriseSchedule = DayPeriodService.sunrisesSunsets

  def getFrequency(data: RDD[MergedData]): Unit ={

    val filteredData = data
      .filter(_.accident.dateTime.isDefined)
      .map(_.accident.dateTime.get).groupBy(date => (date.getDate, date.getMonth)).map(t => (countMorningAndEveningSeconds(t._1)))
    countMorningAndEveningSeconds((6,3))
    println(filteredData.count())

  }
  private def countMorningAndEveningSeconds(date:(Int, Int)): (Long, Long) ={
    val hashDateCalendar = Calendar.getInstance()
    hashDateCalendar.set(Calendar.MONTH, date._1)
    hashDateCalendar.set(Calendar.DAY_OF_MONTH,date._2)
    hashDateCalendar.set(Calendar.HOUR_OF_DAY, 0)
    hashDateCalendar.set(Calendar.MINUTE, 0)
    hashDateCalendar.set(Calendar.SECOND, 0)
    hashDateCalendar.set(Calendar.YEAR, SUNRISE_YEAR)
    hashDateCalendar.set(Calendar.MILLISECOND, 0)
    val lightParameters = sunriseSchedule(hashDateCalendar.getTime)
    (lightParameters(SUNRISE_C).getTime - lightParameters(TWILIGHT_START_C).getTime, lightParameters(TWILIGHT_END_C).getTime - lightParameters(SUNSET_C).getTime)
  }
}
