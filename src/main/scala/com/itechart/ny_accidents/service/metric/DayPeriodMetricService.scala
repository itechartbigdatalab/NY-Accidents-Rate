package com.itechart.ny_accidents.service.metric

import java.util.Calendar

import com.itechart.ny_accidents.constants.GeneralConstants._
import com.itechart.ny_accidents.entity.MergedData
import com.itechart.ny_accidents.service.DayPeriodService.sunrisesSunsets
import org.apache.spark.rdd.RDD

object DayPeriodMetricService {
  private val sunriseSchedule = sunrisesSunsets

  def getFrequency(data: RDD[MergedData]): Map[String,Long] ={
    val filteredData = data.collect()
      .filter(_.accident.dateTime.isDefined)
      .map(_.accident.dateTime.get)
      .groupBy(date => (date.getDate, date.getMonth))
      .map(t => (countMorningAndEveningSeconds(t._2.head), checkAccidents(t._2.toSeq)))
    val preparedData = filteredData
      .reduce(
        {case (((leftMorningSeconds, leftEveningSeconds), (leftMorningAccidents, leftEveningAccidents)),
        ((rightMorningSeconds, rightEveningSeconds), (rightMorningAccidents, rightEveningAccidents))) =>
      ((leftMorningSeconds + rightMorningSeconds, leftEveningSeconds + rightEveningSeconds),
        (leftMorningAccidents + rightMorningAccidents, leftEveningAccidents + rightEveningAccidents ))})
    Map(("morning twilight" ,preparedData._1._1 / preparedData._2._1),("evening twilight", preparedData._1._2 / preparedData._2._2))
  }
  private def countMorningAndEveningSeconds(date:java.util.Date): (Long, Long) ={
    val hashDateCalendar = Calendar.getInstance()
    hashDateCalendar.setTime(date)
    hashDateCalendar.set(Calendar.YEAR, SUNRISE_YEAR)
    hashDateCalendar.set(Calendar.HOUR_OF_DAY, 0)
    hashDateCalendar.set(Calendar.MINUTE, 0)
    val lightParameters = sunriseSchedule(hashDateCalendar.getTime)
    (lightParameters(SUNRISE_C).getTime - lightParameters(TWILIGHT_START_C).getTime, lightParameters(TWILIGHT_END_C).getTime - lightParameters(SUNSET_C).getTime)
  }
  private def checkAccidents(seq:  Seq[java.util.Date]): (Int,Int) ={
    val hashDateCalendar = Calendar.getInstance()
    hashDateCalendar.setTime(seq.head)
    hashDateCalendar.set(Calendar.HOUR_OF_DAY, 0)
    hashDateCalendar.set(Calendar.MINUTE, 0)
    hashDateCalendar.set(Calendar.YEAR, SUNRISE_YEAR)
    val lightParameters = sunriseSchedule(hashDateCalendar.getTime)
    lightParameters.foreach(_.setYear(seq.head.getYear))
    val accidentsMorning = seq.filter(date => date.before(lightParameters(SUNRISE_C)) && date.after(lightParameters(TWILIGHT_START_C)))
    val accidentsEvening = seq.filter(date => date.before(lightParameters(TWILIGHT_END_C)) && date.after(lightParameters(SUNSET_C)))
    (accidentsMorning.size,accidentsEvening.size)
  }
}
