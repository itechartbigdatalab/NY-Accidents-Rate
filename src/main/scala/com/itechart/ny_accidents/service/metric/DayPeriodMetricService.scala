package com.itechart.ny_accidents.service.metric

import java.time.{Duration, LocalDateTime}
import java.time.temporal.ChronoUnit

import com.itechart.ny_accidents.constants.GeneralConstants._
import com.itechart.ny_accidents.entity.MergedData
import com.itechart.ny_accidents.service.DayPeriodService.sunrisesSunsets
import com.itechart.ny_accidents.spark.Spark
import org.apache.spark.rdd.RDD

object DayPeriodMetricService  {
  private val sunriseSchedule = sunrisesSunsets

  def getFrequency(data: RDD[MergedData]):  RDD[(String, Long)] ={
    val filteredData = data.collect()
      .filter(_.accident.localDateTime.isDefined)
      .map(_.accident.localDateTime.get)
      .groupBy(date => (date.getDayOfMonth, date.getMonth.getValue))
      .map {
        case (key, value) => (key, (countMorningAndEveningSeconds(value.head), checkAccidents(value.toSeq)))
      }
    lazy val morningSeconds = filteredData.map(_._2._1._1).sum
    lazy val morningAccidents = filteredData.map(_._2._2._1).sum
    lazy val eveningSeconds = filteredData.map(_._2._1._2).sum
    lazy val eveningAccidents = filteredData.map(_._2._2._2).sum
    Spark.sc.parallelize(Seq(("morning twilight" ,morningSeconds / morningAccidents),("evening twilight", eveningSeconds / eveningAccidents)))
  }
  private def countMorningAndEveningSeconds(date:LocalDateTime): (Long, Long) ={
    val hashDateTime = date.truncatedTo(ChronoUnit.HOURS).withYear(SUNRISE_YEAR)
    val lightParameters = sunriseSchedule(hashDateTime.toLocalDate)
    (Duration.between(lightParameters(TWILIGHT_START_C),lightParameters(SUNRISE_C)).getSeconds,
      Duration.between(lightParameters(SUNSET_C),lightParameters(TWILIGHT_END_C)).getSeconds)
  }
  private def checkAccidents(seq:  Seq[LocalDateTime]): (Int,Int) ={
    val hashDateTime = seq.head.truncatedTo(ChronoUnit.HOURS).withYear(SUNRISE_YEAR)
    val lightParameters = sunriseSchedule(hashDateTime.toLocalDate)
    lightParameters.foreach(_.withYear(seq.head.getYear))
    val accidentsMorning = seq.filter(date => date.isBefore(lightParameters(SUNRISE_C)) && date.isAfter(lightParameters(TWILIGHT_START_C)))
    val accidentsEvening = seq.filter(date => date.isBefore(lightParameters(TWILIGHT_END_C)) && date.isAfter(lightParameters(SUNSET_C)))
    (accidentsMorning.size,accidentsEvening.size)
  }
}
