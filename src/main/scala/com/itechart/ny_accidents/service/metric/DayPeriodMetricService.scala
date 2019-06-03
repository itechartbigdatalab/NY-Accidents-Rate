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
    lazy val nightSeconds = filteredData.map(_._2._1._1).sum
    lazy val morningTwilightSeconds = filteredData.map(_._2._1._2).sum
    lazy val daySeconds = filteredData.map(_._2._1._3).sum
    lazy val eveningTwilightSeconds = filteredData.map(_._2._1._4).sum
    lazy val nightAccidents = filteredData.map(_._2._2._1).sum
    lazy val morningTwilightAccidents = filteredData.map(_._2._2._2).sum
    lazy val dayAccidents = filteredData.map(_._2._2._3).sum
    lazy val eveningTwilightAccidents = filteredData.map(_._2._2._4).sum
    Spark.sc.parallelize(Seq(("night" , nightAccidents * 3600 / nightSeconds ),
      ("morning twilight" , morningTwilightAccidents * 3600 / morningTwilightSeconds ),
      ("day" , dayAccidents * 3600 / daySeconds ),
      ("evening twilight",eveningTwilightAccidents * 3600 / eveningTwilightSeconds )))
  }
  private def countMorningAndEveningSeconds(date:LocalDateTime): (Long, Long, Long, Long) ={
    val hashDateTime = date.truncatedTo(ChronoUnit.HOURS).withYear(SUNRISE_YEAR)
    val lightParameters = sunriseSchedule(hashDateTime.toLocalDate)
    (Duration.between(hashDateTime,lightParameters(TWILIGHT_START_C)).getSeconds
      + Duration.between(lightParameters(TWILIGHT_END_C),hashDateTime.plusDays(1)).getSeconds,
      Duration.between(lightParameters(TWILIGHT_START_C),lightParameters(SUNRISE_C)).getSeconds,
      Duration.between(lightParameters(SUNRISE_C),lightParameters(SUNSET_C)).getSeconds,
      Duration.between(lightParameters(SUNSET_C),lightParameters(TWILIGHT_END_C)).getSeconds
    )
  }
  private def checkAccidents(seq:  Seq[LocalDateTime]): (Int,Int, Int,Int) ={
    val hashDateTime = seq.head.truncatedTo(ChronoUnit.HOURS).withYear(SUNRISE_YEAR)
    val lightParameters = sunriseSchedule(hashDateTime.toLocalDate)
    lightParameters.foreach(_.withYear(seq.head.getYear))
    val accidentsNight = seq.filter(date => date.isBefore(lightParameters(TWILIGHT_START_C)) || date.isAfter(lightParameters(TWILIGHT_END_C)))
    val accidentsMorning = seq.filter(date => date.isBefore(lightParameters(SUNRISE_C)) && date.isAfter(lightParameters(TWILIGHT_START_C)))
    val accidentsDay = seq.filter(date => date.isBefore(lightParameters(SUNSET_C)) && date.isAfter(lightParameters(SUNRISE_C)))
    val accidentsEvening = seq.filter(date => date.isBefore(lightParameters(TWILIGHT_END_C)) && date.isAfter(lightParameters(SUNSET_C)))
    (accidentsNight.size, accidentsMorning.size,accidentsDay.size, accidentsEvening.size)
  }
}