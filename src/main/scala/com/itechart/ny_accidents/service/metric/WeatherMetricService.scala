package com.itechart.ny_accidents.service.metric

import com.itechart.ny_accidents.constants.GeneralConstants
import com.itechart.ny_accidents.constants.Injector.injector
import com.itechart.ny_accidents.entity.{MergedData, WeatherEntity, WeatherForAccident}
import com.itechart.ny_accidents.service.{DayPeriodService, WeatherMappingService}
import com.itechart.ny_accidents.utils.NumberUtils
import org.apache.spark.rdd.RDD

object WeatherMetricService extends PercentageMetricService {
  private lazy val weatherMappingService = injector.getInstance(classOf[WeatherMappingService])

  def getPhenomenonPercentage(data: RDD[MergedData]): RDD[(String, Int, Double)] = {
    val filteredData = data.filter(_.weather.isDefined).map(_.weather.get)
    val length = filteredData.count()
    val groupedData = filteredData.groupBy(_.phenomenon)

    // TODO Need rewrite
    calculatePercentage[WeatherForAccident, String](groupedData, length).map(metric => {
      metric._1.isEmpty match {
        case true => ("Clear", metric._2, metric._3)
        case false => metric
      }
    })
  }

  val definePeriod: RDD[MergedData] => RDD[(String, Int, Double)] = accidentsWithWeather => {
    val filteredData = accidentsWithWeather
      .filter(_.accident.localDateTime.isDefined)
      .map(_.accident.localDateTime.get)
    val length = filteredData.count
    val groupedData = filteredData
      .map(DayPeriodService.defineLighting)
      .groupBy(identity)
    calculatePercentage(groupedData, length)
  }

  def calculateAccidentCountDuringPhenomenonPerHour(mergedAccidents: RDD[MergedData]): RDD[(String, Int, Double, Double)] = {
    val filteredData = mergedAccidents
      .filter(_.accident.dateTimeMillis.isDefined)
      .sortBy(_.accident.dateTimeMillis.get)

    val firstAccidentDateTime = filteredData.first().accident.dateTimeMillis.get
    // how to do it in another way????
    val lastAccidentDateTime = filteredData.take(filteredData.count().toInt).last.accident.dateTimeMillis.get

    val weatherByStation: Map[Int, Seq[WeatherEntity]] = weatherMappingService
      .getWeatherByStationsBetweenDates(firstAccidentDateTime, lastAccidentDateTime)
    val stationCount = weatherByStation.size

    val phenomenonCount = WeatherMetricService.getPhenomenonPercentage(mergedAccidents)
    phenomenonCount.map { case (phenomenonName, accidentCountDuringPhenomenon, _) =>
      val hoursOfPhenomenon = countHoursOfPhenomenon(weatherByStation, stationCount, phenomenonName)
      (phenomenonName, accidentCountDuringPhenomenon, NumberUtils.truncateDouble(hoursOfPhenomenon), NumberUtils.truncateDouble(accidentCountDuringPhenomenon / hoursOfPhenomenon))
    }
  }

  def countHoursOfPhenomenon(weatherByStation: Map[Int, Seq[WeatherEntity]], stationCount: Int, phenomenonName: String): Double = {
    weatherByStation
      .values
      .map(seqOfWeatherByStation => {
        seqOfWeatherByStation
          .sliding(2)
          .flatMap(twoWeatherEntities => {
            val first = twoWeatherEntities.head
            val last = twoWeatherEntities.last
            if (first.phenomenon == last.phenomenon) {
              Seq((first.phenomenon, last.dateTime - first.dateTime))
            } else {
              val halfTime = (last.dateTime - first.dateTime) / 2
              Seq((first.phenomenon, halfTime), (last.phenomenon, halfTime))
            }
          })
          .toSeq
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)
      })
      .reduce(combineTwoMaps)
      .mapValues(_.toDouble / GeneralConstants.MILLIS_IN_HOUR / stationCount.toDouble)(phenomenonName)
  }

  // May move to utils, but we already have a lot of UTILS objects
  def combineTwoMaps(x: Map[String, Long], y: Map[String, Long]): Map[String, Long] = {
    val x0 = x.withDefaultValue(0L)
    val y0 = y.withDefaultValue(0L)
    val keys = x.keys.toSet.union(y.keys.toSet)
    keys.map(k => k -> (x0(k) + y0(k))).toMap
  }

}