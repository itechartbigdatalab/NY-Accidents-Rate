package com.itechart.ny_accidents.parse

import com.google.inject.Singleton
import com.itechart.ny_accidents.constants.GeneralConstants
import com.itechart.ny_accidents.entity.WeatherEntity
import com.itechart.ny_accidents.utils.DateUtils

import scala.io.Source
import scala.util.control.Exception

@Singleton
class WeatherParser {

  // TODO rewrite denamic update
  def parseCsv(path: String): Seq[WeatherEntity] = {
    val bufferedSource = Source.fromFile(path)
    bufferedSource.getLines()
      //      .map(_.split(",").toList)
      .map(_.split(";").toList)
      .toList
      .map(_.map(_.replaceAll("\"", "")).toList)
      .map(parseWeatherLine)
      .filter(_.isDefined)
      .map(_.get)
  }

  def parseCsv(paths: Array[String]): Seq[WeatherEntity] = {
    paths.map(parseCsv).reduce(_ ++ _)
  }

  // TODO rewrite to denamic update
  def parseWeatherLine(columns: List[String]): Option[WeatherEntity] = {
    Exception.allCatch.opt(WeatherEntity(
      0,
      1,
      DateUtils.parseDateToMillis(columns.head, GeneralConstants.DATE_TIME_WEATHER_PATTERN).get,
      columns(1).toDouble,
      columns(3).toDouble,
      columns(4).toDouble,
      columns(6).toDouble,
      columns(8),
      columns(11).toDouble
    ))
  }

}
