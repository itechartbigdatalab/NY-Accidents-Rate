package com.itechart.accidents.integration.ny.parse

import com.google.inject.Singleton
import com.itechart.accidents.constants.GeneralConstants
import com.itechart.accidents.entity.WeatherEntity
import com.itechart.accidents.utils.DateUtils

import scala.io.Source
import scala.util.control.Exception

@Singleton
class NYWeatherParser {
  private lazy val CLEAR_PHENOMENON = "Clear"

  // TODO rewrite dynamic update
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
      .map(weather => if(weather.phenomenon.isEmpty) weather.copy(phenomenon = CLEAR_PHENOMENON) else weather)
  }

  def parseCsv(paths: Array[String]): Seq[WeatherEntity] = {
    paths.map(parseCsv).reduce(_ ++ _)
  }

  // TODO rewrite to dynamic update
  def parseWeatherLine(columns: List[String]): Option[WeatherEntity] = {
    Exception.allCatch.opt(WeatherEntity(
      0,
      1,
      DateUtils.parseDateToMillis(columns.head, GeneralConstants.DATE_TIME_FORMATTER_WEATHER).get,
      columns(1).toDouble,
      columns(3).toDouble,
      columns(4).toDouble,
      columns(6).toDouble,
      columns(8),
      columns(11).toDouble
    ))
  }

}
