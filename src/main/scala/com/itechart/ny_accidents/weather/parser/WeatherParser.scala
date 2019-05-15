package com.itechart.ny_accidents.weather.parser

import com.itechart.ny_accidents.GeneralConstants
import com.itechart.ny_accidents.entity.WeatherEntity
import com.itechart.ny_accidents.utils.DateUtils

import scala.io.Source
import scala.util.control.Exception

class WeatherParser {

  def parseCsv(path: String): Seq[WeatherEntity] = {
    //    CSVReader.open(path)
    //      .all()
    val bufferedSource = Source.fromFile(path)
    bufferedSource.getLines()
      .map(_.split(";"))
      .toList
      .map(_.map(_.replaceAll("\"", "")).toList)
      .map(parseWeatherLine)
      .filter(_.isDefined)
      .map(_.get)
  }

  def parseCsv(paths: Array[String]): Seq[WeatherEntity] = {
    paths.map(parseCsv).reduce(_ ++ _)
  }

  def parseWeatherLine(columns: List[String]): Option[WeatherEntity] = {
    Exception.allCatch.opt(
      WeatherEntity(
      0,
      1,
      DateUtils.parseDate(columns.head, GeneralConstants.DATE_FORMATTER_WEATHER).get,
      columns(1).toDouble,
      columns(3).toDouble,
      columns(4).toDouble,
      columns(6).toDouble,
      columns(8),
      columns(11).toDouble
    ))
  }

}
