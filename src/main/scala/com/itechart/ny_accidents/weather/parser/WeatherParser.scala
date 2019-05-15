package com.itechart.ny_accidents.weather.parser

import com.github.tototoshi.csv.CSVReader
import com.itechart.ny_accidents.GeneralConstants
import com.itechart.ny_accidents.entity.WeatherEntity
import com.itechart.ny_accidents.utils.{DateUtils, StringUtils}

import scala.util.control.Exception

class WeatherParser {

  def parseCsv(path: String): Seq[WeatherEntity] = {
    CSVReader.open(path).all().drop(7).map(parseWeatherLine).filter(_.isDefined).map(_.get)
  }

  def parseWeatherLine(columns: List[String]): Option[WeatherEntity] = {
    Exception.allCatch.opt(WeatherEntity(
      0,
      0,
      DateUtils.parseDate(columns.head, GeneralConstants.DATE_FORMATTER_WEATHER).get,
      columns(1).toInt,
      columns(3).toDouble,
      columns(4).toInt,
      StringUtils.windStringToDoubleParse(columns(6)).get,
      columns(8),
      columns(10).toDouble
    ))
  }

}
