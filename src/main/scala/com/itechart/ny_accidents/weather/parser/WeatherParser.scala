package com.itechart.ny_accidents.weather.parser

import com.github.tototoshi.csv.CSVReader
import com.itechart.ny_accidents.weather.entity.WeatherEntity

import scala.util.control.Exception

class WeatherParser {

  def parseCsv(path: String) : Seq[WeatherEntity] = {
    CSVReader.open(path).all().drop(7).map(parseWeatherLine)
  }

  def parseWeatherLine(columns: List[String]) : Option[WeatherEntity] = {
    Exception.allCatch.opt(WeatherEntity(
      columns
    ))
  }
}
