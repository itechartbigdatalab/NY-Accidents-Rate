package com.itechart.ny_accidents.parse

import java.util.Date

import com.itechart.ny_accidents.constants.GeneralConstants
import com.itechart.ny_accidents.constants.GeneralConstants.{DATE_SUNRISES_FORMAT, DATE_TIME_SUNRISES_FORMAT}
import com.itechart.ny_accidents.entity.WeatherEntity
import com.itechart.ny_accidents.utils.DateUtils
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.scraper.ContentExtractors.{attr, elementList, texts}

import scala.io.Source
import scala.util.control.Exception

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

  def parseSunrisesSunsets: Map[Date, Seq[Date]] = {
    val browser = JsoupBrowser()
    (1 to 12)
      .map("https://sunrise-sunset.org/us/new-york-ny/2019/" + _)
      .map(browser.get)
      .map(_ >> elementList(".day"))
      .map(_.map(day => (day >> attr("rel")) +: (day >> texts("td")).toSeq.take(4)))
      .reduce(_ ++ _)
      .map(dates => {
        val dayStr = dates.head
        (DATE_SUNRISES_FORMAT.parse(dayStr),
          dates.drop(1)
            .map(dayStr + " " + _)
            .map(DATE_TIME_SUNRISES_FORMAT.parse))
      }).toMap
  }

}