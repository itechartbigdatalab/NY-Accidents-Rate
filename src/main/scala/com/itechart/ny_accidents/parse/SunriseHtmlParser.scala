package com.itechart.ny_accidents.parse

import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import com.itechart.ny_accidents.constants.GeneralConstants.DATE_SUNRISES_FORMAT
import com.itechart.ny_accidents.constants.GeneralConstants.DATE_TIME_SUNRISES_FORMAT
import com.itechart.ny_accidents.constants.GeneralConstants.SUNRISE_YEAR

class SunriseHtmlParser {

  def parseSunrisesSunsets: Seq[Seq[String]] = {
    val browser = JsoupBrowser()
    val kisa = (1 to 1)
      .map("https://sunrise-sunset.org/us/new-york-ny/" + SUNRISE_YEAR + "/" + _)
      .map(browser.get)
      .map(_ >> elementList(".day"))
      .map(_.map(day => (day >> attr("rel")) +: (day >> texts("td")).toSeq.take(4)))
      .reduce(_ ++ _)
      .map(dates => {
        val dayStr = dates.head
        DATE_SUNRISES_FORMAT.parse(dayStr) +: dates.drop(1)
          .map(dayStr + " " + _)
          .map(DATE_TIME_SUNRISES_FORMAT.parse)
      })

    Seq[Seq[String]]()
  }

}
