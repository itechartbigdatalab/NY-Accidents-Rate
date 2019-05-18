package com.itechart.ny_accidents.parse

import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._

class SunriseHtmlParser {

  def parseSunrisesSunsets: Seq[Seq[String]] = {
    val browser = JsoupBrowser()
    val kisa = (1 to 1)
      .map("https://sunrise-sunset.org/us/new-york-ny/2019/" + _)
      .map(browser.get)
      .map(_ >> elementList(".day"))
      .map(_.map(day => (day >> attr("rel")) +: (day >> texts("td")).toSeq.take(4)))
      .reduce(_ ++ _)
        .

    Seq[Seq[String]]()
  }

}
