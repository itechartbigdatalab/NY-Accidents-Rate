package com.itechart.ny_accidents.districts.parser

import com.github.tototoshi.csv.CSVReader
import com.itechart.ny_accidents.entity.District
import com.itechart.ny_accidents.utils.PostgisUtils
import com.sun.net.httpserver.Authenticator.{Failure, Success}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}
import scala.util.control.Exception

class DistrictsParser {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  private lazy val GEOM_COL_NUMBER = 1
  private lazy val BORONAME_COL_NUMBER = 3
  private lazy val NTANAME_COL_NUMBER = 5

  // TODO should add check if file exists
  def parseCsv(path: String): Seq[District] = {
    CSVReader.open(path).all().drop(1).map(parseList).filter(_.isDefined).map(_.get)
  }

  def parseList(cols: List[String]): Option[District] = {
    Exception.allCatch.opt(District(cols(NTANAME_COL_NUMBER),
      cols(BORONAME_COL_NUMBER),
      PostgisUtils.getMultiPolygonFromText(cols(GEOM_COL_NUMBER))))
  }
}