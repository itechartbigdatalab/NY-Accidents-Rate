package com.itechart.accidents.parse


import com.google.inject.Singleton
import com.itechart.accidents.entity.DistrictWithGeometry
import com.itechart.accidents.utils.PostgisUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.control.Exception

@Singleton
class DistrictsParser {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  private lazy val GEOM_COL_NUMBER = 1
  private lazy val BORONAME_COL_NUMBER = 3
  private lazy val NTANAME_COL_NUMBER = 5

  // TODO should add check if file exists
  def parseCsv(path: String, spark: SparkSession): Seq[Option[DistrictWithGeometry]] = {
    val csv: Array[Row] = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load(path)
      .collect()

    csv.map(parseList).toSeq
  }

  def parseList(row: Row): Option[DistrictWithGeometry] = {
    Exception.allCatch.opt(DistrictWithGeometry(row(NTANAME_COL_NUMBER).toString,
      row(BORONAME_COL_NUMBER).toString,
      PostgisUtils.getGeometryFromText(row(GEOM_COL_NUMBER).toString)))
  }
}