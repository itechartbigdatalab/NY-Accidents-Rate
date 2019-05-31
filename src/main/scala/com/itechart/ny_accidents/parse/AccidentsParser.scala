package com.itechart.ny_accidents.parse

import java.time.LocalDateTime

import com.itechart.ny_accidents.constants.AccidentsHeader._
import com.itechart.ny_accidents.constants.GeneralConstants
import com.itechart.ny_accidents.constants.GeneralConstants.ZERO_DATE
import com.itechart.ny_accidents.entity.{Accident, AccidentWithoutOption}
import com.itechart.ny_accidents.spark.Spark
import com.itechart.ny_accidents.utils.DateUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import com.itechart.ny_accidents.spark.Spark.districtEncoder
import org.slf4j.LoggerFactory

import scala.util.Try


object AccidentsParser {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private lazy val YEAR_COL = 29
  private lazy val DATE_HEADER = "DATE"
  private lazy val DATE_MASK = "MM/dd/yyyy"

  private lazy val YEAR_HEADER = "YEAR"

  def readCsv(path: String): Dataset[Row] = {
    Spark.sparkSql.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  def readData(fileName: String): Dataset[AccidentWithoutOption] = {
    readCsv(fileName).map(accidentsMapper)
//    Spark.sc.parallelize(csvAccidentsData.map(accidentsMapper))
  }

  def splitDatasetByYear(path: String): (StructType, Map[Int, Array[Row]]) = {
    val csvContent: DataFrame = readCsv(path)
    import Spark.sparkSql.implicits._

    val dfWithSchema = csvContent
      .filter($"$DATE_HEADER".isNotNull)
      .withColumn(YEAR_HEADER, year(to_date($"$DATE_HEADER", DATE_MASK)))

    val schema = dfWithSchema.schema
    val map = dfWithSchema.collect
      .groupBy(row => row.getAs[Int](YEAR_COL))
      .map { case (key, value) => (key, value) }

    (schema, map)
  }

  def accidentsMapper(accident: Row): AccidentWithoutOption = {
    AccidentWithoutOption(
      toLongInt(accident, UNIQUE_NUMBER),
      toLocalDate(accident.getString(DATE_C), accident.getString(TIME_C)),
      toMillis(accident, DATE_C, TIME_C),
      toString(accident, BOROUGH_C),
      toDouble(accident, LATITUDE_C),
      toDouble(accident, LONGITUDE_C),
      toString(accident, ON_STREET_NAME_C),
      toString(accident, CROSS_STREET_NAME_C),
      toString(accident, OFF_STREET_NAME_C),
      toInt(accident, PERSONS_INJURED_C),
      toInt(accident, PERSONS_KILLED_C),
      toInt(accident, PEDESTRIANS_INJURED),
      toInt(accident, PERSONS_KILLED_C),
      toInt(accident, CYCLIST_INJURED),
      toInt(accident, CYCLIST_KILLED),
      toInt(accident, MOTORIST_INJURED),
      toInt(accident, MOTORIST_KILLED),
      toStringList(accident, CONTRIBUTING_FACTOR_VEHICLE_COLUMNS),
      toStringList(accident, VEHICLE_TYPE_CODE_COLUMNS))
  }


  private def toLongInt(accident: Row, column: Int): Long = {
    accident.getInt(column).toLong
  }

  private def toLocalDate(dateStr: String, timeStr: String): LocalDateTime = {
    val dateTimeStr = dateStr + " " + timeStr
    DateUtils.parseDate(dateTimeStr, GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER).getOrElse(ZERO_DATE)
  }

  private def toMillis(row: Row, dateColumn: Int, timeColumn: Int): Long = {
    val dateTime = row.getString(dateColumn) + " " + row.getString(timeColumn)
    DateUtils.parseDateToMillis(dateTime, GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER).getOrElse(0L)
  }

  private def toDouble(accident: Row, column: Int): Double = {
    Try(accident.getDouble(column)).toOption.getOrElse(0)
  }

  private def toString(accident: Row, column: Int): String = {
    accident.getString(column)
  }

  private def toInt(accident: Row, column: Int): Int = {
    Try(accident.getInt(column)).getOrElse(0)
  }

  private def toStringList(row: Row, columns: Array[Int]): List[String] = {
    columns
      .map(toString(row, _))
      .toList
  }

}