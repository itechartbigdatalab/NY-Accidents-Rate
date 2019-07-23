package com.itechart.ny_accidents.parse

import java.time.LocalDateTime

import com.itechart.ny_accidents.constants.AccidentsHeader._
import com.itechart.ny_accidents.constants.GeneralConstants
import com.itechart.ny_accidents.constants.GeneralConstants.ZERO_DATE
import com.itechart.ny_accidents.entity.AccidentSparkFormat
import com.itechart.ny_accidents.spark.Spark
import com.itechart.ny_accidents.spark.Spark.sparkSql.implicits._
import com.itechart.ny_accidents.utils.DateUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.sql.{Date => SqlDate}
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

  def readData(fileName: String): Dataset[AccidentSparkFormat] = {
    readCsv(fileName).map(accidentsMapper)
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


  def accidentsMapper(accident: Row): AccidentSparkFormat = {
    AccidentSparkFormat(
      toLongFromInt(accident, UNIQUE_NUMBER),
      toDate(accident, DATE_C),
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
      toStringList(accident, VEHICLE_TYPE_CODE_COLUMNS)
    )
  }


  private def toLongFromInt(accident: Row, column: Int): Option[Long] = {
    Try(accident.getInt(column).toLong).toOption match {
      case Some(value) =>
        if (value == 0) {
          None
        } else {
          Some(value)
        }
      case None => None
    }
  }

  private def toLocalDate(dateStr: String, timeStr: String): LocalDateTime = {
    val dateTimeStr = dateStr + " " + timeStr
    DateUtils.parseDate(dateTimeStr, GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER).getOrElse(ZERO_DATE)
  }

  private def toMillis(row: Row, dateColumn: Int, timeColumn: Int): Option[Long] = {
    val dateTime = row.getString(dateColumn) + " " + row.getString(timeColumn)
    val value = DateUtils.parseDateToMillis(dateTime, GeneralConstants.DATE_TIME_ACCIDENTS_FORMATTER).getOrElse(0L)
    if (value == 0) None
    else Some(value)
  }

  private def toDouble(accident: Row, column: Int): Option[Double] = {
    val value: Double = Try(accident.getDouble(column)).toOption.getOrElse(0)
    if (value == 0) None
    else Some(value)
  }

  private def toString(accident: Row, column: Int): Option[String] = {
    Option(accident.getString(column))
  }

  private def toInt(accident: Row, column: Int): Int = {
    val value = Try(accident.getInt(column)).getOrElse(0)
    value
  }

  private def toStringList(row: Row, columns: Array[Int]): List[Option[String]] = {
    columns
      .map(toString(row, _))
      .toList
  }

  private def toDate(row: Row, column: Int): Option[SqlDate] = {
    toString(row, column) match {
      case Some(value) => DateUtils.parseSqlDate(value, GeneralConstants.DATE_FORMATTER_ACCIDENTS)
      case None => None
    }
  }
}

