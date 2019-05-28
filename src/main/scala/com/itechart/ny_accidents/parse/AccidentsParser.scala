package com.itechart.ny_accidents.parse

import java.util.Date

import com.google.inject.Singleton
import com.itechart.ny_accidents.constants.AccidentsHeader._
import com.itechart.ny_accidents.constants.GeneralConstants
import com.itechart.ny_accidents.entity.Accident
import com.itechart.ny_accidents.spark.Spark
import com.itechart.ny_accidents.utils.DateUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, Row}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.util.Try

@Singleton
class AccidentsParser {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private lazy val YEAR_COL = 29

  def readCsv(path: String): DataFrame = {
    Spark.sparkSql.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  def readData(fileName: String): RDD[Accident] = {
    val csvAccidentsData = readCsv(fileName).collect()
    Spark.sc.parallelize(csvAccidentsData.map(accidentsMapper))
  }

  def splitDatasetByYear(path: String): (StructType, Map[Int, Array[Row]]) = {
    val csvContent: DataFrame = readCsv(path)
    import Spark.sparkSql.implicits._

    val dfWithSchema = csvContent
      .filter($"DATE".isNotNull)
      .filter("DATE != ''")
      .withColumn("YEAR", year(to_date($"DATE", "MM/dd/yyyy")))

    val schema = dfWithSchema.schema
    val map = dfWithSchema.collect
      .groupBy(row => row.getAs[Int](YEAR_COL))
      .map { case (key, value) => (key, value) }

    (schema, map)
  }

  def accidentsMapper(accident: Row): Accident = {

    Accident(
      toLong(accident, UNIQUE_NUMBER),
      toDate(accident.getString(DATE_C), accident.getString(TIME_C)),
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

  private def toLong(accident: Row, column: Int): Option[Long] = {
    Try(accident.getString(column).toLong).toOption
  }

  private def toDate(dateStr: String, timeStr: String): Option[Date] = {
    val dateTimeStr = dateStr + " " + timeStr
    DateUtils.parseDate(dateTimeStr, GeneralConstants.DATE_TIME_ACCIDENTS_PATTERN)
  }

  private def toMillis(row: Row, dateColumn: Int, timeColumn: Int): Option[Long] = {
    val dateTime = row.getString(dateColumn) + " " + row.getString(timeColumn)
    DateUtils.parseDateToMillis(dateTime, GeneralConstants.DATE_TIME_ACCIDENTS_PATTERN)
  }

  private def toDouble(accident: Row, column: Int): Option[Double] = {
    Try(accident.getString(column).toDouble).toOption
  }

  private def toString(accident: Row, column: Int): Option[String] = {
    Try(accident(column).toString).toOption
  }

  private def toInt(accident: Row, column: Int): Option[Int] = {
    Try(accident.getString(column).toInt).toOption
  }

  private def toStringList(row: Row, columns: Array[Int]): List[Option[String]] = {
    columns
      .map(toString(row, _))
      .toList
  }

}