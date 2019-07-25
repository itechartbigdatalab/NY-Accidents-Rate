package com.itechart.ny_accidents.parse

import com.itechart.ny_accidents.constants.AccidentsHeader._
import com.itechart.ny_accidents.entity.AccidentSparkFormat
import com.itechart.ny_accidents.spark.Spark
import com.itechart.ny_accidents.spark.Spark.sparkSql.implicits._
import com.itechart.ny_accidents.utils.ParserUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory


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
      ParserUtils.toLongFromInt(accident, UNIQUE_NUMBER),
      ParserUtils.toDate(accident, DATE_C),
      ParserUtils.toMillis(accident, DATE_C, TIME_C),
      ParserUtils.toString(accident, BOROUGH_C),
      ParserUtils.toDouble(accident, LATITUDE_C),
      ParserUtils.toDouble(accident, LONGITUDE_C),
      ParserUtils.toString(accident, ON_STREET_NAME_C),
      ParserUtils.toString(accident, CROSS_STREET_NAME_C),
      ParserUtils.toString(accident, OFF_STREET_NAME_C),
      ParserUtils.toInt(accident, PERSONS_INJURED_C),
      ParserUtils.toInt(accident, PERSONS_KILLED_C),
      ParserUtils.toInt(accident, PEDESTRIANS_INJURED),
      ParserUtils.toInt(accident, PERSONS_KILLED_C),
      ParserUtils.toInt(accident, CYCLIST_INJURED),
      ParserUtils.toInt(accident, CYCLIST_KILLED),
      ParserUtils.toInt(accident, MOTORIST_INJURED),
      ParserUtils.toInt(accident, MOTORIST_KILLED),
      ParserUtils.toStringList(accident, CONTRIBUTING_FACTOR_VEHICLE_COLUMNS),
      ParserUtils.toStringList(accident, VEHICLE_TYPE_CODE_COLUMNS)
    )
  }



}

