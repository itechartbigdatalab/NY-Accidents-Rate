package com.itechart.ny_accidents.parse

import com.google.inject.{Inject, Singleton}
import com.itechart.ny_accidents.constants.PopulationsHeader
import com.itechart.ny_accidents.database.DistrictsStorage
import com.itechart.ny_accidents.entity.{Accident, District, Population}
import com.itechart.ny_accidents.service.DistrictsService
import com.itechart.ny_accidents.spark.Spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import scala.util.control.Exception

@Singleton
class PopulationParser @Inject()(districtsService: DistrictsService,
                                 districtsStorage: DistrictsStorage) {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  def readData(fileName: String): RDD[Population] = {

    val csvData: Array[Row] = Spark.sparkSql.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(fileName)
      .collect()

    Spark.sc.parallelize(csvData.map(mapper).filter(_.isDefined).map(_.get))
  }

  def mapper(row: Row): Option[Population] = {
    Exception.allCatch.opt (
      Population(
        row(PopulationsHeader.YEAR_COL).toString.toInt,
        districtsService.getDistrict(row(PopulationsHeader.NTA_NAME)
          .toString.toLowerCase, districtsStorage.districts).get,
        row(PopulationsHeader.POPULATION).toString.toInt
      )
    )
  }
}
