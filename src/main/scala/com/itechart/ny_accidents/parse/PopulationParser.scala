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
  private lazy val POPULATION_YEAR = 2010


  def readData(fileName: String): RDD[Population] = {

    val csvData: Array[Row] = Spark.sparkSql.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(fileName)
      .collect()

    Spark.sc.parallelize(csvData.map(mapper)
      .filter(_.isDefined)
      .map(_.get)
      .filter(_.year == 2010))
  }

  def mapper(row: Row): Option[Population] = {
    Exception.allCatch.opt {
      val districtName = row(PopulationsHeader.NTA_NAME).toString
      val district = districtsService.getDistrict(districtName.toLowerCase,
        districtsStorage.districts).get

      Population(
        row(PopulationsHeader.YEAR_COL).toString.toInt,
        district,
        row(PopulationsHeader.POPULATION).toString.toInt,
        district.geometry.getArea
      )
    }
  }
}

