package com.itechart.accidents.integration.ny.parse

import com.google.inject.Singleton
import com.itechart.accidents.constants.Injector.injector
import com.itechart.accidents.constants.PopulationsHeader
import com.itechart.accidents.database.DistrictsStorage
import com.itechart.accidents.entity.Population
import com.itechart.accidents.service.DistrictsService
import com.itechart.accidents.spark.Spark
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import scala.util.Try
import scala.util.control.Exception

@Singleton
class NYPopulationParser {
  lazy val districtsService: DistrictsService = injector.getInstance(classOf[DistrictsService])
  lazy val districtsStorage: DistrictsStorage = injector.getInstance(classOf[DistrictsStorage])

  private lazy val logger = LoggerFactory.getLogger(getClass)
  private lazy val POPULATION_YEAR = 2010


  def readData(fileName: String): Map[Int, Population] = {
    val csvData: Array[Row] = Spark.sparkSql.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(fileName)
      .collect()

    csvData.map(hashMapMapper).filter(_._2.isDefined)
      .map(tuple => (tuple._1.get, tuple._2.get))
      .filter(_._2.year == POPULATION_YEAR)
      .toMap
  }

  def hashMapMapper(row: Row): (Option[Int], Option[Population]) = {
    val population = mapper(row)
    val hashCode: Option[Int] = Try(population.get.district.hashCode()).toOption

    (hashCode, population)
  }

  def mapper(row: Row): Option[Population] = {
    Exception.allCatch.opt {
      val districtName = row(PopulationsHeader.NTA_NAME).toString
      val district = districtsService.getDistrict(districtName.toLowerCase,
        districtsStorage.districtsWithGeometry).get

      Population(
        row(PopulationsHeader.YEAR_COL).toString.toInt,
        district,
        row(PopulationsHeader.POPULATION).toString.toInt,
        district.geometry.getArea
      )
    }
  }
}
