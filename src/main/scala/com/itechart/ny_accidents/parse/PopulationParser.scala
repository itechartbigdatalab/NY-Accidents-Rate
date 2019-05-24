package com.itechart.ny_accidents.parse

import com.google.inject.{Guice, Injector}
import com.itechart.ny_accidents.GuiceModule
import com.itechart.ny_accidents.constants.PopulationsHeader
import com.itechart.ny_accidents.database.DistrictsStorage
import com.itechart.ny_accidents.entity.Population
import com.itechart.ny_accidents.service.DistrictsService
import com.itechart.ny_accidents.spark.Spark
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import scala.util.Try
import scala.util.control.Exception

object PopulationParser {
  val injector: Injector = Guice.createInjector(new GuiceModule)
  val districtsService: DistrictsService = injector.getInstance(classOf[DistrictsService])
  val districtsStorage: DistrictsStorage = injector.getInstance(classOf[DistrictsStorage])

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
