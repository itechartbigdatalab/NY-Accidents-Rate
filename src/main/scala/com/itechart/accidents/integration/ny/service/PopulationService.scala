package com.itechart.accidents.integration.ny.service

import com.itechart.accidents.entity.{MergedData, Population}
import com.itechart.accidents.utils.NumberUtils
import org.apache.spark.rdd.RDD

import scala.util.Try

object PopulationService {
  private lazy val NUMBER_NORMALIZATION_FACTOR = 1000000

  def mergePopulationAndAccidents(populationMap: Map[Int, Population],
                                  accidents: RDD[MergedData]): RDD[(MergedData, Population)] = {
    accidents.filter(_.district.isDefined).map(accident => {
      val district = accident.district.get
      val population = Try(populationMap(district.hashCode)).toOption
      (accident, population)
    }).filter(_._2.isDefined).map(tuple => (tuple._1, tuple._2.get))
  }

  def calculateDensity(population: Population): Double = {
    NumberUtils.truncateDouble(population.population.toDouble / (population.area * NUMBER_NORMALIZATION_FACTOR))
  }
}
