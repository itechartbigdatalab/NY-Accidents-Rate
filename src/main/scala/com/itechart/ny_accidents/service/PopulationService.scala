package com.itechart.ny_accidents.service

import com.google.inject.Singleton
import com.itechart.ny_accidents.entity.{MergedData, Population}
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
    population.population.toDouble / (population.area * NUMBER_NORMALIZATION_FACTOR)
  }
}
