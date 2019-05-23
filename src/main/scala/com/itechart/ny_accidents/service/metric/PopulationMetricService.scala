package com.itechart.ny_accidents.service.metric

import com.google.inject.Inject
import com.itechart.ny_accidents.database.dao.PopulationStorage
import com.itechart.ny_accidents.entity.MergedData
import com.itechart.ny_accidents.service.PopulationService
import com.itechart.ny_accidents.spark.Spark
import org.apache.spark.rdd.RDD

object PopulationMetricService {

  def getPopulationToNumberOfAccidentsRatio(data: RDD[MergedData]): RDD[(String, Double)] = {

    data.filter(_.district.isDefined).groupBy(_.district.get).map(dat => {
      val districtHash = dat._1.hashCode()
      val accidentsNumber = dat._2.size
      PopulationStorage.populationMap.get(districtHash) match {
        case Some(value) =>
          val ration = accidentsNumber / PopulationService.calculateDensity(value)
          Some(dat._1.districtName, ration)
        case _ => None
      }
    }).filter(_.isDefined).map(_.get)
  }
}
