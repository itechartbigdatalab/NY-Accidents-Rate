package com.itechart.ny_accidents.service.metric

import com.google.inject.Inject
import com.itechart.ny_accidents.database.dao.PopulationStorage
import com.itechart.ny_accidents.entity.MergedData
import com.itechart.ny_accidents.service.PopulationService
import com.itechart.ny_accidents.utils.NumberUtils
import javax.inject.Singleton
import org.apache.spark.rdd.RDD

@Singleton
class PopulationMetricService @Inject()(populationStorage: PopulationStorage) {

  def getPopulationToNumberOfAccidentsRatio(data: RDD[MergedData]): RDD[(String, Double, Double, Int)] = {
    val populationMap = populationStorage.populationMap
    data.filter(_.district.isDefined).groupBy(_.district.get).map{case (district, mergedData) => {
      val districtHash = district.hashCode()
      val accidentsNumber = mergedData.size
      populationMap.get(districtHash) match {
        case Some(value) =>
          val density = PopulationService.calculateDensity(value)
          density match {
            case 0 => None
            case _ =>
              val ratio: Double = accidentsNumber / density
              Some(district.districtName, ratio, density, accidentsNumber)
          }
        case _ => None
      }
    }}.filter(_.isDefined).map(_.get)
  }
}
