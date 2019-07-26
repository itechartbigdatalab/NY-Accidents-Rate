package com.itechart.accidents.service.metric

import com.google.inject.Inject
import com.itechart.accidents.entity.MergedData
import com.itechart.accidents.integration.ny.database.dao.PopulationStorage
import com.itechart.accidents.integration.ny.service.PopulationService
import javax.inject.Singleton
import org.apache.spark.rdd.RDD
import com.itechart.accidents.utils.NumberUtils

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
              val ratio: Double = NumberUtils.truncateDouble(accidentsNumber / density)
              Some(district.districtName, ratio, density, accidentsNumber)
          }
        case _ => None
      }
    }}.filter(_.isDefined).map(_.get)
  }
}
