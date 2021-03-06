package com.itechart.accidents.service.metric

import com.itechart.accidents.entity.{DetailedDistrictData, District, MergedData}
import com.itechart.accidents.spark.Spark
import org.apache.spark.rdd.RDD

object DistrictMetricService extends PercentageMetricService {

  private val weight = 5

  def getDetailedDistrictData(data: RDD[MergedData]):  RDD[DetailedDistrictData] = {
   data.filter(_.district.isDefined).map(data =>
       DetailedDistrictData(data.district.get.districtName, data.accident.pedestriansInjured, data.accident.pedestriansKilled,
        data.accident.cyclistInjured, data.accident.cyclistKilled, data.accident.motoristInjured,
         data.accident.motoristKilled, data.accident.motoristInjured + data.accident.cyclistInjured +
           data.accident.pedestriansInjured +
           weight * (data.accident.motoristKilled + data.accident.cyclistKilled + data.accident.pedestriansKilled),
         data.accident.pedestriansInjured + weight * data.accident.pedestriansKilled,
         data.accident.cyclistInjured + weight * data.accident.cyclistKilled,
         data.accident.motoristInjured + weight * data.accident.motoristKilled
       )).groupBy(_.districtName)
      .map({case (districtName, detailedDistrictData) => (districtName, detailedDistrictData
        .reduce((accumulator,next) => DetailedDistrictData(accumulator.districtName,
          accumulator.pedestriansInjured + next.pedestriansInjured,
          accumulator.pedestriansKilled + next.pedestriansKilled,
          accumulator.cyclistInjured + next.cyclistInjured,
          accumulator.cyclistKilled + next.cyclistKilled,
          accumulator.motoristInjured + next.motoristInjured,
          accumulator.motoristKilled + next.motoristKilled,
          accumulator.total + next.total,
          accumulator.pedestrians + next.pedestrians,
          accumulator.cyclist + next.cyclist,
          accumulator.motorist + next.motorist)))}).values
  }

  def getDistrictsPercentage(data: RDD[MergedData]): RDD[(String, Int, Double)] = {
    val filteredData = data.filter(_.district.isDefined).map(_.district.get)
    val length = filteredData.count()
    val groupedData = filteredData.groupBy(_.districtName)
    calculatePercentage[District, String](groupedData, length)
  }

  def getBoroughPercentage(data: RDD[MergedData]): RDD[(String, Int, Double)] = {
    val filteredData = data.filter(_.district.isDefined).map(_.district.get)
    val length = filteredData.count()
    val groupedData = filteredData.groupBy(_.boroughName)
    calculatePercentage[District, String](groupedData, length)
  }

  def getDistrictsPercentageByBorough(data: RDD[MergedData]): RDD[(String, Map[String, (Int, Double)])] = {
    data.filter(_.district.isDefined).groupBy(_.district.get.boroughName).map(accidentsByBorough => {
      val (borough, data) = accidentsByBorough
      val districts = getDistrictsPercentage(Spark.sc.parallelize(data.toSeq))
        .map(summaryBoroughData => (summaryBoroughData._1, (summaryBoroughData._2, summaryBoroughData._3)))
        .collect
        .toMap
      (borough, districts)
    })
  }
}
