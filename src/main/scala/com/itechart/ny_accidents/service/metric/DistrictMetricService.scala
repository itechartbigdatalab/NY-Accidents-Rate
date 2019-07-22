package com.itechart.ny_accidents.service.metric

import com.itechart.ny_accidents.entity.{DetailedDistrictData, District, MergedData}
import com.itechart.ny_accidents.spark.Spark
import org.apache.spark.rdd.RDD

object DistrictMetricService extends PercentageMetricService {

  def getDetailedDistrictData(data: RDD[MergedData]):  RDD[DetailedDistrictData] = {
    val filteredData = data.filter(_.district.isDefined).map(data =>
       DetailedDistrictData(data.district.get.districtName, data.accident.pedestriansInjured, data.accident.pedestriansKilled,
        data.accident.cyclistInjured, data.accident.cyclistKilled, data.accident.motoristInjured,
         data.accident.motoristKilled, data.accident.motoristInjured + data.accident.cyclistInjured +
           data.accident.pedestriansInjured +
           5 * (data.accident.motoristKilled + data.accident.cyclistKilled + data.accident.pedestriansKilled),
         data.accident.pedestriansInjured + 5 * data.accident.pedestriansKilled,
         data.accident.cyclistInjured + 5 * data.accident.cyclistKilled,
         data.accident.motoristInjured + 5 * data.accident.motoristKilled
       ))

    val groupedData = filteredData.groupBy(_.districtName)
      .map(detailedDistrictData => (detailedDistrictData._1, detailedDistrictData._2
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
          accumulator.motorist + next.motorist))))
    groupedData.values
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
