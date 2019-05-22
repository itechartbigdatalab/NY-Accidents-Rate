package com.itechart.ny_accidents.service.metric

import com.itechart.ny_accidents.entity.{District, MergedData, ReportMergedData}
import com.itechart.ny_accidents.spark.Spark
import org.apache.spark.rdd.RDD

object DistrictMetricService extends PercentageMetricService {

  def getDistrictsPercentage(data: RDD[MergedData]): RDD[(String, Double)] = {
    val filteredData = data.filter(_.district.isDefined).map(_.district.get)
    val length = filteredData.count()
    val groupedData = filteredData.groupBy(_.districtName)
    calculatePercentage[District, String](groupedData, length)
  }

  def getBoroughPercentage(data: RDD[MergedData]): RDD[(String, Double)] = {
    val filteredData = data.filter(_.district.isDefined).map(_.district.get)
    val length = filteredData.count()
    val groupedData = filteredData.groupBy(_.boroughName)
    calculatePercentage[District, String](groupedData, length)
  }

  def getDistrictsPercentageByBorough(data: RDD[MergedData]): RDD[(String, Map[String, Double])] = {
    data.filter(_.district.isDefined).groupBy(_.district.get.boroughName).map(a => {
      val borough = a._1
      val districts = getDistrictsPercentage(Spark.sc.parallelize(a._2.toSeq)).collect.toMap
      (borough, districts)
    })
  }
}
