package com.itechart.ny_accidents.service.metric

import com.itechart.ny_accidents.entity.{District, ReportMergedData}
import com.itechart.ny_accidents.spark.Spark
import org.apache.spark.rdd.RDD

object DistrictMetricService extends PercentageMetricService {

  def getDistrictsPercentage(data: RDD[ReportMergedData]): RDD[(String, Double)] = {
    val filteredData = data.filter(_.district.isDefined).map(_.district.get)
    val length = filteredData.count()
    val groupedData = filteredData.groupBy(_.districtName)
    calculatePercentage[District, String](groupedData, length)
  }

  def getBoroughPercentage(data: RDD[ReportMergedData]): RDD[(String, Double)] = {
    val filteredData = data.filter(_.district.isDefined).map(_.district.get)
    val length = filteredData.count()
    val groupedData = filteredData.groupBy(_.boroughName)
    calculatePercentage[District, String](groupedData, length)
  }

  def getDistrictsPercentageByBorough(data: RDD[ReportMergedData]): RDD[(String, Map[String, Double])] = {
    data.filter(_.district.isDefined).groupBy(_.district.get.boroughName).map(a => {
      val borough = a._1
      val districts = getDistrictsPercentage(Spark.sc.parallelize(a._2.toSeq)).collect.toMap
      (borough, districts)
    })
  }
}
