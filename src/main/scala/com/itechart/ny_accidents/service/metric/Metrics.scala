package com.itechart.ny_accidents.service.metric

import com.itechart.ny_accidents.entity.{District, MergedData, ReportMergedData, WeatherForAccident}
import com.itechart.ny_accidents.spark.Spark
import org.apache.spark.rdd.RDD

object Metrics {
  def getPhenomenonPercentage(data: RDD[MergedData]): RDD[(String, Double)] = {
    val filteredData = data.filter(_.weather.isDefined).map(_.weather.get)
    val length = filteredData.count()
    val groupedData = filteredData.groupBy(_.phenomenon)

    // TODO Need rewrite
    calculatePercentage[WeatherForAccident, String](groupedData, length).map(metric => {
      metric._1.isEmpty match {
        case true => ("Clear", metric._2)
        case false => metric
      }
    })
  }

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

  def calculatePercentage[A,B](data: RDD[(B, Iterable[A])], dataLength: Long): RDD[(B, Double)] = {
    val statsMap: RDD[(B, Int)] = data.map(tuple => (tuple._1, tuple._2.size))
    statsMap.map(tuple => (tuple._1, (tuple._2.toDouble / dataLength.toDouble) * 100.0))
  }
}