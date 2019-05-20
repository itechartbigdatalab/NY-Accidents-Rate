package com.itechart.ny_accidents.service.metric

import com.itechart.ny_accidents.entity.{District, MergedData, WeatherForAccident}

import org.apache.spark.rdd.RDD

object Metrics {
  def getPhenomenonPercentage(data: RDD[MergedData]): RDD[(String, Double)] = {
    val filteredData = data.filter(_.weather.isDefined).map(_.weather.get)
    val length = filteredData.count()
    val groupedData = filteredData.groupBy(_.phenomenon)
    calculatePercentage[WeatherForAccident, String](groupedData, length)
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

  def getDistrictsPercentageByBorough(data: RDD[MergedData]): RDD[(String, RDD[(String, Double)])] = {
    data.filter(_.district.isDefined).groupBy(_.district.get.boroughName).map(a => {
      (a._1, getDistrictsPercentage(data.context.parallelize(a._2.toSeq)))
    })
  }

  def calculatePercentage[A,B](data: RDD[(B, Iterable[A])], dataLength: Long): RDD[(B, Double)] = {
    val statsMap: RDD[(B, Int)] = data.map(tuple => (tuple._1, tuple._2.size))
    statsMap.map(tuple => (tuple._1, (tuple._2.toDouble / dataLength.toDouble) * 100.0))
  }
}
