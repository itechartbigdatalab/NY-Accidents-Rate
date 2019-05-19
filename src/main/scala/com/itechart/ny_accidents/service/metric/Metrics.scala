package com.itechart.ny_accidents.service.metric

import com.itechart.ny_accidents.entity.{District, MergedData, WeatherForAccident}

class Metrics {
  def getPhenomenonPercentage(data: Seq[MergedData]): Map[String, Double] = {
    val filteredData = data.filter(_.weather.isDefined).map(_.weather.get)
    val length = filteredData.length
    val groupedData = filteredData.groupBy(_.phenomenon)
    calculatePercentage[WeatherForAccident, String](groupedData, length)
  }

  def getDistrictsPercentage(data: Seq[MergedData]): Map[String, Double] = {
    val filteredData = data.filter(_.district.isDefined).map(_.district.get)
    val length = filteredData.length
    val groupedData = filteredData.groupBy(_.districtName)
    calculatePercentage[District, String](groupedData, length)
  }

  def getBoroughPercentage(data: Seq[MergedData]): Map[String, Double] = {
    val filteredData = data.filter(_.district.isDefined).map(_.district.get)
    val length = filteredData.length
    val groupedData = filteredData.groupBy(_.boroughName)
    calculatePercentage[District, String](groupedData, length)
  }

  def getDistrictsPercentageByBorough(data: Seq[MergedData]): Map[String, Map[String, Double]] = {
    data.filter(_.district.isDefined).groupBy(_.district.get.boroughName).map(a => {
      (a._1, getDistrictsPercentage(a._2))
    })
  }

  private def calculatePercentage[A,B](data: Map[B, Seq[A]], dataLength: Int): Map[B, Double] = {
    val statsMap: Map[B, Int] = data.map(tuple => (tuple._1, tuple._2.length))
    statsMap.map(tuple => (tuple._1, (tuple._2.toDouble / dataLength.toDouble) * 100.0))
  }
}
