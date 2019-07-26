package com.itechart.accidents.report

import com.itechart.accidents.constants.GeneralConstants
import com.itechart.accidents.entity.MergedData
import com.itechart.accidents.utils.DateUtils
import org.apache.spark.rdd.RDD

class DatasetGenerator {

  def generateAccidentByWeatherDataset(data: RDD[MergedData]): RDD[Seq[String]] = {
    data
      .filter(obj => obj.weather.isDefined &&
        obj.accident.localDateTime.isDefined &&
        obj.accident.latitude.isDefined &&
        obj.accident.longitude.isDefined)
      .map(mergedData => Seq(
        DateUtils.getStringFromDate(mergedData.accident.localDateTime.get,
          GeneralConstants.KIBANA_REPORT_TIME_FORMAT),
        mergedData.accident.latitude.toString,
        mergedData.accident.longitude.toString,
        mergedData.weather.get.temperature.toString,
        mergedData.weather.get.phenomenon.toString))
  }

  def generateAccidentByRegionDataset(data: RDD[MergedData]): RDD[Seq[String]] = {
    data.filter(obj => obj.district.isDefined &&
      obj.accident.latitude.isDefined &&
      obj.accident.longitude.isDefined)
      .map(mergedData => Seq(
        DateUtils.getStringFromDate(mergedData.accident.localDateTime.get,
          GeneralConstants.KIBANA_REPORT_TIME_FORMAT),
        mergedData.accident.latitude.toString,
        mergedData.accident.longitude.toString,
        mergedData.district.get.districtName,
        mergedData.district.get.boroughName))
  }
}
