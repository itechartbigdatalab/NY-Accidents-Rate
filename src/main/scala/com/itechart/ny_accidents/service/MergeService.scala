package com.itechart.ny_accidents.service

import com.itechart.ny_accidents.database.DistrictsStorage
import com.itechart.ny_accidents.database.dao.MergedDataCacheDAO
import com.itechart.ny_accidents.entity.{Accident, MergedData, ReportAccident, ReportMergedData}
import com.itechart.ny_accidents.spark.Spark
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.reflect.ClassTag

object MergeService {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  private var counter = 0
  private val ds = new DistrictsService
  private val service = new WeatherMappingService()
//  private val cache = new MergedDataCacheDAO


  def mergeAccidentsWithWeatherAndDistricts[A,B](accidents: RDD[A], fun: A => B)(implicit tag: ClassTag[B]): RDD[B] = {
    accidents.map(obj => fun(obj))
  }

  def mapper(value: Accident): MergedData = {
    logger.debug("Accident: " + value)
    println("COUNTER: " + counter)
    counter += 1
    value.uniqueKey match {
      case Some(pk) =>
        MergedDataCacheDAO.readFromCache(pk) match {
          case Some(data) => data
          case None =>
            val data = createMergedData(value)
            MergedDataCacheDAO.cacheData(data)
            data
        }
      case None => createMergedData(value)
    }

  }

  private def createMergedData(value: Accident): MergedData = {
    (value.latitude, value.longitude) match {
      case (Some(latitude), Some(longitude)) =>
        val district = ds.getDistrict(latitude, longitude, DistrictsStorage.districts)
        value.dateTimeMillis match {
          case Some(mills) =>
            val weather = service.findWeatherByTimeAndCoordinates(mills, latitude, longitude)
            MergedData(value, district, weather)
          case _ => MergedData(value, district, None)
        }
      case _ => MergedData(value, None, None)
    }
  }

  // todo remove counter!
  def splitDataMapper(value: ReportAccident): ReportMergedData = {
    println("COUNTER: " + counter)
    counter += 1

    (value.latitude, value.longitude) match {
      case (Some(latitude), Some(longitude)) =>
        val district = ds.getDistrict(latitude, longitude, DistrictsStorage.districts)
        value.dateTimeMillis match {
          case Some(mills) =>
            val weather = service.findWeatherByTimeAndCoordinates(mills, latitude, longitude)
            ReportMergedData(value, district, weather)
          case _ => ReportMergedData(value, district, None)
        }
      case _ => ReportMergedData(value, None, None)
    }
  }
}