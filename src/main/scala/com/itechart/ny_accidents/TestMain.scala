package com.itechart.ny_accidents

import com.google.inject.Guice
import com.itechart.ny_accidents.database.DistrictsStorage
import com.itechart.ny_accidents.database.dao.DistrictsDAO
import com.itechart.ny_accidents.database.dao.cache.MergedDataCacheDAO
import com.itechart.ny_accidents.entity.District
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.service.MergeService
import com.itechart.ny_accidents.service.metric.WeatherMetricService
import com.itechart.ny_accidents.utils.StringUtils

object TestMain extends App {
  val injector = Guice.createInjector(new GuiceModule)
  val accidentsParser = injector.getInstance(classOf[AccidentsParser])
  val mergeService = injector.getInstance(classOf[MergeService])
  val weatherMetricService = injector.getInstance(classOf[WeatherMetricService])
  val cacheService = injector.getInstance(classOf[MergedDataCacheDAO])
  val districtsDao = injector.getInstance(classOf[DistrictsDAO])
  val districtsStorage = new DistrictsStorage(districtsDao)

  val dist = districtsStorage.districts.head

  println("AREA: " + dist.geometry.getArea)
  println(dist.districtName)

}
