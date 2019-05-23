package com.itechart.ny_accidents

import com.google.inject.Guice
import com.itechart.ny_accidents.database.dao.PopulationStorage
import com.itechart.ny_accidents.parse.PopulationParser
import info.debatty.java.stringsimilarity.{Levenshtein, NormalizedLevenshtein}
import org.slf4j.LoggerFactory

object TestMain extends App {
  lazy val logger = LoggerFactory.getLogger(getClass)
//
  val injector = Guice.createInjector(new GuiceModule)
//  val accidentsParser = injector.getInstance(classOf[AccidentsParser])
//  val mergeService = injector.getInstance(classOf[MergeService])
//  val weatherMetricService = injector.getInstance(classOf[WeatherMetricService])
//  val cacheService = injector.getInstance(classOf[MergedDataCacheDAO])
//  sys.addShutdownHook(cacheService.close)
//
//  val path = "src/main/resources/data.csv"
//  val raws = accidentsParser.readData(path).cache()
//  println("RAWS DATA READ")
//
//  val mergeData: RDD[MergedData] = mergeService
//    .mergeAccidentsWithWeatherAndDistricts[Accident, MergedData](raws, mergeService.mapper).cache()
//  println("Merged data size: " + mergeData.count())

  val populationParser = injector.getInstance(classOf[PopulationParser])
  val populationStorage = new PopulationStorage(populationParser)
  println("WTF?" + populationStorage.populationMap.size.toString)


}
