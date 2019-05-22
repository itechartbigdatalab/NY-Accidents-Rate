package com.itechart.ny_accidents

import com.google.inject.Guice
import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.database.DistrictsStorage
import com.itechart.ny_accidents.parse.PopulationParser
import com.itechart.ny_accidents.service.DistrictsService
import com.itechart.ny_accidents.utils.StringUtils

object TestMain extends App {
  val injector = Guice.createInjector(new GuiceModule)
  val districtsService = injector.getInstance(classOf[DistrictsService])
  val districtsStorage = injector.getInstance(classOf[DistrictsStorage])


  val populationParser = new PopulationParser(districtsService, districtsStorage)
  populationParser.readData(Configuration.POPULATION_DATASET_PATH)

//  val s1 = "DUMBO-VINEGAR HILL-DOWNTOWN BRKLYN-BOERUM HILL"
//  val s2 = "Mariner's Harbor-Arlington-Port Ivory-Graniteville"
//  println(StringUtils.getLineMatchPercentage(s1.toUpperCase(), s2.toUpperCase()))
}
