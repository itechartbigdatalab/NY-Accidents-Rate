package com.itechart.accidents.integration.ny.database.dao

import com.google.inject.{Inject, Singleton}
import com.itechart.accidents.constants.Configuration
import com.itechart.accidents.entity.Population
import com.itechart.accidents.integration.ny.parse.NYPopulationParser

@Singleton
class PopulationStorage @Inject()(populationParser: NYPopulationParser) {
  val populationMap: Map[Int, Population] = populationParser.readData(Configuration.POPULATION_FILE_PATH)
}
