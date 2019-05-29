package com.itechart.ny_accidents.database.dao

import com.google.inject.{Inject, Singleton}
import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.entity.Population
import com.itechart.ny_accidents.parse.PopulationParser

@Singleton
class PopulationStorage @Inject()(populationParser: PopulationParser) {
  val populationMap: Map[Int, Population] = populationParser.readData(Configuration.POPULATION_FILE_PATH)
}
