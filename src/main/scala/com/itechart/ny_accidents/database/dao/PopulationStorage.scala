package com.itechart.ny_accidents.database.dao

import com.google.inject.{Inject, Singleton}
import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.entity.Population
import com.itechart.ny_accidents.parse.PopulationParser

object PopulationStorage {
  lazy val populationMap: Map[Int, Population] = PopulationParser.readData(Configuration.POPULATION_FILE_PATH)
}
