package com.itechart.ny_accidents

import com.google.inject.Guice
import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.utils.FileWriterUtils

object DatasetSplitApplication extends App {
  val injector = Guice.createInjector(new GuiceModule)
  val accidentsParser = injector.getInstance(classOf[AccidentsParser])

  val (schema, datasets) = accidentsParser.splitDatasetByYear(Configuration.PATH_TO_DATASET_TO_SPLIT)
  datasets.foreach{case (key, value) =>
    FileWriterUtils.createNewCsv(
      s"${Configuration.SPLIT_DATASET_BASE_FOLDER}/${Configuration.CSV_BASE_NAME}_$key.csv",
      value,
      schema)}
}
