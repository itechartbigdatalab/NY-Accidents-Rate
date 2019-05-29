package com.itechart.ny_accidents

import com.google.inject.Guice
import com.itechart.ny_accidents.constants.Configuration
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.utils.FileWriterUtils
import com.itechart.ny_accidents.constants.Configuration._

object DatasetSplitApplication extends App {
  val injector = Guice.createInjector(new GuiceModule)
  val accidentsParser = injector.getInstance(classOf[AccidentsParser])

  accidentsParser.splitDatasetByYear(PATH_TO_DATASET_TO_SPLIT) match {case (schema, datasets) => {
    datasets.foreach{case (key, value) =>
      FileWriterUtils.createNewCsv(
      s"$SPLIT_DATASET_BASE_FOLDER/${CSV_BASE_NAME}_$key.csv",
      value, schema)
    }
  }}
}
