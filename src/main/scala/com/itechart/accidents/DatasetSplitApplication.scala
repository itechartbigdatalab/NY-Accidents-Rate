package com.itechart.accidents

import com.itechart.accidents.constants.Configuration._
import com.itechart.accidents.integration.ny.parse.NYAccidentsParser
import com.itechart.accidents.utils.FileWriterUtils

object DatasetSplitApplication extends App {

  NYAccidentsParser.splitDatasetByYear(PATH_TO_DATASET_TO_SPLIT) match {case (schema, datasets) =>
    datasets.foreach{case (key, value) =>
      FileWriterUtils.createNewCsv(
      s"$SPLIT_DATASET_BASE_FOLDER/${CSV_BASE_NAME}_$key.csv",
      value, schema)
    }
  }
}
