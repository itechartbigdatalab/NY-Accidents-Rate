package com.itechart.ny_accidents

import com.itechart.ny_accidents.constants.Configuration._
import com.itechart.ny_accidents.parse.AccidentsParser
import com.itechart.ny_accidents.utils.FileWriterUtils

object DatasetSplitApplication extends App {

  AccidentsParser.splitDatasetByYear(PATH_TO_DATASET_TO_SPLIT) match {case (schema, datasets) =>
    datasets.foreach{case (key, value) =>
      FileWriterUtils.createNewCsv(
      s"$SPLIT_DATASET_BASE_FOLDER/${CSV_BASE_NAME}_$key.csv",
      value, schema)
    }
  }
}
