package com.itechart.ny_accidents.utils

import info.debatty.java.stringsimilarity.NormalizedLevenshtein

import scala.util.{Failure, Success, Try}

object StringUtils {
  private val levenshtein = new NormalizedLevenshtein()

  def windStringToDoubleParse(windStr: String): Option[Double] = {
    Try(windStr.filter(_.isDigit).mkString("").toDouble) match {
      case Success(value) => Some(value)
      case Failure(_) => None
    }
  }

  def getLineMatchPercentage(firstString: String, secondString: String): Double = {
    levenshtein.similarity(firstString, secondString)
  }
}
