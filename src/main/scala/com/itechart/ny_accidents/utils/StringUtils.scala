package com.itechart.ny_accidents.utils

import scala.collection.immutable
import scala.util.{Failure, Success, Try}

object StringUtils {

  def windStringToDoubleParse(windStr: String): Option[Double] = {
    Try(windStr.filter(_.isDigit).mkString("").toDouble) match {
      case Success(value) => Some(value)
      case Failure(_) => None
    }
  }

  def getLineMatchPercentage(firstString: String, secondString: String): Double = {
    val coincidenceNumber: Int = firstString.map(char => {
      secondString.contains(char) match {
        case true => 1
        case false => 0
      }
    }).sum

    coincidenceNumber.toDouble /
      (secondString.length.toDouble + secondString.length.toDouble
        - coincidenceNumber.toDouble) * 100
  }
}
