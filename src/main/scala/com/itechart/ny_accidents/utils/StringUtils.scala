package com.itechart.ny_accidents.utils

import scala.util.{Failure, Success, Try}

object StringUtils {

  def windStringToDoubleParse(windStr: String): Option[Double] = {
    Try(windStr.filter(_.isDigit).mkString("").toDouble) match {
      case Success(value) => Some(value)
      case Failure(_) => None
    }
  }

}
