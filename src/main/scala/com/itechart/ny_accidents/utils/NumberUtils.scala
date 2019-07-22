package com.itechart.ny_accidents.utils

import scala.math.BigDecimal

object NumberUtils {
  def truncateDouble(value:Double): Double ={
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

}

