package com.itechart.ny_accidents.utils

object NumberUtils {
  def validateDouble(number: Double): Option[Double] = {
    if(number == Double.PositiveInfinity || number == Double.NegativeInfinity){
      None
    } else {
      Some(number)
    }
  }
}
