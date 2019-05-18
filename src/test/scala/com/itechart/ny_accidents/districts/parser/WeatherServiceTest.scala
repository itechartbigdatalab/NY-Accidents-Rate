package com.itechart.ny_accidents.districts.parser

import com.itechart.ny_accidents.service.WeatherMappingService
import org.scalatest.FunSpec

class WeatherServiceTest extends FunSpec {

  describe("WeatherService") {
    it("should return correct pressure") {
      val resultValue = 757.4
      val service = new WeatherMappingService()
      val returnedValue = service.findWeatherByTimeAndCoordinates(1557862500000L, 40.597415, -74.164599)

      assert(returnedValue.isDefined && returnedValue.get.pressure == resultValue)
    }
  }

}
