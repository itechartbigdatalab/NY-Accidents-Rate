package com.itechart.ny_accidents.entity

object AccidentsHeader {
  val date = "DATE"
  val time = "TIME"
  val latitude = "LATITUDE"
  val longitude = "LONGITUDE"

  val onStreet = "ON STREET NAME"
  val crossStreet = "CROSS STREET NAME"
  val offStreet = "OFF STREET NAME"
  val personsInjured = "NUMBER OF PERSONS INJURED"
  val personsKilled = "NUMBER OF PERSONS KILLED"
  val pedastriansInjured = "NUMBER OF PEDESTRIANS INJURED"
  val pedastriansKilled = "NUMBER OF PEDESTRIANS KILLED"
  val cyclistInjured = "NUMBER OF CYCLIST INJURED"
  val cyclistKilled = "NUMBER OF CYCLIST KILLED"
  val motoristInjured = "NUMBER OF MOTORIST INJURED"
  val motoristKilled = "NUMBER OF MOTORIST KILLED"
  val contributingFactors = Array("CONTRIBUTING FACTOR VEHICLE 1","CONTRIBUTING FACTOR VEHICLE 2","CONTRIBUTING FACTOR VEHICLE 3","CONTRIBUTING FACTOR VEHICLE 4","CONTRIBUTING FACTOR VEHICLE 5")
  val vehicleType =Array( "VEHICLE TYPE CODE 1","VEHICLE TYPE CODE 2","VEHICLE TYPE CODE 3","VEHICLE TYPE CODE 4","VEHICLE TYPE CODE 5")
}
