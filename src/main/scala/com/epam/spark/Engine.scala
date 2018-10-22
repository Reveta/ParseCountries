package com.epam.spark


/**
  * object with main logic
  * inner object sparkContext*/
trait Engine {

  val REGION_INDEX = 1
  val POPULATION_INDEX = 2
  val POP_DENSITY_INTEGER_INDEX = 4
  val POP_DENSITY_FRACTIONAL_INDEX = 5

  def isHeaderCsv(line: String): Boolean = line.startsWith("Country,Region,Population,Area")

  def getCaseRegionIPopultionIpopDensity(line: String): (String, Long, Double) = {
    //    println(line)
    val colAr: Array[String] = line.split(",") //get array with columns
    val popDensity = colAr(POP_DENSITY_INTEGER_INDEX)
      .concat("." + colAr(POP_DENSITY_FRACTIONAL_INDEX)) //e.g.: "528', '8" -> "528.8"
      .replace("\"", "").toDouble //e.g.: "528.8" -> 528.8
    (colAr(REGION_INDEX), colAr(POPULATION_INDEX).toLong, popDensity)
  }

  def clearKey(key: String): String = {
    //fix problem with input "region" data
    //e.g.: "ASIA (EX. NEAR EAST)         " to "ASIA (EX. NEAR EAST)"
    key.replace("\"", "").trim()
  }

}
