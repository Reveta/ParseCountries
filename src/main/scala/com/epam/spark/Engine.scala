package com.epam.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * object with main logic
  * inner object sparkContext*/
object Engine {

  val sparkContext: SparkContext = new SparkContext(
    new SparkConf()
      .setAppName("simpleReading")
      .setMaster("local[*]"))

  private val indexes: scala.collection.immutable.Map[String, Integer] = Map(
    "populationIndex" -> 2,
    "regionIndex" -> 1,
    "popDensityIntegerIndex" -> 4,
    "popDensityFractionalIndex" -> 5
  )

  def getRDDRegionIPopultionIpopDensity(rddCsv: RDD[String]): RDD[(String, Long, Double)] = {
    val header: String = rddCsv.first()

    return rddCsv
      .filter((_: String) != header) //ignore of first row that contains column names
      .map((line: String) => getCaseRegionIPopultionIpopDensity(line))
  }

  def getCaseRegionIPopultionIpopDensity(line: String): (String, Long, Double) = {
//    println(line)
    val colAr: Array[String] = line.split(",") //get array with columns
    val regionIPopultionIpopDensity: (String, Long, Double) = (
      colAr(indexes("regionIndex")),

      colAr(indexes("populationIndex")) //string to long
        .toLong,

      colAr(indexes("popDensityIntegerIndex"))
        .concat("." + colAr(indexes("popDensityFractionalIndex"))) //e.g.: "528', '8" -> "528.8"
        .replace("\"", "").toDouble //e.g.: "528.8" -> 528.8
    )

//    println(regionIPopultionIpopDensity)
    return regionIPopultionIpopDensity
  }

  def getPopulationSum(rdd: RDD[(String, Long, Double)]): Long = {
    //get population(_2) from RDD and summarize them by reduce
    return rdd.map(_._2).reduce(_ + _)
  }


  def getPopulationDensityAtRegions(rdd: RDD[(String, Long, Double)]): RDD[(String, Double)] = {
    //get region(x1) and density(x3) and summarize density by key, where key is region
    return rdd.map {
      case (x1, x2, x3) => (
        clearKey(x1),
        x3
      )
    }
      .reduceByKey(_ + _)
  }

   def clearKey(key: String): String = {
    //fix problem with input "region" data
    //e.g.: "ASIA (EX. NEAR EAST)         " to "ASIA (EX. NEAR EAST)"
    return key.replace("\"", "").trim()
  }

}
