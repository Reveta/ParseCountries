package com.epam.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Main {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("simpleReading").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val inputFile: RDD[String] = sc.textFile("src\\main\\scala\\resourses\\countries_of_the_world.csv")

    //get region, population and density from csv file
    val csvFile: RDD[(String, Long, Double)] = getRegionIPopultionIpopDensity(inputFile)

    //Show to console sum of population
    println("Population total: " + getPopulationSum(csvFile))

    //Show Population Density at region
    println("Regional Population Density:")
    getPopulationDensityAtRegions(csvFile).foreach(x => println("* " + x))
  }

  def getRegionIPopultionIpopDensity(rddCsv: RDD[String]): RDD[(String, Long, Double)] = {
    val header: String = rddCsv.first()

    val populationIndex: Int = 2
    val regionIndex: Int = 1
    val popDensityIntegerIndex: Int = 4
    val popDensityFractionalIndex: Int = 5

    rddCsv.filter(_ != header).map(line => { //ignore of first row that contains column names
      val colAr: Array[String] = line.split(",") //get array with columns
      (
        colAr(regionIndex),

        colAr(populationIndex)//string to long
          .toLong,

        colAr(popDensityIntegerIndex)
          .concat("." + colAr(popDensityFractionalIndex)) //e.g.: "528', '8" -> "528.8"
          .replace("\"", "").toDouble //e.g.: "528.8" -> 528.8
      )
    })
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
          x3)
      }
        .reduceByKey(_ + _)
    }

    private def clearKey(key: String): String = {
      //fix problem with input "region" data
      //e.g.: "ASIA (EX. NEAR EAST)         " to "ASIA (EX. NEAR EAST)"
      return key.replace("\"", "").trim()
    }

  }
