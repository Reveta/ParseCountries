package com.epam.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Main {
  def main(args: Array[String]): Unit = {

    val inputFile: RDD[String] = Engine.sparkContext
      .textFile("src\\main\\scala\\resourses\\countries_of_the_world.csv")

    //get region, population and density from csv file
    val csvFile: RDD[(String, Long, Double)] = Engine.getRegionIPopultionIpopDensity(inputFile)

    //Show to console sum of population
    println("Population total: " + Engine.getPopulationSum(csvFile))

    //Show Population Density at region
    println("Regional Population Density:")
    Engine.getPopulationDensityAtRegions(csvFile).foreach((x: (String, Double)) => println("* " + x))
  }

  def getSparkContext(): SparkContext = {
    return new SparkContext(new SparkConf().setAppName("simpleReading").setMaster("local[*]"))
  }

}
