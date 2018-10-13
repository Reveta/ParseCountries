package com.epam.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Main {
  def main(args: Array[String]): Unit = {
    val engine: Engine.type = Engine

    val conf: SparkConf = new SparkConf().setAppName("simpleReading").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val inputFile: RDD[String] = sc.textFile("src\\main\\scala\\resourses\\countries_of_the_world.csv")

    //get region, population and density from csv file
    val csvFile: RDD[(String, Long, Double)] = engine.getRegionIPopultionIpopDensity(inputFile)

    //Show to console sum of population
    println("Population total: " + engine.getPopulationSum(csvFile))

    //Show Population Density at region
    println("Regional Population Density:")
    engine.getPopulationDensityAtRegions(csvFile).foreach((x: (String, Double)) => println("* " + x))
//    engine.getPopulationDensityAtRegions(csvFile).foreach(x => println("* " + x))
//    engine.getPopulationDensityAtRegions(csvFile).foreach(println)
  }
}
