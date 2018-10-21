package com.epam.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * start point*/
object Main extends App {

  println(args.mkString("\n"))

  //resolve input argument
  val (master: String, file: String) = args.toList match {
    case firstArg :: secondArg :: Nil => (firstArg, secondArg)
    case firstArg :: Nil => (firstArg, "src\\main\\scala\\resourses\\countries_of_the_world.csv")
    case Nil => ("local", "src\\main\\scala\\resourses\\countries_of_the_world.csv")
  }

  private val sparkContext: SparkContext = SparkSession
    .builder()
    .appName("SparkSessionZipsExample")
    .master("local")
    .getOrCreate().sparkContext

  private val inputFile: RDD[String] = sparkContext.textFile(file)

  //get region, population and density from csv file
  private val csvFile: RDD[(String, Long, Double)] = Engine.getRDDRegionIPopultionIpopDensity(inputFile)

  //Show to console sum of population
  println("Population total: " + Engine.getPopulationSum(csvFile))

  //Show Population Density at region
  println("Regional Population Density:")
  Engine.getPopulationDensityAtRegions(csvFile).foreach((x: (String, Double)) => println("* " + x))

  Thread.sleep(10000)
}
