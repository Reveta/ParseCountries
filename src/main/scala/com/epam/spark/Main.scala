package com.epam.spark

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * start point*/
object Main extends App with Engine {

  println(args.mkString("\n"))

  //resolve input argument
  val (master: String, file: String, saveFile: String) = args.toList match {
    case firstArg :: secondArg :: thirdArg :: Nil => (firstArg, secondArg, thirdArg)
    case firstArg :: Nil => (firstArg, "src/main/resourses/countries_of_the_world.csv", "./" + new Date().getTime)
    case Nil => ("local", "src/main/resourses/countries_of_the_world.csv", "./" + new Date().getTime)
  }

  private val sparkContext: SparkContext = SparkSession
    .builder()
    .appName("SparkSessionZipsExample")
    .master(master)
    .getOrCreate().sparkContext

  private val inputRDD: RDD[String] = sparkContext.textFile(file)

  private val noHeaderRDD: RDD[String] = inputRDD.filter(!isHeaderCsv(_))

  //get region, population and density from csv file
  private val parsedRDD: RDD[(String, Long, Double)] = noHeaderRDD.map(getCaseRegionIPopultionIpopDensity)

  //Show to console sum of population
  //println("Population total: " + getPopulationSum(parsedRDD))

  //Show Population Density at region
  println("Regional Population Density:")
  val regionsRDD: RDD[(String, Double)] = parsedRDD.map { case (x1, x2, x3) => (clearKey(x1), x3)
  }.reduceByKey(_ + _)

  val resultStringRDD: RDD[String] = regionsRDD.map(_.toString)

  //resultStringRDD.saveAsTextFile(saveFile)
  resultStringRDD.collect().foreach(println)

  // while (true) {
  //   Thread.sleep(10000)
  // }
}
