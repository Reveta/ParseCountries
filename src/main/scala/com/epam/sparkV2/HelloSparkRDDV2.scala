package com.epam.sparkV2


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}


object HelloSparkRDDV2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("simpleReading").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val distFile: RDD[String] = sc.textFile("src\\main\\scala\\resourses\\countries_of_the_world.csv")
    val header = distFile.first()

    val populationIndex: Int = 2
    val regionIndex: Int = 1
    val popDensityIntegerIndex: Int = 4
    val popDensityFractionalIndex: Int = 5

    val csvFile: RDD[(String, String, Double)] = distFile.filter(_ != header).map(line => {
      val colAr = line.split(",")
      (colAr(regionIndex), colAr(populationIndex), colAr(popDensityIntegerIndex).concat("." + colAr(popDensityFractionalIndex)).replace("\"", "").toDouble)
    })


    //Show to console sum of population
    val population: RDD[String] = csvFile.map(_._2)
    val popSum: RDD[Long] = population.map(_.toLong)
    println("Population total: " + popSum.reduce(_ + _))


    //Show Population Density at region
    val popDensityByRegion: RDD[(String, Double)] = csvFile.map { case (x1, x2, x3) => (x1, x3) }.reduceByKey(_ + _)
    popDensityByRegion.foreach(println)
  }

}
