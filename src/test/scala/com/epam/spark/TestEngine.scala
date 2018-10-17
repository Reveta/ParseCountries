package scala.com.epam.spark


import com.epam.spark.Engine
import com.epam.spark.Engine.clearKey
import org.apache.spark.rdd.RDD
import org.junit.Assert.assertEquals
import org.junit.{Ignore, Test}

class TestEngine {

  @Test
  def clearKeyTest(): Unit = {
    val input = "ASIA (EX. NEAR EAST)         "
    val expected: String = "ASIA (EX. NEAR EAST)"

    assertEquals(
      Engine.clearKey(input),
      expected
    )
  }

  @Ignore
  @Test
  def getPopulationDensityAtRegionsTest(): Unit = {
    val expected: RDD[(String, Double)] = getTestRDD().map {
      case (x1, x2, x3) => (clearKey(x1), x3) }.reduceByKey(_ + _)

    assertEquals(
      Engine.getPopulationDensityAtRegions(getTestRDD()),
      getTestRDD().map { case (x1, x2, x3) => (clearKey(x1), x3) }.reduceByKey(_ + _)
    )
  }

  @Test
  def getPopulationSumTest(): Unit = {
    val expected: Long = getTestRDD()
      .map(_._2)
      .reduce(_ + _)

    assertEquals(
      Engine.getPopulationSum(getTestRDD()),
      expected
    )
  }

  private def getTestRDD(): RDD[(String, Long, Double)] = {
    val inputFile: RDD[String] = Engine.sparkContext
      .textFile("src\\test\\scala\\resorses\\countries_of_the_world.csv")

    val header: String = inputFile.first()

    val populationIndex: Int = 2
    val regionIndex: Int = 1
    val popDensityIntegerIndex: Int = 4
    val popDensityFractionalIndex: Int = 5

    inputFile.filter(_ != header).map(line => { //ignore of first row that contains column names
      val colAr: Array[String] = line.split(",") //get array with columns
      (
        colAr(regionIndex),

        colAr(populationIndex) //string to long
          .toLong,

        colAr(popDensityIntegerIndex)
          .concat("." + colAr(popDensityFractionalIndex)) //e.g.: "528', '8" -> "528.8"
          .replace("\"", "").toDouble //e.g.: "528.8" -> 528.8
      )
    })
  }
}
