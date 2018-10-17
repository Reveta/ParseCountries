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

//  @Ignore
  @Test
  def getPopulationDensityAtRegionsTest(): Unit = {
    val expected: RDD[(String, Double)] = getTestRDD().map {
      case (x1, x2, x3) => (clearKey(x1), x3)
    }.reduceByKey(_ + _)

    assertEquals(
      Engine.getPopulationDensityAtRegions(getTestRDD()).count(),
      expected.count()
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

  @Test
  def getCaseRegionIPopultionIpopDensityTest(): Unit = {
    val input = "Afghanistan ,ASIA (EX. NEAR EAST)         ,31056997,647500,\"48,0\",\"0,00\",\"23,06\",\"163,07\",700,\"36,0\",\"3,2\",\"12,13\",\"0,22\",\"87,65\",1,\"46,6\",\"20,34\",\"0,38\",\"0,24\",\"0,38\""
    val expected = "(ASIA (EX. NEAR EAST)         ,31056997,48.0)"
    val testData: (String, Long, Double) = Engine.getCaseRegionIPopultionIpopDensity(input)

    assertEquals(testData._1, "ASIA (EX. NEAR EAST)         ")
    assertEquals(testData._2, 31056997l)
//    assertEquals(testData._3, 48.0)

    assertEquals(
      testData.toString(),
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
