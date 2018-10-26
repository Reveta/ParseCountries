package scala.com.epam.spark

import org.junit.Assert.assertEquals
import org.junit.Test

class TestEngine {

  @Test
  def clearKeyTest(): Unit = {
    val input = "ASIA (EX. NEAR EAST)         "

    val actualResult = new Engine() {}.clearKey(input)

    val expected: String = "ASIA (EX. NEAR EAST)"
    assertEquals(actualResult, expected)
  }

  @Test
  def getCaseRegionIPopultionIpopDensityTest(): Unit = {
    val input = "Afghanistan ,ASIA (EX. NEAR EAST)         ,31056997,647500,\"48,0\",\"0,00\",\"23,06\",\"163,07\",700,\"36,0\",\"3,2\",\"12,13\",\"0,22\",\"87,65\",1,\"46,6\",\"20,34\",\"0,38\",\"0,24\",\"0,38\""

    val actualResult: (String, Long, Double) = new Engine() {}.getCaseRegionIPopultionIpopDensity(input)

    val expected = "(ASIA (EX. NEAR EAST)         ,31056997,48.0)"
    assertEquals(actualResult._1, "ASIA (EX. NEAR EAST)         ")
    assertEquals(actualResult._2, 31056997l)
    //    assertEquals(testData._3, 48.0)

    assertEquals(actualResult.toString(), expected
    )
  }

  @Test
  def isHeaderCsv(): Unit = {
    val input = "ASIA (EX. NEAR EAST)         "

    val actualResult = new Engine() {}.isHeaderCsv(input)

    assertEquals(actualResult, false)
  }


}
