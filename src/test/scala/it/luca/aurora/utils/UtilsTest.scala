package it.luca.aurora.utils

import Utils.insertElementAtIndex
import org.apache.log4j.Logger
import org.scalatest.FunSuite

class UtilsTest extends FunSuite {

  private final val logger = Logger.getLogger(getClass)

  test("testInsertElementAtIndex") {

    val s: Seq[Int] = Seq(0, 1, 3)
    val insertIndex: Int = 2
    val (beforeElement, afterElement): (Int, Int) = (s(insertIndex - 1), s(insertIndex))
    val sPlusElement: Seq[Int] = insertElementAtIndex(s, 2, insertIndex)

    // ASSERTIONS
    assert(sPlusElement.length == (s.length + 1))
    assertResult(2)(sPlusElement(insertIndex))
    assertResult(beforeElement)(sPlusElement(insertIndex - 1))
    assertResult(afterElement)(sPlusElement(insertIndex + 1))

    logger.info(s"New Seq: ${sPlusElement.mkString(", ")}")
  }
}
