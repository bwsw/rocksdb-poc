package com.bwsw.rocksDBPoC

import java.io.File

import com.bwsw.rocksDBPoC.util.delete
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec}

class GateWaySpec extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll {

  val dbPath = "testdb"

  override def afterAll() = {
    super.afterAll()
    delete(new File(dbPath))
  }

  after {
    gw.delete(key1)
    gw.delete(key2)
  }

  val gw = GateWay[String](dbPath)

  val (key1, value1) = ((1, 1, 1), "SampleValue1")
  val (key2, value2) = ((1, 1, 2), "SampleValue2")

  "GateWay#get" should "return appropriate value" in {
    gw.put(key1, value1)
    gw.put(key2, value2)
    assert(gw.get(key1).contains(value1))
    assert(gw.get(key2).contains(value2))
  }

  it should "return None" in {
    assert(gw.get(key1).isEmpty)
    assert(gw.get(key1).isEmpty)
  }

  "GateWay#put" should "update appropriate value" in {
    val newValue1 = "NewValue1"
    val newValue2 = "NewValue2"
    gw.put(key1, value1)
    gw.put(key2, value2)
    gw.put(key1, newValue1)
    gw.put(key2, newValue2)
    assert(gw.get(key1).contains(newValue1))
    assert(gw.get(key2).contains(newValue2))
  }

  "GateWay#getAll" should "get all appropriate values from DB" in {
    gw.put(key1, value1)
    gw.put(key2, value2)
    gw.put((1, 2, 1), "BadValue1")
    gw.put((2, 1, 1), "BadValue2")
    gw.put((Int.MaxValue, 600, 600), "MaxValue")

    val (key3, value3) = ((1, 1, 3), "SampleValue3")
    gw.put(key3, value3)
    assert(gw.getAll((key1._1, key1._2)) == Seq((key1, value1), (key2, value2), (key3, value3)))
  }

  "GateWay#delete" should "remove appropriate value from DB" in {
    gw.put(key1, value1)
    gw.put(key2, value2)
    gw.delete(key1)
    gw.delete(key2)
    assert(gw.get(key1).isEmpty)
    assert(gw.get(key2).isEmpty)
  }

}
