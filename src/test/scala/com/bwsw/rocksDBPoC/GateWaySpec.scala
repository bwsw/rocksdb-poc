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

  val gw = new GateWay[String](dbPath)

  val key1 = (2, 2, 1)
  val value1 = "SampleValue1"

  val key2 = (Int.MaxValue, 2, 2)
  val value2 = "SampleValue2"

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

  "GateWay#delete" should "remove appropriate value from DB" in {
    gw.put(key1, value1)
    gw.put(key2, value2)
    gw.delete(key1)
    gw.delete(key2)
    assert(gw.get(key1).isEmpty)
    assert(gw.get(key2).isEmpty)
  }

}
