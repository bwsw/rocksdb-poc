package com.bwsw.rocksDBPoC

import java.io.File
import com.bwsw.rocksDBPoC.util.{delete, time}

object Benchmark extends App {

  if (args.length < 3) {
    println("Usage: \"sbt\" \"run <DBPath> <N> <M>\"")
    println("Where DBPath is a path to test db (will create if missing). N is a number of initial dummy records to put in DB. M of them will be created with same int pair prefix")
    System.exit(1)
  }

  val (dbPath, n, m) = (args(0), args(1).toInt, args(2).toInt)
  val db = GateWay[String](dbPath)
  val prefix = (1, 1)

  println("Filling database...")
  val (_, fTaken) = time {

    for (i <- 1 to n - m) {
      db.put((i, i, i), s"value#$i, ${i + 1}, ${i + 2}")
    }

    for (i <- 1 to m) {
      db.put((prefix._1, prefix._2, i), s"prefixed with $prefix value#$i")
    }

  }
  println(s"$fTaken ms taken to fill database")

  println(s"Searching for $prefix prefixed values...")
  val (values, sTaken) = time { db.getAll(prefix) }
  println(s"Got ${values.length} values in $sTaken ms")

  println(s"Deleting database $dbPath")
  delete(new File(dbPath))
}
