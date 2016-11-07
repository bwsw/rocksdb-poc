package com.bwsw.rocksDBPoC

package object util {

  /**
    * Deletes all files of parameter directory recursively
    */
  def delete(file: java.io.File) {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete)
    file.delete
  }

  /**
    * Evaluates block and measures how much time in ms did it take to finish
    * @param block Function to evaluate
    * @return Pair of result and time taken to accomplish it in ms
    */
  def time[R](block: => R): (R, Long) = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    (result, t1 - t0)
  }

}
