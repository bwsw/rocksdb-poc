package com.bwsw.rocksDBPoC

package object util {
  import java.io._
  def delete(file: File) {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete)
    file.delete
  }
}
