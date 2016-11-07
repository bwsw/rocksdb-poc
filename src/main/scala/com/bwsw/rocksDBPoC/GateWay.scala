package com.bwsw.rocksDBPoC

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import org.rocksdb.{Options, RocksDB}
import scala.collection.mutable.ArrayBuffer

/**
  * Wraps "low"-level RocksDB-access Java interface to more Scala-like DAO.
  * Key is always just a triple of ints
  * @param path Path to DB
  * @tparam T Type of values to map from keys in DB
  */
class GateWay[T <: java.io.Serializable](path: String) {
  GateWay //See companion object for explanations

  type Key = (Int, Int, Int)
  type Prefix = (Int, Int)

  /*
  Used for putting values in DB
   */
  private def serialize(value: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.close()
    baos.toByteArray
  }

  /*
  Used for getting values from DB
   */
  private def deserialize(value: Array[Byte]): T = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(value))
    val result = ois.readObject()
    ois.close()
    result.asInstanceOf[T]
  }
//
  private def normalize(bytes: Array[Byte]): Array[Byte] = bytes match {
    case Array(a) => Array[Byte](0, 0, 0, a)
    case Array(a, b) => Array[Byte](0, 0, a, b)
    case Array(a, b, c) => Array[Byte](0, a, b, c)
    case Array(a, b, c, d) => bytes
  }

  private def keyToBytes(key: Key) : Array[Byte] = Array(
    normalize(BigInt(key._1).toByteArray),
    normalize(BigInt(key._2).toByteArray),
    normalize(BigInt(key._3).toByteArray)
  ).flatten

  private def prefixToBytes(prefix: Prefix) : Array[Byte] = Array(
    normalize(BigInt(prefix._1).toByteArray),
    normalize(BigInt(prefix._2).toByteArray)
  ).flatten

  private def bytesToKey(bytes: Array[Byte]): Key =
    (ByteBuffer.wrap(bytes.take(4)).getInt,
      ByteBuffer.wrap(bytes.slice(4, 8)).getInt,
      ByteBuffer.wrap(bytes.slice(8, 12)).getInt)

  val options = new Options()
    .setCreateIfMissing(true)
    .useFixedLengthPrefixExtractor(8)

  val db = RocksDB.open(options, path)

  /**
    * Get value from DB by key
    * @return Some(T) if OK, None if value was not found
    */
  def get(key: Key): Option[T] = Option(db.get(keyToBytes(key))).map(deserialize)

  /**
    * Get all values from DB which correspond to key prefix
    * @param prefix Is an Int-pair prefix
    * @return Sequence of all appropriate key/value pairs
    */
  def getAll(prefix: Prefix): Seq[(Key, T)] = {
    val iter = db.newIterator()
    val results: ArrayBuffer[(Key, T)] = ArrayBuffer()
    val criteria = prefixToBytes(prefix)
    iter.seek(criteria)
    while (iter.isValid && iter.key.startsWith(criteria)) {
      results += ((bytesToKey(iter.key), deserialize(iter.value)))
      iter.next()
    }
    results
  }

  /**
    * Create or update value by key
    */
  def put(key: Key, value: T) = db.put(keyToBytes(key), serialize(value))

  /**
    * Delete key/value pair from DB by key
    */
  def delete(key: Key) = db.remove(keyToBytes(key))

  /**
    * Release all C++ resources
    * see https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#closing-a-database
    */
  override def finalize() = {
    super.finalize()
    db.close()
    options.close()
  }

}

/**
  * Loading of C++ library after first GateWay object instantiation.
  * In RockDB's sample they use Java's static constructor for this
  * see https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/RocksDBSample.java#L19
  */
object GateWay {
  RocksDB.loadLibrary()
}
