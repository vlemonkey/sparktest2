package com.test

import org.apache.spark.SparkContext

/**
 * Created by s 
 *  16-5-30.
 */
object InvertedIndex {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "invertedindex test")
    sc.setLogLevel("WARN")

    val spark = sc.parallelize(List("id1\thello wold ni hao a wo hen hao", "id2\thello wo hao ni ye hao", "id3\tni hao wo hao da jia hao"))
    val words = spark.flatMap { item =>
      val Array(id, content) = item.split("\t")
      content.split(" ").map((_, id)).distinct
    }.reduceByKey(_ + "|" + _)

    words.foreach(println)
  }
}
