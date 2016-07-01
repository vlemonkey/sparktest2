package com.test.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Created by s on 16-5-3.
 */
object VDEDTest {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "VDED Test")
    sc.setLogLevel("WARN")

    val setA: VertexRDD[Int] = VertexRDD(sc.parallelize(0L until 5L).map(id => (id, 1)))
    val rddB: RDD[(VertexId, Double)] = sc.parallelize(0L until 5L)
      .flatMap(id => List((id, 1.0), (id, 2.0)))

    rddB.foreach(println)
    println(s"rddB count:${rddB.count()}")

    val setB: VertexRDD[Double] = setA.aggregateUsingIndex(rddB, _+_)
    setB.foreach(println)
  }

}
