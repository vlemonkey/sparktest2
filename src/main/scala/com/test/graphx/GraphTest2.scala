package com.test.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx._

/**
 * Created by s on 16-4-22.
 */
object GraphTest2 {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Graphx test2")
    sc.setLogLevel("WARN")
    val list = List("3,7",
                    "5,3",
                    "5,7",
                    "2,5")
    val rawEdgesRdd = sc.parallelize(list).map{
      case lines =>
        val ss = lines.split(",")
        val src = ss(0).toLong
        val dst = ss(1).toLong
        if(src < dst)
          (src, dst)
        else
          (dst, src)
    }.distinct()

    println(s"\nrawEdgesRdd:")
    rawEdgesRdd.foreach(println)

    val edgesGraph = Graph.fromEdgeTuples(rawEdgesRdd, "default prop")
    println(s"\n:edgesGraph vertices")
    edgesGraph.vertices.foreach(println)
    println(s"\n:edgesGraph edges")
    edgesGraph.edges.foreach(println)

  }

}
