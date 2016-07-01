package com.test.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Created by s on 16-4-22.
 */
object JoinTest {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Join test")
    sc.setLogLevel("WARN")


    val edgeRdd = sc.parallelize(List("1,2", "2,3", "3,1", "1,4")).map{
      case lines =>
        val ss = lines.split(",")
        (ss(0).toLong, ss(1).toLong)
    }
//    val graph = GraphLoader.edgeListFile(sc, "")

    val graph = Graph.fromEdgeTuples(edgeRdd, "")

    val vertexRdd: RDD[(VertexId, String)] = sc.parallelize(List("1,Taro","2,Jiro", "5,xxxx")).map{
      case lines =>
        val ss = lines.split(",")
        (ss(0).toLong, ss(1))
    }

    graph.vertices.foreach(println)
    graph.edges.foreach(println)
//    graph.mapVertices((id, attr) => "xxxx").vertices.foreach(println)
    println(s"\njoin:")
    val a = graph.mapVertices((id, attr) => "balabala").joinVertices(vertexRdd){
      (vid, attr1, attr2) => s"$vid:$attr1:$attr2"
    }

    a.vertices.foreach(println)

    println(s"\njoin outDegrees:")
    val b = graph.mapVertices((_, _) => 0).joinVertices(graph.outDegrees){
      (_, _, outDeg) => outDeg
    }
    b.vertices.foreach(println)

    println(s"\nouterJoin:")
    val c = graph.mapVertices((_, _) => 0).outerJoinVertices(vertexRdd){
      (_, _, xx) => xx.getOrElse("None")
    }
    c.vertices.foreach(println)


    println(s"\n -----------join----------")
    graph.joinVertices(vertexRdd){
      (_, _, x) => x
    }.vertices.foreach(println)

    println(s"\n ----------outer join--------")
    graph.outerJoinVertices(vertexRdd){
      (_, _, x) => x
    }.vertices.foreach(println)

    sc.stop()
  }
}
