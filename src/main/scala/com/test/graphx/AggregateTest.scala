package com.test.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

/**
 * Created by s on 16-4-22.
 */
object AggregateTest {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "aggregateMessages test")
    sc.setLogLevel("WARN")

    val graph = GraphGenerators.logNormalGraph(sc, numVertices = 5)
      .mapVertices((id, _) => id.toDouble)
    println(s"\nrandom graph:")
    graph.vertices.foreach(println)
    graph.edges.foreach(println)

    println(s"\noder:")
    val older: VertexRDD[(Int, Double, Int)] = graph.aggregateMessages[(Int, Double, Int)](
      triplet => {
        println(s"triplet.srcAttr:${triplet.srcAttr} trpilet.dstAttr:${triplet.dstAttr}")
        if(triplet.srcAttr > triplet.dstAttr) {
//          triplet.sendToDst(1, triplet.srcAttr, 0)
          triplet.sendToSrc(1, triplet.srcAttr, 0)
        }
      },
      (a, b) => {
        println(s"a._1:${a._1} -- a._2:${a._2}")
        println(s"b._1:${b._1} -- b._2:${b._2}")
        (a._1 + b._1, a._2 + b._2, a._3 + b._3)
      }
    )

    println(s"\noder collect:")
    older.foreach(println)

    println(s"\n\n------------------mapRduceTriplets--------------")

    def msgFun(triplet: EdgeTriplet[Double, Int]): Iterator[(VertexId, String)] = {
      Iterator((triplet.dstId, "Hi"))
    }
    def reduceFun(a: String, b:String): String = s"$a  $b"
    val result = graph.mapReduceTriplets[String](msgFun, reduceFun)
    result.foreach(println)

    // 上面代码等同于
    def msgFun2(triplet: EdgeContext[Double, Int, String]){
      triplet.sendToDst("Hi")
    }
    def reduceFun2(a: String, b: String) = s"$a  $b"
    val result2 = graph.aggregateMessages(msgFun2, reduceFun2)
    println(s"等同于")
    result2.foreach(println)


    // computing degree
    println("\ncomputing degree")
    def max(a:(VertexId, Int), b:(VertexId, Int)):(VertexId, Int) = {
      if(a._2 > b._2) a else b
    }
    val maxInDegree = graph.inDegrees.reduce(max)
    val maxOutDegree = graph.outDegrees.reduce(max)
    val maxDegree = graph.degrees.reduce(max)
    println(s"maxInDegree:$maxInDegree")
    println(s"maxOutDegree:$maxOutDegree")
    println(s"maxDegree:$maxDegree")


    // collecting Neighbors
    println(s"\ncollecting Neighbors")
    graph.collectNeighborIds(EdgeDirection.In).foreach{
      x => println(s"x:${x._1}");x._2.foreach(println)
    }

    graph.collectNeighbors(EdgeDirection.In).foreach{
      x => println(s"x:${x._1}");x._2.foreach(println)
    }
  }

}
