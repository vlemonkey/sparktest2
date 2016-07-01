package com.test.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

/**
  * Created by s on 16-4-26.
  */
object PregelTestEasy {
   def main(args: Array[String]) {
     val sc = new SparkContext("local", "Pregel Test")
     sc.setLogLevel("WARN")

     val graph = GraphGenerators.logNormalGraph(sc, numVertices = 5)
       .mapEdges(e => e.attr.toDouble)
     println(s"\ngraph vertices:")
     graph.vertices.foreach(println)
     println(s"\ngraph edges:")
     graph.edges.foreach(println)

     val sourceId: VertexId = 42
     val initalGraph: Graph[(Double), Double] = graph.mapVertices((id, _) =>
       if(id == sourceId) 0.0
       else Double.PositiveInfinity
     )
     println(s"\ninitalGraph vertices:")
     initalGraph.vertices.foreach(println)
     println(s"\ninitalGraph edges:")
     initalGraph.edges.foreach(println)

     val sssp = initalGraph.pregel(Double.PositiveInfinity)(
       (id, dist, newDist) => {
         println(s"\nid:$id dist:$dist newDist:$newDist")
         math.min(dist, newDist)
       },
       triplet => {
 //        println(s"\n${triplet.srcId} srcAttr:${triplet.srcAttr}  attr:${triplet.attr}  dstAttr:${triplet.dstAttr}")
         if(triplet.srcAttr + triplet.attr < triplet.dstAttr){
           Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
         }else {
           Iterator.empty
         }
       },
       (a, b) => math.min(a, b)
     )
     println(s"\nsssp:")
    println(sssp.vertices.collect.mkString("\n"))
   }
 }
