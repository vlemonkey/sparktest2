package com.test.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

/**
  * Created by s on 16-4-26.
  */
object PregelAF {
   def main(args: Array[String]) {
     val sc = new SparkContext("local", "Pregel Test")
     sc.setLogLevel("WARN")

     val graph = GraphGenerators.logNormalGraph(sc, numVertices = 10)
       .mapEdges(e => e.attr.toDouble)
     println(s"\ngraph vertices:")
     graph.vertices.foreach(println)
     println(s"\ngraph edges:")
     graph.edges.foreach(println)
     println(s"\ngraph triplet:")
     graph.triplets.foreach(println)

     val sourceId: VertexId = 2
     val endId: VertexId = 4
     val initalGraph = graph.mapVertices((id, _) => if(id == sourceId) sourceId.toString else "")
     println(s"\ninitalGraph vertices:")
     initalGraph.vertices.foreach(println)
     println(s"\ninitalGraph edges:")
     initalGraph.edges.foreach(println)
     println(s"\ninitalGraph triplets:")
     initalGraph.triplets.foreach(println)
     println(List(1,2).mkString(end="}", sep=",", start="{"))

     val sssp = initalGraph.pregel(initialMsg="",
                                   maxIterations=4,
                                   activeDirection=EdgeDirection.Out)(
       (id, dist, newDist) => {
         println(s"id:dist:newDist -- $id:$dist:$newDist")
         s"$dist $newDist,"
       },
       triplet => {
 //        println(s"\n${triplet.srcId} srcAttr:${triplet.srcAttr}  attr:${triplet.attr}  dstAttr:${triplet.dstAttr}")
         println(s"triplets:${triplet}")
         if(triplet.dstAttr != endId){
           Iterator((triplet.dstId, triplet.dstAttr))
         }else {
           Iterator.empty
         }
       },
       (a, b) => {println(s""); a}
     )
     println(s"\nsssp:")
    println(sssp.vertices.collect.mkString("\n"))
 //    println(sssp.edges.collect.mkString("\n"))
   }
 }