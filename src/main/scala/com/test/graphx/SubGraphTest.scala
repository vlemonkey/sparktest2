package com.test.graphx

import org.apache.spark.graphx._
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by s on 16-4-19.
 */
object SubGraphTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("graphx subgraph test").setMaster("local")
    val sc   = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"),   Edge(5L, 0L, "colleague")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println)

    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    validGraph.edges.foreach(println)

    val maskGraph = graph.connectedComponents().mask(validGraph)
    println(s"\nconnectedComponents:")
    maskGraph.vertices.foreach(println)

    graph.groupEdges(merge = (a, b) => a + b).vertices.collect()

//    val nonUniqueCosts: RDD[(VertexId, Double)]
//    val uniqueCosts: VertexRDD[Double] =
//      graph.vertices.aggregateUsingIndex(nonUniqueCosts, (a, b) => a + b)
//    val joinGraph = graph.joinVertices(uniqueCosts)(
//      (id, oldCost, extraCost) => oldCost + extraCost
//    )

    sc.stop()
  }

}
