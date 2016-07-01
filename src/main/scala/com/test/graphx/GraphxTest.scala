package com.test.graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by s on 16-4-18.
 */
object GraphxTest {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Graphx test").setMaster("local"))
    sc.setLogLevel("WARN")

    class VertexProperty()
    class UserProperty(val name: String) extends VertexProperty
    class ProductProperty(val name: String, price: Double) extends VertexProperty

//    val graphx: Graph[VertexProperty, String] = null

    val array = Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                      (5L, ("franklin", "prof")), (2L, ("istoica", "prof")))
    val users: RDD[(VertexId, (String, String))] = sc.parallelize(array)

    val array2 = Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"))
    val relationships: RDD[Edge[String]] = sc.parallelize(array2)

    val defaultUser = ("John Doe", "Missing")

    val graph = Graph(users, relationships, defaultUser)

    println(s"graph.vertices foreach println")
    graph.vertices.foreach(println)
    println(s"\ngraph.deges foreach println")
    graph.edges.foreach(println)

    println(s"\nvertices count:${graph.vertices.filter{case(id, (name, pos)) => pos == "postdoc"}.count()}")
    println(s"\nedges count:${graph.edges.filter(e => e.srcId > e.dstId).count()}")
    println(s"\nedges count:${graph.edges.filter{case Edge(src, dst, prop) => src > dst}.count}")

    println(s"\ntriplets")
    graph.triplets.foreach(println)

    println(s"\ntriplets2")
    graph.triplets.map(triplet =>
      triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
      .collect.foreach(println(_))

    graph.collectNeighborIds(EdgeDirection.Either).collect()
    graph.collectNeighbors(EdgeDirection.In).collect()

//    val newVertices = graph.vertices.map { case (id, attr) => (id, mapUdf(id, attr)) }
//    val newGraph = Graph(newVertices, graph.edges)

    val inputGraph: Graph[Int, String] =
      graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))
    val outputGraph: Graph[Double, Double] =
      inputGraph.mapTriplets(triplet => 1.0 / triplet.srcAttr).mapVertices((id, _) => 1.0)

//    graph.out

  }
}
