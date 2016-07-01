package com.test.graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zcq on 16-4-27.
 */
object FindPath {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("findPathCircle").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val vertices: RDD[(VertexId, List[String])] =
      sc.parallelize(
        Array((1L, List("1")), (2L, List("2")), (3L, List("3")), (4L, List("4")), (5L, List("5")),
              (6L, List("6")), (7L, List("7")), (10L, List("10")), (8L, List("8")), (9L, List("9")),
              (11L, List("11")), (12L, List("12")))
      )

    // Create an RDD for edges
    val relationships: RDD[Edge[Boolean]] =
      sc.parallelize(
        Array(Edge(1L, 2L, true), Edge(1L, 4L, true), Edge(1L, 5L, true), Edge(1L, 6L, true),
        Edge(2L, 3L, true), Edge(3L, 4L, true), Edge(4L, 5L, true), Edge(5L, 6L, true), Edge(10L, 1L, true),
        Edge(1L, 8L, true), Edge(8L, 9L, true), Edge(9L, 4L, true), Edge(7L, 11L, true), Edge(6L, 7L, true),
        Edge(1L, 12L, true), Edge(12L, 6L, true), Edge(1L, 3L, true), Edge(3L, 9L, true), Edge(5L, 1L, true))
      )

    val graph = Graph(vertices, relationships)

    val startId = 1L
    val endId = 6L

    val initGraph = graph.mapVertices((id, v) => if (id == startId) v else v)


    initGraph.vertices.collect.foreach(println)

    val initialMsg = List("")


    def vprog(vertexId: VertexId, value: List[String], message: List[String]): List[String] = {
      println(s"id--->$vertexId,val--->$value,msg----->$message")
      //println("msg+value---->"+(value+message))
      //当顶点不是终点时
      if (vertexId != endId) {
        //第一轮时，如果消息为空，保持该节点属性不变
        if (message.contains(""))
          value
        //如果消息不为空，将属性替换为新消息和本身的原始属性
        else {
          message.map(_ + "->" + vertexId.toString)
        }

      }
      //当顶点是终点时，将每条路径作为一个新元素保存到顶点的属性里
      else
        value ++ message.map(_ + "->" + endId.toString)
    }

    def sendMsg(triplet: EdgeTriplet[List[String], Boolean]): Iterator[(VertexId, List[String])] = {
      //判断源点属性里是否包含起点，包含就迭代，不包含就跳出
      if (judgePath(triplet.srcAttr, startId.toString)) {
        //根据限定的圈数：1，将绕行圈数超过1圈的所有路径去掉
        val realPath = triplet.srcAttr.filter(limitCircleLaps(_, triplet.dstId, 1))

        if (realPath.size == 0) Iterator.empty else Iterator((triplet.dstId, realPath))
      }
      else
        Iterator.empty
    }

    def mergeMsg(msg1: List[String], msg2: List[String]): List[String] = {
      // println("msg1--->" + msg1 + ",msg2--->" + msg2);
      //当同一顶点接收到多条消息时，将其合并
      msg1 ++ msg2
    }


    //判断list的每个元素是否包含指定字符串，只有当所有元素都包含时，返回true
    def judgePath(list: List[String], str: String) = {
      var b = true
      list.foreach { x =>
        if (b) {
          val ss = x.split("\\->")
          if (!ss.contains(str)) b = false
        }
      }
      b
    }

    /**
     * 限定绕环圈数
     * @param everyPath：发送消息的list列表中，每条元素，即每条路径
     * @param dstId：接收消息的目标点的id
     * @param laps：限定可以绕行的圈数
     * @return
     */
    def limitCircleLaps(everyPath: String, dstId: VertexId, laps: Int) = {
      val list = everyPath.split("\\->").toList.filter(_ == dstId.toString)
      if (list.size == laps + 1) {
        false
      } else {
        true
      }
    }

    val minGraph = initGraph.pregel(initialMsg,
      100,
      EdgeDirection.Out)(
        vprog,
        sendMsg,
        mergeMsg).subgraph(vpred = (id, attr) => id == endId)

    minGraph.vertices.collect.map {
      case (vertexId, value) => println(vertexId + "--->" + value) //value.foreach(println)
    }
  }
}

