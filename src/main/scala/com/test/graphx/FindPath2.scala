package com.test.graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

/**
 * Created by zcq on 16-4-27.
 */
object FindPath2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("findPathCircle").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val vertices: RDD[(VertexId, ListBuffer[String])] =
      sc.parallelize(
        Array((1L, ListBuffer("1")), (2L, ListBuffer("2")), (3L, ListBuffer("3")), (4L, ListBuffer("4")), (5L, ListBuffer("5")),
              (6L, ListBuffer("6")), (7L, ListBuffer("7")), (10L, ListBuffer("10")), (8L, ListBuffer("8")), (9L, ListBuffer("9")),
              (11L, ListBuffer("11")), (12L, ListBuffer("12")))
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
    val sep = "->"
    val laps = 1

    val initialMsg = ListBuffer("")

    def vprog(vertexId: VertexId, value: ListBuffer[String], message: ListBuffer[String]): ListBuffer[String] = {
//      println(s"id--->$vertexId,val--->$value,msg----->$message")
      //当顶点不是终点时
      if (vertexId != endId) {
        if(message.contains("")) value
        else message.map(_ + sep + vertexId.toString)
      }
      //当顶点是终点时，将每条路径作为一个新元素保存到顶点的属性里
      else
        value ++ message.map(_ + sep + endId.toString)
    }

    def sendMsg(triplet: EdgeTriplet[ListBuffer[String], Boolean]): Iterator[(VertexId, ListBuffer[String])] = {
      //判断源点属性里是否包含起点，包含就迭代，不包含就跳出
      if (judgePath(triplet.srcAttr, startId.toString)) {

        //根据限定的圈数：1，将绕行圈数超过1圈的所有路径去掉
        val realPath = triplet.srcAttr.filter(limitCircleLaps(_, triplet.dstId, laps))
        if (realPath.size == 0) Iterator.empty else Iterator((triplet.dstId, realPath))
      }
      else
        Iterator.empty
    }

    def mergeMsg(msg1: ListBuffer[String], msg2: ListBuffer[String]): ListBuffer[String] = {
      // println("msg1--->" + msg1 + ",msg2--->" + msg2);
      //当同一顶点接收到多条消息时，将其合并
      msg1 ++ msg2
    }


    //判断list的每个元素是否包含指定字符串，只有当所有元素都包含时，返回true
    def judgePath(list: ListBuffer[String], str: String) = {
      !list.map(_.split(sep).contains(str)).contains(false)
    }

    /**
     * 限定绕环圈数
     * @param everyPath：发送消息的list列表中，每条元素，即每条路径
     * @param dstId：接收消息的目标点的id
     * @param laps：限定可以绕行的圈数
     * @return
     */
    def limitCircleLaps(everyPath: String, dstId: VertexId, laps: Int) = {
      val list = everyPath.split(sep).filter(_ == dstId.toString)
      list.size != laps + 1
    }

    val minGraph = graph.pregel(initialMsg,
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

