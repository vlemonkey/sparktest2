package com.test

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.TreeSet
import scala.util.Random

/**
 * Created by s on 16-3-16.
 */
object ReduceTest2 {
  def main(args: Array[String]) {
    class RichSet(val s: TreeSet[Int]){
      def +(treeSet: TreeSet[Int]) = s + treeSet.head
    }

    implicit def TreeSetPlus(s: TreeSet[Int]) = new RichSet(s)

    val aa = TreeSet(1,2)
    val bb = TreeSet(3,4)
    aa + bb


    val conf = new SparkConf().setAppName("ReduceByKey Test").setMaster("spark://cloud138:7077")
    val sc = new SparkContext(conf)

    val a = sc.parallelize(Seq("a,b,c,d,a,b,c,b"))
    val b = a.flatMap(_.split(",")).map((_, List(TreeSet(Random.nextInt()), 222, 33)))

    val c = b.reduceByKey{
      case(x, y) =>
        (x, y).zipped.map{
          case(x: TreeSet[Int], y:TreeSet[Int]) => x + y
          case(x: Int, y: Int) => x + y
//          case(x: Any, y: Any) => Integer.parseInt(x.toString) + Integer.parseInt(y.toString)
        }
    }
    c.collect()
  }


}
