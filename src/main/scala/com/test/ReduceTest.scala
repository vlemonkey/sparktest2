package com.test

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.util.Random

/**
 * Created by s on 16-3-16.
 */
object ReduceTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ReduceByKey Test").setMaster("spark://cloud138:7077")
    val sc = new SparkContext(conf)

    val a = sc.parallelize(Seq("a,b,c,d,a,b,c,b"))
    val b = a.flatMap(_.split(",")).map((_, List(mutable.TreeSet(Random.nextInt()), 222, 33)))

//    val b = sc.parallelize("Seq((a, List(Set(100), 100, 20)), (a, List(Set(110), 80, 40)), (b, List(Set(100), 100, 20)))")

    val c = b.reduceByKey{
      case(x, y) =>
        (x, y).zipped.map{
          case(x: mutable.TreeSet[Int], y:mutable.TreeSet[Int]) => x + y.head
          case(x: Any, y: Any) => Integer.parseInt(x.toString) + Integer.parseInt(y.toString)
        }
    }
    c.collect()

    val str = "a,df,as,fd,as,fd,asd,fa,ds,fa,sdf"


  }
}
