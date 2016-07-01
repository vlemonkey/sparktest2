package com.test

import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

import java.util.Properties

/**
 * Created by s on 16-3-15.
 */
object Spark2Mysql {
  def main(args: Array[String]) {
    val url = "jdbc:mysql://10.95.3.113:3306/oral"
    val prop = new Properties()
    prop.setProperty("user", "tescomm")
    prop.setProperty("password", "tescomm")

    val conf = new SparkConf().setAppName("Spark RDD to Mysql")
      .setMaster("spark://cloud138:7077")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = sc.textFile("baotest/jdbctest/input/").map {
      item =>
        val items = item.split(",")
        Row.apply(items(0),
          items(1).toInt)
    }

    val schema = StructType(
      StructField("name", StringType) ::
        StructField("age", IntegerType) :: Nil)

    val df = sqlContext.createDataFrame(data, schema)
    df.write.mode(SaveMode.Append).jdbc(url, "test2", prop)
    sc.stop()
  }

}
