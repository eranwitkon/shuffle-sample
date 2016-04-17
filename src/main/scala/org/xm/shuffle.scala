package org.xm

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by eranw on 13/04/16.
  */
object shuffleSampleDriver {


  def main(args: Array[String]): Unit = {}

  var conf = new SparkConf().setAppName("Spark Shuffle - reduceByKey")
  var sc = new SparkContext(conf)

  sc.setLogLevel("ERROR")
  val sourceFile = "hdfs://master:9000/data/"
  sc.setJobDescription("reduceByKey")
  val wordsCount = sc.textFile(sourceFile)
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey((a, b) => a + b)

  print(wordsCount.first())
  sc.stop()

  conf = new SparkConf().setAppName("Spark Shuffle - groupByKey")
  sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  sc.setJobDescription("groupByKey")
  val wordsCountGB = sc.textFile(sourceFile)
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .groupByKey()
      .map(t => (t._1,t._2.sum))

  print(wordsCountGB.first())
}