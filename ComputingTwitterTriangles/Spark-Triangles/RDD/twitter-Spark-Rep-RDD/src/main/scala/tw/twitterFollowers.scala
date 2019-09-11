package tw


import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.log4j.LogManager
import org.apache.log4j.Level
//import org.apache.spark.rdd.{PairRDDFunctions, RDD}
//import org.apache.spark.sql.SparkSession
//import org.fusesource.leveldbjni.All
//
//import scala.collection.Map
//import scala.reflect.ClassTag
//import scala.collection.mutable.HashMap
//import scala.collection.Seq


object twitterFollowers {


  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.twitterFollowers <input dir> <output dir>")
      System.exit(1)
    }


    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))


    var data = textFile.map(word => mapBasedOnFile(word)) //Max filter
      .filter { case (from, to) => from <= 50000 && to <= 50000 }

    val listToBroadcast = data.groupByKey().collectAsMap().map(x => (x._1, x._2.toList)) //create adjacency list

    val BroadcastedList = data.sparkContext.broadcast(listToBroadcast) //Broadcast

    // finds triangles
    def findTrio(from: Int, to: Int): Int = {
      var triangles = 0
      val x = BroadcastedList.value.get(to)
      if (x != None) { //check adjaceny for path2
        for (path1 <- x.get) {
          val y = BroadcastedList.value.get(path1)
          if (y != None) {
            for (path2 <- y.get) {  //Check adjaecency for path 3
              if (path2 == from) { //Check for triangles
                triangles += 1
              }
            }
          }
        }
      }
      triangles

    }

    // for each partition count triangles
    val count = data.mapPartitions(iter => iter.map(x => findTrio(x._1, x._2))).sum() / 3

    println("************************************************************")
    println("triangles = " +count)
    println("************************************************************")

  }
    // converts csv to pair rdd
    def mapBasedOnFile(args: String): (Int, Int) = {
      var a = args.split(",")
      var b = a(0).toInt
      var c = a(1).toInt
      (b, c)

    }



}