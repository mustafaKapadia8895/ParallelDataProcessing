package tw

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object twitterFollowers {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.twitterFollowers <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    
    val textFile = sc.textFile(args(0))
    val counts = textFile.map(word => mapBasedOnFile(word))
                 .reduceByKey(_ + _)

      counts.saveAsTextFile(args(1))
//    println("-------------------------------------------------")
//    println(counts.toDebugString)
//    println("-------------------------------------------------")
  }

  // Used to determine if input comes from nodes.csv file or edges.csv file and emit corresponding values
  def mapBasedOnFile(args: String) : (String, Int) = {
    if (args.contains(",")){
      return (args.substring(args.indexOf(",")+1), 1)
    }
    else
      return (args, 0)

  }
}