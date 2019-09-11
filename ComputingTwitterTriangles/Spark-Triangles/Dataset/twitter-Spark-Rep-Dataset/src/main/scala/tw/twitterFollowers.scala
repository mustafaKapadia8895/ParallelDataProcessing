package tw

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast




object twitterFollowers {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.twitterFollowers <input dir> <output dir>")
      System.exit(1)
    }
//    val conf = new SparkConf().setAppName("twitter-followers")
//    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================

    val spark = SparkSession
      .builder()
      .appName("twitter-followers")
      .getOrCreate()
    import spark.implicits._


    val ds = spark.read.csv(args(0)).toDF("from", "to")

    val filtered=ds.filter("from <=125000 and to <=125000")

    val path2=filtered.as("S1").join(broadcast(filtered.as("S2")),
      $"S1.to" === $"S2.from" && $"S1.from" =!= $"S2.to",
      "inner") //Path 2 Broadcast join
      .select("S1.from", "S2.to") //Remove middle columns


      val triangles =  path2.as("S3").join(broadcast(filtered.as("S4")), //Path 3 Join
      $"S3.to" === $"S4.from" && $"S4.to" === $"S3.from").count()/3 // Check for triangles and count


  }



}