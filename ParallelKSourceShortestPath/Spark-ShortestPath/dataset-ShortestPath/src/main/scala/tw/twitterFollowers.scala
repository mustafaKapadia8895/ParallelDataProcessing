package tw

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._




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
      .appName("Dataset Shortest Path")
      .getOrCreate()

    val k1 = "1"
    val k2 = "2"

    import spark.implicits._

    val graphDataSet = spark.read.csv(args(0)).toDF("from", "to").repartition($"from")

    // Persist in memory
    graphDataSet.persist()

    // create distances dataset as (node, (ds1, ds2)) where ds1= distance from source 1 and ds2 is distance from source2
    var distance = graphDataSet.select("from").distinct().select($"from".as("node"), when($"from" === k1, 0.0).otherwise(Double.PositiveInfinity).as("d1"),
      when($"from" === k2, 0.0).otherwise(Double.PositiveInfinity).as("d2")).repartition($"node")

    for(i <- 1 to 10)
    {
      // join
        distance=graphDataSet.as("G").join(distance.as("D") , $"G.from" === $"D.node")
          // select relevant columns, union with distance
          .select($"G.to".as("node"), ($"D.d1"+1).as("d1"), ($"D.d2"+1).as("d2")).union(distance)
          // find minimum distance from source for each node for each source
        .groupBy("node").min("d1","d2").toDF("node", "d1", "d2")
     }


    // find maximum distance for each source
  val maxD = distance.select($"node",
            when($"d1" === Double.PositiveInfinity, -1).otherwise($"d1").as("d1"),
            when($"d2" === Double.PositiveInfinity, -1).otherwise($"d2").as("d2"))
      .select(max($"d1"), max($"d2"))

    // print dataset to console
  maxD.show()

    spark.sparkContext.stop()



  }




}