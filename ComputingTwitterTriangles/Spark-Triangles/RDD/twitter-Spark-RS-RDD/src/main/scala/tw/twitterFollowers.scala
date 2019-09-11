package tw


import org.apache.spark.{SparkConf, SparkContext}
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

    var data = textFile.map(word=> mapBasedOnFile(word))
              .filter { case (from, to) => from <= 40000 && to<=40000 } //max filter

    var data2 = data.map{case (x,y) => (y,x)} //creates (to,from)


      val triangles=data2.join(data) //join for path 2
        .filter {case (mid, (from,to)) => from!=to} //removes paths of type 1->2->1
        .map { case (x,(y,z)) => (y,z)} // removes mid and creates (to,from)
        .join(data2) // join for path 3
      .filter{case (x, (y,z)) => y==z} //find triangles
        .count()/3

    println("*********************************************************")
    println(triangles)
    println("************************************************************")




  }

  // converts text file to pair rdd
  def mapBasedOnFile(args: String) : (Int,Int) = {
    var a=args.split(",")
    var b=a(0).toInt
    var c =a(1).toInt
      (b,c)

  }




}