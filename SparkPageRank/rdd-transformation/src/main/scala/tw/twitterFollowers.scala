package tw

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession




object twitterFollowers {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.twitterFollowers <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("rdd-transformations")
    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================




    val k = 100

    var graph= new Array[(Int, Int)](k*k)

    // Create Graph
    for (i <- 1 to k*k){
      if(i%k == 0){
        graph(i-1) = (i, 0)
      }
      else{
        graph(i-1) = (i, i+1)
      }
    }

    // create ranks
    var vertex = new Array[(Int, Double)](k*k+1)
    vertex(0) = (0, 0)

    for(i <- 1 to k*k){
      vertex(i) = (i, 1.0/(k*k))
    }

    // Create RDDs
    var graphRDD = sc.parallelize(graph)
    var ranksRDD = sc.parallelize(vertex)

    for(i <- 1 to 10)
      {

        // Join to transfer PR to outlink
        var temp =graphRDD.join(ranksRDD)
          .flatMap(joinPair =>
            if (joinPair._1 % k == 1)
              List((joinPair._1, 0.0), (joinPair._2))
            else
              List(joinPair._2))

        // sum up PR values for each node
        var temp2 = temp.reduceByKey(_+_)

        // Lookup PR of node 0(dummy node)
        var delta = temp2.lookup(0)(0)

        var delta2 = delta/(k*k)

        // Distribute PR of dummy node
        ranksRDD = temp2.map { case (v1, pr) => addValues(v1, pr, delta2) }
        var sum=ranksRDD.map{case (v, pr) => if(v==0) 0 else pr}.sum()

        // Print sum pf PR for iteration
        println("*********************************************")
        println("Iteration-"+i+" sum : " + sum)
        println("*********************************************")

      }

    // Print nodes and pr for nodes 0 to 100
    var top100 = ranksRDD.sortByKey().take(101)

    for(i <- 0 to 100)
      {
        println(top100(i))
      }
    var output=sc.parallelize(top100)
    output.saveAsTextFile(args(1))


  }

  // helper function for distributing pr of dummy node
  def addValues(i: Int, d: Double, delta: Double) : (Int, Double) = {
    if(i == 0)
      return (i, d)
    else
      return (i, d+(delta))
  }
}