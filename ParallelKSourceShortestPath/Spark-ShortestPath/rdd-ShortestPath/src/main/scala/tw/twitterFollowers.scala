package tw

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession





object twitterFollowers {

  var k1 = "1"
  var k2= "2"
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.twitterFollowers <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("rdd-ShortestPath")
    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================

    // create adjacency list
    val adjList = sc.textFile(args(0)).map{line =>
      val split = line.split(",")
      (split(0), List(split(1)))
    }
        .reduceByKey((a,b) => List.concat(a,b))

    // persist adjList in memory
    adjList.persist()



    // Create distance rdd
    var distances = adjList.map{case (id, adj) => (id, mapSources(id, adj))}

    for(i <- 1 to 10)
    {
      // Follow algorithm => join -> extract values -> reduce by key by selecting min
        distances=joinWithCommonPartitioner(adjList,distances).flatMap{ case (n, (adj, currentD)) => extractValues(n, adj, currentD)}
            .reduceByKey((x,y) => (math.min(x._1, y._1), math.min(x._2, y._2)))
   }

    // find max of min distances
    val maxd=distances.flatMap(joinpair =>
    List((k1, joinpair._2._1),(k2, joinpair._2._2))
    )
        .reduceByKey((x,y)=>
          if((x.isInfinite & !y.isInfinite) || (!x.isInfinite & y.isInfinite))
            math.min(x,y)
          else if(x.isInfinite & y.isInfinite)
            Double.PositiveInfinity
          else
            math.max(x,y)
        )

    // save to output
    maxd.saveAsTextFile(args(1))
  }

  // if source, return distance to itself as 0, else infinity
  def mapSources(str: String, list: List[Any]) : (Double, Double) = {

    if(str == k1)
      (0.0, Double.PositiveInfinity)
    else if (str == k2)
      (Double.PositiveInfinity, 0.0)
    else
      (Double.PositiveInfinity, Double.PositiveInfinity)
  }

  // for each node n in m's adjacency list, emit n, distance of m+1 for each source
  def extractValues(str: String, strings: List[String], tuple: (Double, Double)) : List[(String, (Double, Double))] =
  {
    strings.map((_, (tuple._1+1, tuple._2+1))) ++ List((str, tuple))
  }

  // join using a common partitioner
  def joinWithCommonPartitioner(adjList: RDD[(String, List[String])],
                                distances: RDD[(String, (Double, Double))]) : RDD[(String, (List[String], (Double,Double)))]= {
      // If addressRDD has a known partitioner we should use that,
      // otherwise it has a default hash parttioner, which we can reconstruct by
      // getting the number of partitions.
      val adjListPartitioner = adjList.partitioner match {
        case (Some(p)) => p
        case (None) => new HashPartitioner(adjList.partitions.length)
      }

      adjList.join(distances, adjListPartitioner)
    }

}