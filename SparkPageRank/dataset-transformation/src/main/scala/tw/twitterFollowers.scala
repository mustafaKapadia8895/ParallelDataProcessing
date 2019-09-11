package tw

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
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
    
//    val textFile = sc.textFile(args(0))
//    val counts = textFile.map(word => mapBasedOnFile(word))
//                  .groupByKey()
//                 .map(x => (x._1, x._2.sum))



    val spark = SparkSession
      .builder()
      .appName("dataset-transformation")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.setCheckpointDir("/tmp")



    val k = 100

    var graph= new Array[(Int, Int)](k*k)

    // Create graph
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
    vertex(0) = (0, 0.0)

    for(i <- 1 to k*k){
      vertex(i) = (i, 1.0/(k*k))
    }

    // convert to Datasets
    var graphDataSet = spark.createDataFrame(graph).toDF("from", "to").repartition($"from")
    var ranksDataSet = spark.createDataFrame(vertex).toDF("node", "rank").repartition($"node")

    // persist
    graphDataSet.persist()

    // checkpoint
    graphDataSet=graphDataSet.checkpoint(false)
    ranksDataSet=ranksDataSet.checkpoint(eager = false)


    for(i <- 1 to 10)
      {
        println("**********************************")
        println(i)
        println("***********************************")
        // join
        var temp = graphDataSet.as("G").join(ranksDataSet.as("R"),
          $"G.from" === $"R.node")


        // find nodes with no inlinks
        var startNodes = temp.filter($"G.from" % k  === 1).select("G.from", "R.rank")
          .map(row=> (row.getInt(0) , 0)).toDF("node", "rank")

        // find pr sum for each inlink
        var othernodes= temp.select("to", "rank").groupBy("to").sum().select("to", "sum(rank)")


        // Checkpoint to avoid recomputation
        temp=temp.checkpoint(eager = false)
        ranksDataSet=ranksDataSet.checkpoint(eager = false)
        othernodes=othernodes.checkpoint(eager =false)
        startNodes=startNodes.checkpoint(eager = false)

        // retrieve pr for 0 node(dummy)
        var zeroNode=othernodes.where("to==0").collect()(0)(1)

        // Distribute pr of dummy node
        var allnodes=startNodes.union(othernodes).toDF("node", "rank")
        ranksDataSet =allnodes.map(row => if (row.getInt(0) != 0)
          (row.getInt(0), row.getDouble(1)+zeroNode.asInstanceOf[Number].doubleValue()/(k*k))
        else (row.getInt(0), row.getDouble(1))).toDF("node", "rank")

        // Find sum of pr's for iteration
        val prsum = ranksDataSet.filter("node != 0").select("rank").groupBy().sum().toDF("iteration" +i)
        prsum.show(false)
      }


    // retrieve nodes 0-100 with PRs
    var maxvals  = ranksDataSet.sort("node").head(101)
 //   ranksDataSet.explain
    ranksDataSet = spark.sparkContext.parallelize(maxvals).map(row => (row.getInt(0), row.getDouble(1))).toDF("node", "rank")
    ranksDataSet.write.csv(args(1))
    ranksDataSet.show(101,false)


  }


}