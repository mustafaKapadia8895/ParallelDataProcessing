package tw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class MrShortestPath extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(MrShortestPath.class);



	// Global Counter
	public static enum UpdateCounter {
		UPDATED
	}

	// Splits input on , and outputs key value pairs
	public static class TokenizerMapperAdj extends Mapper<Object, Text, IntWritable, IntWritable> {

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String edge[] = value.toString().split(",");
			int from = Integer.parseInt(edge[0]);
			int to = Integer.parseInt(edge[1]);
			context.write(new IntWritable(from), new IntWritable(to));
			// Handle dangling nodesi
			context.write(new IntWritable(to), new IntWritable(0));

		}
	}

	// Outputs adjacency list for each node
	public static class ReducerAdj extends Reducer<IntWritable, IntWritable, Node, NullWritable> {
		private final IntWritable result = new IntWritable();

		List<Integer> klist = new ArrayList<>();

		// Retrieve sources from context
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			String ks[]= context.getConfiguration().get("ks").split(",");
			for(String k : ks)
			{
				klist.add(Integer.parseInt(k));
			}
		}

		@Override
		public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {

			java.util.List<Integer> adjList = new ArrayList<>();
			for(IntWritable value : values)
			{
				if(value.get() != 0)
				adjList.add(value.get());
			}
			Node node = new Node(key.get());
			node.setAdjList(adjList);

			// Sets isActive for source nodes
			for(int kval : klist)
			{
				if(node.getNodeId() == kval)
					node.setActive(true);
			}
			context.write(node, null);

		}
	}

	// Outputs the object itself and the nodes from the adjacency list of each node
	public static class TokenizerMapperKSource extends Mapper<Object, Text, IntWritable, Node> {

		List<Integer> klist = new ArrayList<>();

		// Retrieve sources from context
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			String ks[]= context.getConfiguration().get("ks").split(",");
			for(String k : ks)
			{
				klist.add(Integer.parseInt(k));
			}
		}

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String s= value.toString();
			Node node;
			node = Node.jsonToNode(s);
			node.setIsObj(true);

			// Set distance of source from itself as 0
			for(int kVal : klist)
			{
				if(node.getNodeId() == kVal)
				{
					node.getkMap().put(kVal, 0);
				}
			}

			context.write(new IntWritable(node.getNodeId()), node);

			// If node is active, emit it's adjacency list with distance set to distance to node+1
			if(node.isActive())
			{
				for(int adjNode : node.getAdjList())
				{
					Node node1 = new Node(adjNode);
					for(int k : klist)
					{
						if(node.getkMap().containsKey(k))
						{
							node1.getkMap().put(k, node.getkMap().get(k)+1);
						}

					}
					context.write(new IntWritable(adjNode), node1);
				}

			}

		}
	}

	// Outputs the node object with updated value of dmin for each source
	public static class ReducerKSource extends Reducer<IntWritable, Node, Node, NullWritable> {
		List<Integer> klist = new ArrayList<>();

		// retrieve sources from context
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			String ks[]= context.getConfiguration().get("ks").split(",");
			for(String k : ks)
			{
				klist.add(Integer.parseInt(k));
			}
		}

		@Override
		public void reduce(final IntWritable key, final Iterable<Node> values, final Context context) throws IOException, InterruptedException {


			// Set value for each source to infinity
			Map<Integer, Integer> kmap = new HashMap<>();
			for(int k : klist)
			{
				kmap.put(k, Integer.MAX_VALUE);
			}

			Node m = null;

			for(Node node : values)
			{
				// Object found
				if(node.getIsObj())
				{
					m = new Node(node.getNodeId(), node.getAdjList(), node.getIsObj(), node.isActive(), node.getkMap());
					m.setActive(false);
				}
				// Check if the distance is the minimum
				else
				{
					for (int k: node.getkMap().keySet())
					{
						if(node.getkMap().get(k) < kmap.get(k))
						{
							kmap.put(k, node.getkMap().get(k));
						}
					}
				}
			}

			// Check if dmin is lesser than node.min for each source
			for(int k : klist)
			{
				if(m.getkMap().containsKey(k))
				{
					if (m.getkMap().get(k) > kmap.get(k))
					{
						m.getkMap().put(k, kmap.get(k));
						m.setActive(true);
						context.getCounter(UpdateCounter.UPDATED).increment(1);
					}
				}
				else if(kmap.get(k)< Integer.MAX_VALUE)
				{
					m.getkMap().put(k, kmap.get(k));
					m.setActive(true);
					context.getCounter(UpdateCounter.UPDATED).increment(1);
				}

			}
			context.write(m, null);

		}
	}

	// Outputs the minimum distance to a node from each source
	public static class TokenizerMapperAggregate extends Mapper<Object, Text, IntWritable, IntWritable> {

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String input = value.toString();
			Node node;
			node = Node.jsonToNode(input);
			Map<Integer,Integer> kmap =node.getkMap();
			for(int k : kmap.keySet())
			{
				context.write(new IntWritable(k),new IntWritable(kmap.get(k)));
			}
		}
	}


	// finds the maximum distance for the node furthest away for each source
	public static class ReducerAggregate extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {

			int max = 0;
			for(IntWritable value: values)
			{
				int val = value.get();
				if(val> max)
					max=val;
			}
			context.write(key, new IntWritable(max));
		}
	}




	@Override
	public int run(final String[] args) throws Exception {

		final Configuration conf = getConf();
		final Job jobAdj = Job.getInstance(conf, "Shortest Path");

		// Random Sampler
		Random rand = new Random();

		int  n = rand.nextInt(11316811) + 1;
		String ks=""+n;
		for(int i = 1; i<= Integer.parseInt(args[2])-1; i++)
		{
			n = rand.nextInt(11316811) + 1;
			ks = ks+","+n;
		}



		jobAdj.setJarByClass(MrShortestPath.class);
		final Configuration jobConf = jobAdj.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
//		if (fileSystem.exists(new Path(args[2]+"*"))) {
//			fileSystem.delete(new Path(args[2]+"*"), true);
//		}
		// ================


		// Creates Adjacency Lists
		jobAdj.setMapperClass(TokenizerMapperAdj.class);
		jobAdj.setReducerClass(ReducerAdj.class);
		jobAdj.setMapOutputKeyClass(IntWritable.class);
		jobAdj.setMapOutputValueClass(IntWritable.class);
		jobAdj.setOutputKeyClass(Node.class);
		jobAdj.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(jobAdj, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobAdj, new Path(args[1] + "0"));
		jobAdj.getConfiguration().set("ks", ks);

		if (!jobAdj.waitForCompletion(true)) {
			System.exit(1);
		}

		long counter;
		int i = 0;
		do {
			// Finds path to nodes from K sources
			Job jobKSource = Job.getInstance(conf, "round 2");
			jobKSource.setJarByClass(MrShortestPath.class);
			jobKSource.setMapperClass(TokenizerMapperKSource.class);
			jobKSource.setReducerClass(ReducerKSource.class);
			jobKSource.setOutputKeyClass(Node.class);
			jobKSource.setOutputValueClass(NullWritable.class);
			jobKSource.setMapOutputKeyClass(IntWritable.class);
			jobKSource.setMapOutputValueClass(Node.class);
			FileInputFormat.addInputPath(jobKSource, new Path(args[1] + i));

			FileOutputFormat.setOutputPath(jobKSource, new Path(args[1] + (i + 1)));
			jobKSource.getConfiguration().set("ks", ks);
			if (!jobKSource.waitForCompletion(true)) {
				System.exit(1);
			}
			counter = jobKSource.getCounters().findCounter(UpdateCounter.UPDATED).getValue();
			i++;
		} while (counter != 0);


		// Finds maximum of shortest paths for each source
		Job jobAggregate = Job.getInstance(conf, "round 3");
		jobAggregate.setJarByClass(MrShortestPath.class);
		jobAggregate.setMapperClass(TokenizerMapperAggregate.class);
		jobAggregate.setReducerClass(ReducerAggregate.class);
		jobAggregate.setOutputKeyClass(IntWritable.class);
		jobAggregate.setOutputValueClass(IntWritable.class);
		jobAggregate.setMapOutputKeyClass(IntWritable.class);
		jobAggregate.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(jobAggregate, new Path(args[1] + i));

		FileOutputFormat.setOutputPath(jobAggregate, new Path(args[1] + (i + 1)));
		if (!jobAggregate.waitForCompletion(true)) {
			System.exit(1);
		}
		return 1;
	}

		public static void main ( final String[] args){
			if (args.length != 3) {
				throw new Error("Three arguments required:\n<input-dir> <output-dir> k");
			}

			try {
				ToolRunner.run(new MrShortestPath(), args);
			} catch (final Exception e) {
				logger.error("", e);
			}
		}


}