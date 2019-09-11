package tw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

// Datatype StringInt = (Node: Int, Direction : String)

public class TwitterFollowers extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(TwitterFollowers.class);

	// Global Counter
	public static enum UpdateCounter {
		UPDATED
	}
	public static class TokenizerMapper1 extends Mapper<Object, Text, StringInt, StringInt> {

		private final Text word = new Text();
		private final int max=50000;

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			String temp=value.toString();
			String a[]=temp.split(",");
			int f = Integer.parseInt(a[0]);
			int t=Integer.parseInt(a[1]);

			//filter
			if(f<=max && t<=max){

				// make (to,from) and (from,to)
				context.write(new StringInt('f', f), new StringInt('t', t));
				context.write(new StringInt('t', t), new StringInt('f', f));
			}

		}
	}

	// Partition based on Node
	public static class CustomPartitioner extends
			Partitioner< StringInt, StringInt >
	{
		@Override
		public int getPartition(StringInt key, StringInt value, int numReduceTasks)
		{
			return (((Integer)key.getNode()).hashCode()) % numReduceTasks;
		}
	}

	// Construct path 2
	public static class Reducer1 extends Reducer<StringInt, StringInt, IntWritable, IntWritable> {
		private final IntWritable result = new IntWritable();

		// Due to secondatry sort all the to's come before from's in values
		@Override
		public void reduce(final StringInt key, final Iterable<StringInt> values, final Context context) throws IOException, InterruptedException {
			List<Integer> froms=new ArrayList<Integer>();
			for(StringInt val : values)
			{
				if(val.getDir() == 't' )
				{
					froms.add(val.getNode());
				}

				else
				{
					for(int v : froms)
					{
						if(v != val.getNode())
							context.write(new IntWritable(val.getNode()), new IntWritable(v));

					}
				}

			}

		}
	}

	// Group based on node for each reduce call
	public static class CustomGroup extends WritableComparator{
		protected CustomGroup()
		{
			super(StringInt.class, true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			StringInt s1= (StringInt) a;
			StringInt s2 = (StringInt)b;
			Integer n1=s1.getNode();
			Integer n2=s2.getNode();
			return n1.compareTo(n2);
		}
	}

	// receives input from edges.csv and attaches identifier 1
	public static class TokenizerMapper2 extends Mapper<Object, Text, IntPair, Text> {
		private final int max=50000;
		private final Text word = new Text();
		private TokenizerMapper2(){
			word.set("1");
		}

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			String temp=value.toString();
			String a[]=temp.split(",");
			int f = Integer.parseInt(a[0]);
			int t=Integer.parseInt(a[1]);
			if(f<=max && t<=max)
			context.write(new IntPair(t, f), word);

		}
	}

	// Receieves input from previous job and attaches identifier 2
	public static class TokenizerMapper3 extends Mapper<Object, Text, IntPair, Text> {

		private final Text word = new Text();
		private TokenizerMapper3(){
			word.set("2");
		}

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			String temp=value.toString();
			String a[]=temp.split(",");
			int f = Integer.parseInt(a[0]);
			int t=Integer.parseInt(a[1]);
			context.write(new IntPair(f,t), word);

		}
	}

	// counts triangles
	public static class Reducer2 extends Reducer<IntPair, Text, NullWritable, NullWritable> {
		int triangles=0;
		@Override
		public void reduce(final IntPair key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			int m=0;
			int n=0;
			for(Text v : values)
			{
				// counts number of entering edges
				if(v.toString().equals("1")){
					m++;
				}
				// counts number of edges leaving
				else{
					n++;
				}
			}
			triangles+= m*n;


		}

		// updates global counter
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.getCounter(UpdateCounter.UPDATED).increment(triangles);
			super.cleanup(context);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job1 = Job.getInstance(conf, "Twitter Followers");
		job1.setJarByClass(TwitterFollowers.class);
		final Configuration jobConf = job1.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
		// finds path2
		job1.setMapperClass(TokenizerMapper1.class);
		//job.setCombinerClass(IntSumReducer.class);
		job1.setPartitionerClass(CustomPartitioner.class);
		job1.setGroupingComparatorClass(CustomGroup.class);
		job1.setReducerClass(Reducer1.class);
		job1.setMapOutputKeyClass(StringInt.class);
		job1.setMapOutputValueClass(StringInt.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}

		// counts triangles
		Job job2 = Job.getInstance(conf, "round 2");
		job2.setJarByClass(TwitterFollowers.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(NullWritable.class);
		job2.setMapOutputKeyClass(IntPair.class);
		job2.setMapOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class,TokenizerMapper2.class);
		MultipleInputs.addInputPath(job2, new Path(args[1]+"/part-r*"),TextInputFormat.class, TokenizerMapper3.class);

		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}

		// reirieve counter
		long counter = job2.getCounters().findCounter(UpdateCounter.UPDATED).getValue();
		System.out.println("**********************************************");
		System.out.println("No. of triangles = " +counter/3.0 );

		return 1;


	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <intermediate-dir> <output-dir>");
		}
		
		try {
			ToolRunner.run(new TwitterFollowers(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}