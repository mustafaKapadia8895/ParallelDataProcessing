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

	//Global counter
	public static enum UpdateCounter {
		UPDATED
	}
	public static class TokenizerMapper1 extends Mapper<Object, Text, StringInt, StringInt> {

		private final Text word = new Text();
		private final int max=1000;

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			String temp=value.toString();
			String a[]=temp.split(",");
			int f = Integer.parseInt(a[0]);
			int t=Integer.parseInt(a[1]);

			// Max filter
			if(f<=max && t<=max){

				context.write(new StringInt('f', f), new StringInt('t', t));
				context.write(new StringInt('t', t), new StringInt('f', f));
			}

		}
	}

	//Partition based on node
	public static class CustomPartitioner extends
			Partitioner< StringInt, StringInt >
	{
		@Override
		public int getPartition(StringInt key, StringInt value, int numReduceTasks)
		{
			return (((Integer)key.getNode()).hashCode()) % numReduceTasks;
		}
	}


	

	public static class Reducer1 extends Reducer<StringInt, StringInt, IntWritable, IntWritable> {
		long result=0;

		@Override
		public void reduce(final StringInt key, final Iterable<StringInt> values, final Context context) throws IOException, InterruptedException {
			long numOfTos=0;
			// To's come before from's due to comparator (secondary sort)
			for(StringInt val : values)
			{
				if(val.getDir() == 't' )
				{
					numOfTos++;
				}

				else
				{
					result+=numOfTos;
				}

			}


		}

		// updates counter
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.getCounter(UpdateCounter.UPDATED).increment(result);
			super.cleanup(context);
		}
	}

	// Groups based on node for a single function call
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



		long counter = job1.getCounters().findCounter(UpdateCounter.UPDATED).getValue();
		System.out.println("**********************************************");
		System.out.println("No. of path2's = " +counter );

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