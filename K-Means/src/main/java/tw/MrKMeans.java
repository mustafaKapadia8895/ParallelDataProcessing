package tw;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;

import com.sun.xml.internal.xsom.impl.util.Uri;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
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

// 564512 : Maximum followers for a user
public class MrKMeans extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(MrKMeans.class);



	// Global Counter
	public static enum ConvergenceCounter {
		CONVERGENCE
	}


	// from,to => to,1
	public static class CountFollowersMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String[] toFrom = value.toString().split(",");
			int to = Integer.parseInt(toFrom[1]);
			context.write(new IntWritable(to), new IntWritable(1));
		}
	}

	// Emit key , sum of values
	public static class CountFollowersReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private final IntWritable result = new IntWritable();

		int max = 0;

		@Override
		public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			int followers=0;
			for (IntWritable follower : values)
			{
				followers+=follower.get();
			}
			context.write(key,new IntWritable(followers));
		}

	}

	// emits the closest center for each node
	public static class kMeansMapper extends Mapper<Object, Text, DoubleWritable, IntWritable> {



		ArrayList<Double> centroids = new ArrayList<>();
		// Retrieve centroids from context
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			String centroidString = context.getConfiguration().get("centroids");
			for(String centroid : centroidString.split(","))
			{
				centroids.add(Double.parseDouble(centroid));
			}

		}

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			String input[] = value.toString().split(",");
			int followers = Integer.parseInt(input[1]);
			double closestCenter = centroids.get(0);
			double minDistance = Math.abs(followers - centroids.get(0));

			// Find centroid that is closest to node
			for(double centroid : centroids)
			{
				if (Math.abs(centroid-followers) < minDistance)
				{
					minDistance=Math.abs(centroid-followers);
					closestCenter = centroid;
				}
			}

			context.write(new DoubleWritable(closestCenter), new IntWritable(followers));

		}
	}

	// Computes the new centroids
	public static class kMeansReducer extends Reducer<DoubleWritable, IntWritable, DoubleWritable, DoubleWritable> {


		long counter = 0;
		ArrayList<Double> centroids = new ArrayList<>();
		// retrieve Centroids from context
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			String centroidString = context.getConfiguration().get("centroids");
			for(String centroid : centroidString.split(","))
			{
				centroids.add(Double.parseDouble(centroid));
			}
		}

		@Override
		public void reduce(final DoubleWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {


			double sse=0;
			int sum=0;
			int count=0;

			// Find sum of values and compute sse
			for(IntWritable followers: values)
			{
				sum += followers.get();
				count++;
				sse += Math.pow((key.get() - followers.get()), 2);
			}
			double newCentroid = sum/(count*1.0);
			context.write(new DoubleWritable(newCentroid), new DoubleWritable(sse));
			// Check for convergence
			if(Math.abs(key.get() - newCentroid) > 0.001)
				counter++;

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);

			// Increment global counter
			context.getCounter(ConvergenceCounter.CONVERGENCE).increment(counter);
		}
	}




	@Override
	public int run(final String[] args) throws Exception {

		final Configuration conf = getConf();
		final Job countFollowers = Job.getInstance(conf, "Count Followers");



		countFollowers.setJarByClass(MrKMeans.class);
		final Configuration jobConf = countFollowers.getConfiguration();
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


		// find follower counts
		countFollowers.setMapperClass(CountFollowersMapper.class);
//		countFollowers.setCombinerClass(CountFollowersReducer.class);
		countFollowers.setReducerClass(CountFollowersReducer.class);
		countFollowers.setMapOutputKeyClass(IntWritable.class);
		countFollowers.setMapOutputValueClass(IntWritable.class);
		countFollowers.setOutputKeyClass(IntWritable.class);
		countFollowers.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(countFollowers, new Path(args[0]));
		FileOutputFormat.setOutputPath(countFollowers, new Path(args[1]+"0"));

		if (!countFollowers.waitForCompletion(true)) {
			System.exit(1);
		}


		// Initial centroids
		String centroids="1,2,3,4";
		long counter;
		int i = 0;
		// 10 iterations
		do {

			Job jobKSource = Job.getInstance(conf, "iteration "+ (i+1));
			final Configuration jobConf2 = jobKSource.getConfiguration();
			jobConf2.set("mapreduce.output.textoutputformat.separator", ",");

			jobKSource.setJarByClass(MrKMeans.class);
			jobKSource.setMapperClass(kMeansMapper.class);
			jobKSource.setReducerClass(kMeansReducer.class);
			jobKSource.setOutputKeyClass(DoubleWritable.class);
			jobKSource.setOutputValueClass(DoubleWritable.class);
			jobKSource.setMapOutputKeyClass(DoubleWritable.class);
			jobKSource.setMapOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(jobKSource, new Path(args[1] + "0"));

			FileOutputFormat.setOutputPath(jobKSource, new Path(args[1] + (i+1)));
			// add centroids to context
			jobKSource.getConfiguration().set("centroids", centroids);
			if (!jobKSource.waitForCompletion(true)) {
				System.exit(1);
			}


			counter=jobKSource.getCounters().findCounter(ConvergenceCounter.CONVERGENCE).getValue();

			i++;
			centroids="";


			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(URI.create("s3://hw1-twitter"), conf);
			Path valPath=new Path("/K-Means-11Machines-Badk-2/"+"output"+i);
			FileStatus[] valFilePathList = fs.listStatus(valPath);

			// Sum sse and add new centroids to context for the next iteration
			double sse=0;
			for (FileStatus file : valFilePathList) {
				String filePath=file.getPath().toString();
				if (filePath.contains("part") && !filePath.contains(".crc")) {
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
					String line;
					while ((line=br.readLine()) != null) {

						String[] cents = line.split(",");
						centroids += cents[0] + ",";
						sse+=Double.parseDouble(cents[1]);

					}
				}
			}
			System.out.println("Iteration : "+i);
			System.out.println("Centroids : "+centroids);
			System.out.println("SSE = " + sse);




		} while (counter != 0 && i<10);


		return 1;
	}

	public static void main ( final String[] args){
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir> k");
		}

		try {
			ToolRunner.run(new MrKMeans(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}


}