package tw;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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


public class TwitterFollowers extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(TwitterFollowers.class);

    // Global Counter
    public static enum UpdateCounter {
		UPDATED
	}
    public static class TokenizerMapper2 extends Mapper<Object, Text, NullWritable, NullWritable> {
        HashMap<Integer, ArrayList<Integer>> h = new HashMap<>();
        int triangles=0;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();

            // create hashmap
            if (cacheFiles != null && cacheFiles.length > 0) {
                try {
                        File edges = new File("edges");
                        FileInputStream f = new FileInputStream(edges);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(f));
                        String line;
                        while ((line = reader.readLine()) != null)
                        {
                            String a[] = line.split(",");
                            int from = Integer.parseInt(a[0]);
                            int to = Integer.parseInt(a[1]);
                            if (h.containsKey(from)) {
                                h.get(from).add(to);
                            }
                            else
                            {
                                h.put(from, new ArrayList<Integer>(Arrays.asList(to)));
                            }

                        }


                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }

        }


        @Override
        public void map(final Object key, final Text value, final Context context) throws
                IOException, InterruptedException {
            String a[] = value.toString().split(",");
            int from = Integer.parseInt(a[0]);
            int to = Integer.parseInt(a[1]);

            // check hashmap for path 2
            for(Integer x : h.getOrDefault(to, new ArrayList<>()))
            {
                // check hashmap for path 3
                for(Integer y : h.getOrDefault(x, new ArrayList<>()))
                {
                    // check triangle condition
                    if(y==from)
                        triangles+=1;
                }
            }

        }

        // Increment counter
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.getCounter(UpdateCounter.UPDATED).increment(triangles);
            super.cleanup(context);
        }

    }


    public static class TokenizerMapper1 extends Mapper<Object, Text, IntWritable, IntWritable> {
        private final int max = 70000;


        // Max filter
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

            String temp = value.toString();
            String a[] = temp.split(",");
            int f = Integer.parseInt(a[0]);
            int t = Integer.parseInt(a[1]);
            if (f <= max && t <= max)
                context.write(new IntWritable(f), new IntWritable(t));

        }
    }

    // To ensure a single intermediate file is created
    public static class Reducer1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {


        @Override
        public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException
        {
            for(IntWritable i : values)
            {
                context.write(key, i);
            }

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
        job1.setReducerClass(Reducer1.class);
        //job.setCombinerClass(IntSumReducer.class);
        job1.setNumReduceTasks(1);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        final Configuration conf2=getConf();
        Job job2 = Job.getInstance(conf2, "round 2");
        // broadcast cache
        job2.addCacheFile(new URI(args[1]+"/part-r-00000"+"#edges"));

        job2.setJarByClass(TwitterFollowers.class);
        job2.setMapperClass(TokenizerMapper2.class);
        job2.setNumReduceTasks(0);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]+"/part-r-00000"));


        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        // retrieve counter
        long triangles=job2.getCounters().findCounter(UpdateCounter.UPDATED).getValue();

        System.out.println("*****************************************");
        System.out.println("triangles = " +triangles/3.0);

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