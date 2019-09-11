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
    public static class TokenizerMapper1 extends Mapper<Object, Text, NullWritable, NullWritable> {
        HashMap<Integer, ArrayList<Integer>> h = new HashMap<>();
        long path2=0;
        @Override
        public void setup(Mapper<Object, Text, NullWritable, NullWritable>.Context context) throws IOException, InterruptedException {
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
                            if(from<=500000 && to<=500000) //filter
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

            // filter and check hashmap for path 2s
            if(from <=500000 && to<=500000)
            for(Integer x : h.getOrDefault(to, new ArrayList<>()))
            {

                path2+=1;

            }

        }

        // Increment global counter
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.getCounter(UpdateCounter.UPDATED).increment(path2);
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
        job1.setMapperClass(TokenizerMapper1.class);
        //job.setCombinerClass(IntSumReducer.class);
        job1.setNumReduceTasks(0);
        job1.setMapOutputKeyClass(NullWritable.class);
        job1.setMapOutputValueClass(NullWritable.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(NullWritable.class);
        Path path = new Path(args[0]+"/edges.csv");
        FileInputFormat.addInputPath(job1, path);
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.addCacheFile(new URI(path.toString()+"#edges"));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Retrieve global counter

        long path2=job1.getCounters().findCounter(UpdateCounter.UPDATED).getValue();

        System.out.println("*****************************************");
        System.out.println("path 2's = " +path2);

        return 0;


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