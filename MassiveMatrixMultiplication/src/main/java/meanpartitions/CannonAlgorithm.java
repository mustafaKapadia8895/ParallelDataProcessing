package meanpartitions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class CannonAlgorithm extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(CannonAlgorithm.class);

    private static final Integer P = 4; // number of regions
    private static final Integer A_ROW = 100;
    private static final Integer A_COL = 100;
    private static final Integer B_ROW = 100;
    private static final Integer B_COL = 100;

    private static Pair<Integer, Integer> getBlockCoordinates(SparseEncoding data) {
        int i,j;
        if (data.getSrc() == 'A') {
            // find i,j for block
            i = data.getRow() / (A_ROW / (int) Math.sqrt(P));
            j = data.getCol() / (A_COL / (int) Math.sqrt(P));

        } else if (data.getSrc() == 'B') {
            // find i,j for block
            i = data.getRow() / (B_ROW / (int) Math.sqrt(P));
            j = data.getCol() / (B_COL / (int) Math.sqrt(P));

        } else {
            // src == 'C'
            i = data.getRow() / (A_ROW / (int) Math.sqrt(P));
            j = data.getCol() / (B_COL / (int) Math.sqrt(P));
        }
        return new Pair(i, j);
    }

//    @Deprecated
//    private static CannonKey setupKey(SparseEncoding data) {
//        CannonKey key = new CannonKey();
//        // find i,j of block
//        Pair<Integer, Integer> p = getBlockCoordinates(data);
//        int i = p.getKey();
//        int j = p.getValue();
//
//        if (data.getSrc() == 'A') {
//            // send to respective partition
//            key.setPartitionRow(i);
//            key.setPartitionCol((i+j) % (int) Math.sqrt(P));
//        } else if (data.getSrc() == 'B') {
//            // send to respective partition
//            key.setPartitionRow((i+j) % (int) Math.sqrt(P));
//            key.setPartitionCol(j);
//        } else {
//            // src == 'C'
//            key.setPartitionRow(i);
//            key.setPartitionCol(j);
//        }
//        return key;
//    }

    private static CannonKey alignBlockForFirstIteration(Pair<Integer, Integer> blockCoordinates, SparseEncoding data) {
        CannonKey key = new CannonKey();
        // i,j of block
        int i = blockCoordinates.getKey();
        int j = blockCoordinates.getValue();

        if (data.getSrc() == 'A') {
            // send to respective partition
            key.setPartitionRow(i);
            key.setPartitionCol((i+j) % (int) Math.sqrt(P));
        } else if (data.getSrc() == 'B') {
            // send to respective partition
            key.setPartitionRow((i+j) % (int) Math.sqrt(P));
            key.setPartitionCol(j);
        } else {
            // src == 'C'
            key.setPartitionRow(i);
            key.setPartitionCol(j);
        }
        return key;
    }

    /**
     * Blow up row and col of data to adjust to the corresponding position in the block
     * @param key
     * @param data
     * @return
     */
    private static SparseEncoding adjustData(CannonKey key, Pair<Integer, Integer> blockCoordinates, SparseEncoding data) {
        if (Objects.equals(blockCoordinates.getKey(), key.getPartitionRow()) &&
            Objects.equals(blockCoordinates.getValue(), key.getPartitionCol())) {
            // data was not shifted, so return key as-is
            return data;
        }
        // data was shifted..

        int i, j, val_per_row, val_per_col;

        if (data.getSrc() == 'A') {
            // reduce coordinates of data to base of 0,0
            val_per_row = A_ROW / (int) Math.sqrt(P);
            val_per_col = A_COL / (int) Math.sqrt(P);

            i = data.getRow() % val_per_row;
            j = data.getCol() % val_per_col;

        } else if (data.getSrc() == 'B') {
            // reduce coordinates of data to base of 0,0
            val_per_row = B_ROW / (int) Math.sqrt(P);
            val_per_col = B_COL / (int) Math.sqrt(P);

            i = data.getRow() % val_per_row;
            j = data.getCol() % val_per_col;

        } else {
            // src == 'C'
            // reduction isn't required, because I said so!!
            // blowing up isn't required cuz nothing really matters!! - Metallica (cuz references matter)
            throw new IllegalArgumentException();
        }

        // blow up reduced coordinates to match final coordinates of resultant block
        data.setRow((key.getPartitionRow() * val_per_row) + i);
        data.setCol((key.getPartitionCol() * val_per_col) + j);

        return data;
    }

    private static Pair<CannonKey, SparseEncoding> firstIterMap(SparseEncoding data) {
        if (data.getSrc() == 'A') {
            Pair<Integer, Integer> blockCoordinates = getBlockCoordinates(data);
            CannonKey key = alignBlockForFirstIteration(blockCoordinates, data);
            // update row and column of data correspond to the assigned block
            data = adjustData(key, blockCoordinates, data);
            return new Pair<>(key, data);
        } else if (data.getSrc() == 'B') {
            Pair<Integer, Integer> blockCoordinates = getBlockCoordinates(data);
            CannonKey key = alignBlockForFirstIteration(blockCoordinates, data);
            // update row and column of data correspond to the assigned block
            data = adjustData(key, blockCoordinates, data);
            return new Pair<>(key, data);
        } else {
            Pair<Integer, Integer> blockCoordinates = getBlockCoordinates(data);
            CannonKey key = new CannonKey();
            key.setPartitionRow(blockCoordinates.getKey());
            key.setPartitionCol(blockCoordinates.getValue());
            return new Pair<>(key, data);
        }
    }

    private static Pair<CannonKey, SparseEncoding> secondIterMap(SparseEncoding data) {
        if (data.getSrc() == 'A') {
            // find which region it fits in
            Pair<Integer, Integer> blockCoordinates = getBlockCoordinates(data);
            CannonKey key = new CannonKey();
            key.setPartitionRow(blockCoordinates.getKey());
            key.setPartitionCol(blockCoordinates.getValue());
            // roll left block
            key.setPartitionCol( Math.floorMod(key.getPartitionCol() - 1,  (int) Math.sqrt(P)));
            data = adjustData(key, blockCoordinates, data);
            return new Pair<>(key, data);
        } else if (data.getSrc() == 'B') {
            // find which region it fits in
            Pair<Integer, Integer> blockCoordinates = getBlockCoordinates(data);
            CannonKey key = new CannonKey();
            key.setPartitionRow(blockCoordinates.getKey());
            key.setPartitionCol(blockCoordinates.getValue());
            // roll up block
            key.setPartitionRow( Math.floorMod(key.getPartitionRow() - 1,  (int) Math.sqrt(P)));
            data = adjustData(key, blockCoordinates, data);
            return new Pair<>(key, data);
        } else {
            Pair<Integer, Integer> blockCoordinates = getBlockCoordinates(data);
            CannonKey key = new CannonKey();
            key.setPartitionRow(blockCoordinates.getKey());
            key.setPartitionCol(blockCoordinates.getValue());
            return new Pair<>(key, data);
        }
    }

    public static class CannonFirstIterMapper extends Mapper<Object, Text, CannonKey, SparseEncoding> {

        @Override
        public void map(final Object obj, final Text value, final Context context) throws IOException, InterruptedException {
            SparseEncoding data = new SparseEncoding(value.toString());
            Pair<CannonKey, SparseEncoding> kv = firstIterMap(data);
            context.write(kv.getKey(), kv.getValue());
        }
    }

    public static class CannonSecondIterMapper extends Mapper<Object, Text, CannonKey, SparseEncoding> {

        @Override
        public void map(final Object obj, final Text value, final Context context) throws IOException, InterruptedException {
            SparseEncoding data = new SparseEncoding(value.toString());
            Pair<CannonKey, SparseEncoding> kv = secondIterMap(data);
            context.write(kv.getKey(), kv.getValue());
        }
    }

//    /**
//     * Mapper for left matrix for the 1st iteration of Cannon Algorithm
//     *
//     * Required information:
//     * i,j =  row and col indices for the item
//     * p = number of block partitions (has to be a perfect square!!)
//     *
//     * Send the block A_ij to C_lm
//     *  where:
//     *      l  = i
//     *      m  = (i + j) mod sqrt(p)
//     */
//    public static class CannonFirstIterMapper_A extends Mapper<Object, Text, CannonKey, SparseEncoding> {
//
//        @Override
//        public void map(final Object obj, final Text value, final Context context) throws IOException, InterruptedException {
//            SparseEncoding data = new SparseEncoding(value.toString());
//            data.setSrc('A');
//
//            Pair<CannonKey, SparseEncoding> kv = firstIterMap(data);
//
//            context.write(kv.getKey(), kv.getValue());
//        }
//    }
//
//    /**
//     * Mapper for right matrix for the 1st iteration of Cannon Algorithm
//     *
//     * Required information:
//     * i,j =  row and col indices for the item
//     * p = number of block partitions (has to be a perfect square!!)
//     *
//     * Send the block A_ij to C_lm
//     *  where:
//     *      l  = (i + j) mod sqrt(p)
//     *      m  = j
//     */
//    public static class CannonFirstIterMapper_B extends Mapper<Object, Text, CannonKey, SparseEncoding> {
//
//        @Override
//        public void map(final Object obj, final Text value, final Context context) throws IOException, InterruptedException {
//            SparseEncoding data = new SparseEncoding(value.toString());
//            data.setSrc('B');
//            Pair<CannonKey, SparseEncoding> kv = firstIterMap(data);
//            context.write(kv.getKey(), kv.getValue());
//        }
//    }
//
//    /**
//     * Mapper for left matrix for the 2nd iteration of Cannon Algorithm
//     *
//     * Required information:
//     * i,j = row and col indices for the item
//     * p = number of block partitions (has to be a perfect square!!)
//     *
//     * Left shift all the blocks in A
//     */
//    public static class CannonSecondIterMapper_A extends Mapper<Object, Text, CannonKey, SparseEncoding> {
//
//        @Override
//        public void map(final Object obj, final Text value, final Context context) throws IOException, InterruptedException {
//            SparseEncoding data = new SparseEncoding(value.toString());
//            data.setSrc('A');
//            Pair<CannonKey, SparseEncoding> kv = secondIterMap(data);
//            context.write(kv.getKey(), kv.getValue());
//        }
//    }
//
//    /**
//     * Mapper for right matrix for the 2nd iteration of Cannon Algorithm
//     *
//     * Required information:
//     * i,j =  row and col indices for the item
//     * p = number of block partitions (has to be a perfect square!!)
//     *
//     * Upward shift all the blocks in B
//     */
//    public static class CannonSecondIterMapper_B extends Mapper<Object, Text, CannonKey, SparseEncoding> {
//
//        @Override
//        public void map(final Object obj, final Text value, final Context context) throws IOException, InterruptedException {
//            SparseEncoding data = new SparseEncoding(value.toString());
//            data.setSrc('B');
//            Pair<CannonKey, SparseEncoding> kv = secondIterMap(data);
//            context.write(kv.getKey(), kv.getValue());
//        }
//    }
//
//    /**
//     * Mapper for the partial result matrix C
//     * Simply pass-through to correct reducer
//     */
//    public static class CannonMapper_C extends Mapper<Object, Text, CannonKey, SparseEncoding> {
//
//        @Override
//        public void map(final Object obj, final Text value, final Context context) throws IOException, InterruptedException {
//            SparseEncoding data = new SparseEncoding(value.toString());
//            data.setSrc('C');
//            Pair<CannonKey, SparseEncoding> kv = firstIterMap(data);
//            context.write(kv.getKey(), kv.getValue());
//        }
//    }

    /**
     * Receives 2 sub-matrices, say A_ij and B_ij
     * C_ij = (sequential) matrix multiplication of A_ij and B_ij sub-matrices
     *
     * Using Hashmaps for SPARSE OPTIMIZATION!!!!!!
     * Ain't no one got time to store them stupid ass zeros -\_(-.-)_-
     */
    public static class CannonReducer extends Reducer<CannonKey, SparseEncoding, SparseEncoding, NullWritable> {
        // TODO: do we need a grouping comparator??

//        @Deprecated
//        private static Pair<Integer, Integer> adjustKey(CannonKey key, SparseEncoding data) {
//            Pair<Integer, Integer> pair;
//            int i, j, val_per_row, val_per_col;
//
//            if (data.getSrc() == 'A') {
//                // reduce coordinates of data to base of 0,0
//                val_per_row = A_ROW / (int) Math.sqrt(P);
//                val_per_col = A_COL / (int) Math.sqrt(P);
//
//                i = data.getRow() % val_per_row;
//                j = data.getCol() % val_per_col;
//
//                // blow up reduced coordinates to match final coordinates of resultant block
//                pair = new Pair<>((key.getPartitionRow() * val_per_row) + i, (key.getPartitionCol() * val_per_col) + j);
//
//            } else if (data.getSrc() == 'B') {
//                // reduce coordinates of data to base of 0,0
//                val_per_row = B_ROW / (int) Math.sqrt(P);
//                val_per_col = B_COL / (int) Math.sqrt(P);
//
//                i = data.getRow() % val_per_row;
//                j = data.getCol() % val_per_col;
//
//                // blow up reduced coordinates to match final coordinates of resultant block
//                pair = new Pair<>((key.getPartitionRow() * val_per_row) + i, (key.getPartitionCol() * val_per_col) + j);
//
//            } else {
//                // src == 'C'
//                // reduction isn't required, because I said so!!
//                // blowing up isn't required cuz nothing really matters!! - Metallica (cuz references matter)
//                pair = new Pair<>(data.getRow(), data.getCol());
//            }
//
//            return pair;
//        }

        private static Pair<Integer, Integer> reduceKey(CannonKey key, SparseEncoding data) {
            // reduce coordinates of data to base of 0,0

            Pair<Integer, Integer> pair;
            int i, j, val_per_row, val_per_col;

            if (data.getSrc() == 'A') {
                // reduce coordinates of data to base of 0,0
                val_per_row = A_ROW / (int) Math.sqrt(P);
                val_per_col = A_COL / (int) Math.sqrt(P);

                i = data.getRow() % val_per_row;
                j = data.getCol() % val_per_col;

                pair = new Pair<>(i, j);

            } else if (data.getSrc() == 'B') {
                // reduce coordinates of data to base of 0,0
                val_per_row = B_ROW / (int) Math.sqrt(P);
                val_per_col = B_COL / (int) Math.sqrt(P);

                i = data.getRow() % val_per_row;
                j = data.getCol() % val_per_col;

                pair = new Pair<>(i, j);

            } else {
                // src == 'C'
                val_per_row = A_ROW / (int) Math.sqrt(P);
                val_per_col = B_COL / (int) Math.sqrt(P);

                i = data.getRow() % val_per_row;
                j = data.getCol() % val_per_col;

                pair = new Pair<>(i, j);
            }

            return pair;

        }

        private static Pair<Integer, Integer> blowupKey_C(CannonKey key, Pair<Integer, Integer> p) {
            // blow up reduced coordinates to match final coordinates of resultant block

            int val_per_row, val_per_col;

            val_per_row = A_ROW / (int) Math.sqrt(P);
            val_per_col = B_COL / (int) Math.sqrt(P);

            // blow up reduced coordinates to match final coordinates of resultant block
            return new Pair<>((key.getPartitionRow() * val_per_row) + p.getKey(), (key.getPartitionCol() * val_per_col) + p.getValue());
        }

        @Override
        public void reduce(final CannonKey key, final Iterable<SparseEncoding> values, final Context context)
                throws IOException, InterruptedException {
            Map<Pair<Integer, Integer>, Integer> A_map = new HashMap<>();
            Map<Pair<Integer, Integer>, Integer> B_map = new HashMap<>();
            Map<Pair<Integer, Integer>, Integer> C_map = new HashMap<>();

            for (SparseEncoding data : values) {
                if (data.getSrc() == 'A') {
                    context.write(data, null); // pass on A for next iteration
                    A_map.put(reduceKey(key, data), data.getVal());
                } else if (data.getSrc() == 'B') {
                    context.write(data, null); // pass on B for next iteration
                    B_map.put(reduceKey(key, data), data.getVal());
                } else {
                    // src == 'C'
                    C_map.put(reduceKey(key, data), data.getVal());
                }
            }

            int val_per_row = A_ROW / (int) Math.sqrt(P);
            int val_per_col = B_COL / (int) Math.sqrt(P);

            // multiply A and B
            for (int i = 0; i < val_per_row; i++) {

                // this is wrong, but assuming square sub-matrices it's FINE - should look at cols of B
                for (int j = 0; j < val_per_col; j++) {

                    // this is wrong, but assuming square sub-matrices it's FINE - should look at rows of B
                    for (int k = 0; k < val_per_col; k++) {

                        C_map.put(
                                new Pair<>(i, j),
                                C_map.getOrDefault(new Pair<>(i, j), 0) +
                                        (A_map.getOrDefault(new Pair<>(i, k), 0) *
                                                B_map.getOrDefault(new Pair<>(k, j), 0))
                        );
                    }
                }
            }

            for (Map.Entry<Pair<Integer, Integer>, Integer> val : C_map.entrySet()) {
                if (val.getValue() != 0) {
                    Pair<Integer, Integer> p = blowupKey_C(key, val.getKey());
                    context.write(
                            new SparseEncoding(
                                    p.getKey(),
                                    p.getValue(),
                                    val.getValue(),
                                    'C'
                            ),
                            null
                    );
                }
            }
        }
    }

    /**
     * Custom partitioner class
     */
    public static class CannonKeyPartitioner extends Partitioner<CannonKey, SparseEncoding> {
        @Override
        public int getPartition(CannonKey key, SparseEncoding value, int numPartitions) {
            int partitionId = (((int) Math.sqrt(P)) * key.getPartitionRow())  + key.getPartitionCol();
            logger.debug(
                    "key: " + key.getPartitionRow() + "," + key.getPartitionCol() +
                            " value: " + value.toString() +
                            " partitionid: " + partitionId
            );
            return partitionId;
        }
    }

    @Override
    public int run(final String[] args) throws Exception {

        logger.setLevel(Level.INFO);

        int iteration = 0;
        String ip, op_iter1, op_iter2;
        do {
            if(iteration > 0) {
                ip = args[1]+(iteration-1)+"/phase-2";
                op_iter1 = args[1]+iteration+"/phase-1";
                op_iter2 = args[1]+iteration+"/phase-2";
            } else {
                ip = args[0];
                op_iter1 = args[1]+iteration+"/phase-1";
                op_iter2 = args[1]+iteration+"/phase-2";
            }

            final Configuration conf = getConf();

            final Job job = Job.getInstance(conf, this.getClass().getName());
            job.setJarByClass(this.getClass());
            final Configuration jobConf = job.getConfiguration();
            jobConf.set("mapreduce.output.textoutputformat.separator", "");

            FileInputFormat.addInputPath(job, new Path(ip+"/part-*"));

            job.setMapOutputKeyClass(CannonKey.class);
            job.setMapOutputValueClass(SparseEncoding.class);

            job.setMapperClass(CannonFirstIterMapper.class);
            job.setReducerClass(CannonReducer.class);

            job.setPartitionerClass(CannonKeyPartitioner.class);

            job.setNumReduceTasks(P);

            job.setOutputKeyClass(SparseEncoding.class);
            job.setOutputValueClass(NullWritable.class);

            FileOutputFormat.setOutputPath(job, new Path(op_iter1));

            if(!job.waitForCompletion(true)) {
                return 1;
            }

            // iter - 2

            final Job job2 = Job.getInstance(conf, this.getClass().getName());
            job2.setJarByClass(this.getClass());

            jobConf.set("mapreduce.output.textoutputformat.separator", "");


            FileInputFormat.addInputPath(job2, new Path(op_iter1+"/part-*"));


            job2.setMapOutputKeyClass(CannonKey.class);
            job2.setMapOutputValueClass(SparseEncoding.class);

            job2.setMapperClass(CannonSecondIterMapper.class);
            job2.setReducerClass(CannonReducer.class);

            job2.setPartitionerClass(CannonKeyPartitioner.class);

            job2.setNumReduceTasks(P);

            job2.setOutputKeyClass(SparseEncoding.class);
            job2.setOutputValueClass(NullWritable.class);

            FileOutputFormat.setOutputPath(job2, new Path(op_iter2));

            if (!job2.waitForCompletion(true)) {
                return 1;
            }

            iteration += 1;
        } while (iteration < Math.sqrt(P)/2);

        return 0;
    }
}