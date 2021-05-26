package edu.kul.dtai.bdap;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RideRevenueCalculator {

    public static class CompositeKeyCreationMapper extends Mapper<Object, Text, CompositeKey, SegmentWritable> {

        private CompositeKey compositeKey = new CompositeKey();
        private SegmentWritable segment = new SegmentWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            segment.parseLine(value.toString());
            if (segment.getStartStatus() || segment.getEndStatus()) {
                compositeKey.set(segment.getId(), segment.getStartTimestamp());
                context.write(compositeKey, segment);
            }
        }
    }

    public static class TripConstructorReducer
            extends Reducer<CompositeKey, SegmentWritable, IntWritable, TripWritable> {

        private IntWritable id;
        private TripWritable trip = new TripWritable();
        private int tripCounter = 0;

        public void reduce(CompositeKey key, Iterable<SegmentWritable> segments, Context context)
                throws IOException, InterruptedException {
            for (SegmentWritable seg : segments) {
                if (seg.getStartStatus() == true && seg.getEndStatus() == true) {
                    // Existing trip continues
                    if (trip.getId() != -1) {
                        trip.addStop(seg.getStartPoint());
                    }
                } else if (seg.getStartStatus() == false && seg.getEndStatus() == true) {
                    // New trip begins
                    trip = new TripWritable();
                    trip.setId(++tripCounter);
                    trip.setStartTimestamp(seg.getStartTimestamp());
                    trip.addStop(seg.getEndPoint());
                } else if (seg.getStartStatus() == true && seg.getEndStatus() == false) {
                    // Existing trip ends
                    trip.setEndTimestamp(seg.getEndTimestamp());
                    trip.addStop(seg.getStartPoint());
                    id = new IntWritable(key.getId());
                    context.write(id, trip);
                } else {
                    System.out.println("Broken status sequence...");
                    break;
                }

            }

        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Ride Revenue with Secondary Sorting");
        job.setJarByClass(RideRevenueCalculator.class);

        // Mapper configuration
        job.setMapperClass(CompositeKeyCreationMapper.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(SegmentWritable.class);

        // Partitioning/Sorting/Grouping configuration
        job.setPartitionerClass(NaturalKeyPartitioner.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);
        job.setGroupingComparatorClass(NaturalKeyComparator.class);

        // Reducer configuration
        job.setReducerClass(TripConstructorReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(TripWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
