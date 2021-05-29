package edu.kul.dtai.bdap;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
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
        private TripWritable trip = null;

        public void reduce(CompositeKey key, Iterable<SegmentWritable> segments, Context context)
                throws IOException, InterruptedException {
            int tripCounter = 0;

            for (SegmentWritable seg : segments) {
                // System.out.println(seg.toString());
                if (seg.getStartStatus() == true && seg.getEndStatus() == true) {
                    // Existing trip continues
                    if (trip != null) {
                        trip.addStop(seg.getStartPoint());
                    }
                } else if (seg.getStartStatus() == false && seg.getEndStatus() == true) {
                    // New trip begins
                    if (trip != null)
                        tripCounter--;
                    trip = new TripWritable();
                    trip.setId(++tripCounter);
                    trip.setStartTimestamp(seg.getEndTimestamp());
                    trip.addStop(seg.getEndPoint());
                } else if (seg.getStartStatus() == true && seg.getEndStatus() == false) {
                    // Existing trip ends
                    if (trip != null) {
                        trip.setEndTimestamp(seg.getStartTimestamp());
                        trip.addStop(seg.getStartPoint());
                        id = new IntWritable(key.getId());
                        context.write(id, trip);
                        trip = null;
                    }
                } else {
                    System.out.println("Error: Broken status sequence!");
                    break;
                }

            }

        }
    }

    public static class AirportRidesMapper extends Mapper<Object, Text, Text, TripWritable> {

        private Point2D airportLocation = new Point2D(37.62131, -122.37896);
        private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        private TripWritable trip;
        private final static double minDist = 1; // 1km

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            trip = new TripWritable();
            trip.parseLine(value.toString());
            for (Point2D stop : trip.getStops()) {
                double dist = distGPS(airportLocation, stop);
                if (dist < minDist) {
                    Text date = new Text(this.simpleDateFormat.format(new Date(trip.getStartTimestamp())));
                    context.write(date, trip);
                    break;
                }
            }
        }

    }

    public static class RevenueCalculatorReducer extends Reducer<Text, TripWritable, Text, DoubleWritable> {

        private final static double constFee = 3.5; // constant fee, USD
        private final static double addFee = 1.71; // additional charge per km

        public void reduce(Text dt, Iterable<TripWritable> trips, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            DoubleWritable result = new DoubleWritable(0);
            for (TripWritable trip : trips) {
                List<Point2D> stops = trip.getStops();
                for (int i = 1; i < trip.getNumStops(); i++) {
                    sum += distGPS(stops.get(i), stops.get(i - 1));
                }
            }

            result.set(constFee + sum * addFee);
            context.write(dt, result);
        }

    }

    public static Job runTripConstructor(Path input, Path output) throws Exception {

        Job job = Job.getInstance(new Configuration(), "Construct trips");
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

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }

    public static Job runRevenueCalculator(Path input, Path output) throws Exception {

        Job job = Job.getInstance(new Configuration(), "Compute ride revenue");
        job.setJarByClass(RideRevenueCalculator.class);

        // Mapper configuration
        job.setMapperClass(AirportRidesMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TripWritable.class);

        // Reducer configuration
        job.setReducerClass(RevenueCalculatorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }

    public static double distGPS(Point2D p1, Point2D p2) {
        double R = 6371.009; // earth's radius in km
        double dPhi = Math.PI * (p2.getLatitude() - p1.getLatitude()) / 180;
        double dLambda = Math.PI * (p2.getLongitude() - p1.getLongitude()) / 180;
        double dist = R * Math.sqrt(Math.pow(dPhi, 2) + Math.pow(dLambda, 2));
        return dist;
    }

    public static void main(String[] args) throws Exception {

        Path input = new Path(args[0]);
        Path output1 = new Path(args[1], "pass1");

        // Job 1
        Job tripConstructorJob = runTripConstructor(input, output1);
        if (!tripConstructorJob.waitForCompletion(true)) {
            System.exit(1);
        }

        // Job 2
        // Take output1 (trips) and compute revenue
        // Mapper will filter airport rides
        // Reducer will compute distance and revenue
        Job revenueCalculatorJob = runRevenueCalculator(output1, new Path(args[1], "pass2"));
        if (!revenueCalculatorJob.waitForCompletion(true)) {
            System.exit(2);
        }

    }
}
