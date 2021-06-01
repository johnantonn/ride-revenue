package edu.kul.mai.bdap;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

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
            try {
                // Parse segment
                segment.parseLine(value.toString());
                // Check if segment is valid
                if (isValid(segment)) {
                    compositeKey.set(segment.getId(), segment.getStartTimestamp());
                    context.write(compositeKey, segment);
                }
            } catch (Exception e) {
                System.out.println(e);
            }
        }

        private boolean isValid(SegmentWritable seg) {
            if (seg.getStartStatus() || seg.getEndStatus()) {
                return (seg.getStartPoint().getLongitude() < -100) && (seg.getEndPoint().getLongitude() < -100)
                        && (seg.getEndTimestamp() - seg.getStartTimestamp() < 1000 * 3600);
            } else {
                return false;
            }
        }
    }

    public static class TripConstructorReducer
            extends Reducer<CompositeKey, SegmentWritable, IntWritable, TripWritable> {

        private IntWritable id;
        private TripWritable trip = null;

        public void reduce(CompositeKey key, Iterable<SegmentWritable> segments, Context context)
                throws IOException, InterruptedException {

            // Helper variables
            int tripCounter = 0;
            SegmentWritable prevSeg = new SegmentWritable();

            // Loop over sorted segments
            for (SegmentWritable seg : segments) {
                // Middle segment (trip continues)
                if (seg.getStartStatus() == true && seg.getEndStatus() == true) {
                    if (trip != null) {
                        if (seg.getStartTimestamp() > prevSeg.getEndTimestamp()) {
                            trip.addStop(seg.getStartPoint());
                        }
                        // Check speed
                        if (speed(seg) > 200) {
                            trip = null; // invalidate trip
                            continue;
                        }
                        trip.addStop(seg.getEndPoint());
                        prevSeg = seg;
                    }
                }
                // Start segment (trip starts)
                else if (seg.getStartStatus() == false && seg.getEndStatus() == true) {
                    if (trip != null)
                        tripCounter--;
                    trip = new TripWritable();
                    trip.setId(++tripCounter);
                    trip.setStartTimestamp(seg.getEndTimestamp());
                    trip.addStop(seg.getEndPoint());
                    prevSeg = seg;
                }
                // End segment (trip ends)
                else if (seg.getStartStatus() == true && seg.getEndStatus() == false) {
                    if (trip != null) {
                        trip.setEndTimestamp(seg.getStartTimestamp());
                        if (seg.getStartTimestamp() > prevSeg.getEndTimestamp()) {
                            trip.addStop(seg.getStartPoint());
                        }
                        id = new IntWritable(key.getId());
                        context.write(id, trip);
                        trip = null;
                    }
                }
                // Error case
                else {
                    System.out.println("Error: Broken status sequence!");
                    trip = null; // invalidate trip
                }

            }

        }

        private double speed(SegmentWritable seg) {
            double dist = flatSurfDist(seg.getStartPoint(), seg.getEndPoint());
            double td = (double) (seg.getEndTimestamp() - seg.getStartTimestamp()) / 3600000;
            return dist / td;
        }
    }

    public static class AirportRidesMapper extends Mapper<Object, Text, Text, TripWritable> {

        private Point2D airportLocation = new Point2D(37.62131, -122.37896); // SFO gps coordinates
        private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        private static TimeZone timeZoneLA = TimeZone.getTimeZone("America/Los_Angeles");
        private TripWritable trip; // trip object to be used iteratively
        private final static double minDist = 1; // 1km

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                // Parse trip
                trip = new TripWritable();
                trip.parseLine(value.toString());
                dateFormat.setTimeZone(timeZoneLA);
                for (Point2D stop : trip.getStops()) {
                    double dist = flatSurfDist(airportLocation, stop); // distance from the airport
                    // Check if airport ride
                    if (dist < minDist) {
                        Text date = new Text(this.dateFormat.format(new Date(trip.getStartTimestamp())));
                        context.write(date, trip);
                        break; // don't look into further stops
                    }
                }
            } catch (Exception e) {
                System.out.println(e);
            }
        }

    }

    public static class RevenueCalculatorReducer extends Reducer<Text, TripWritable, Text, DoubleWritable> {

        private final static double constFee = 3.5; // constant fee, USD
        private final static double costPerKm = 1.71; // additional charge per km

        public void reduce(Text dt, Iterable<TripWritable> trips, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            DoubleWritable result = new DoubleWritable(0);
            // Loop over trips
            for (TripWritable trip : trips) {
                List<Point2D> stops = trip.getStops();
                // Loop over stops
                for (int i = 1; i < trip.getNumStops(); i++) {
                    sum += flatSurfDist(stops.get(i), stops.get(i - 1)); // sum distance
                }
            }
            // Calculate total trip cost
            result.set(constFee + sum * costPerKm);
            context.write(dt, result);
        }

    }

    public static Job runTripConstructor(Path input, Path output) throws Exception {

        // Job definition
        Job job = Job.getInstance(new Configuration(), "Trip Construction");
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
        job.setNumReduceTasks(1);

        // Input/Output paths
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }

    public static Job runRevenueCalculator(Path input, Path output) throws Exception {

        // Job definition
        Job job = Job.getInstance(new Configuration(), "Revenue Calculation");
        job.setJarByClass(RideRevenueCalculator.class);

        // Mapper configuration
        job.setMapperClass(AirportRidesMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TripWritable.class);

        // Reducer configuration
        job.setReducerClass(RevenueCalculatorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(1);

        // Input/Output paths
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }

    public static double flatSurfDist(Point2D p1, Point2D p2) {
        double R = 6371.009; // earth's radius in km
        double phi1 = Math.toRadians(p1.getLatitude()); // lat1 in rads
        double phi2 = Math.toRadians(p2.getLatitude()); // lat2 in rads
        double dPhi = Math.toRadians(p2.getLatitude() - p1.getLatitude()); // lat diff in rads
        double dLambda = Math.toRadians(p2.getLongitude() - p1.getLongitude()); // lon diff in rads
        double a = Math.sin(dPhi / 2) * Math.sin(dPhi / 2)
                + Math.sin(dLambda / 2) * Math.sin(dLambda / 2) * Math.cos(phi1) * Math.cos(phi2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }

    public static void main(String[] args) throws Exception {

        // Input/Output paths
        Path input = new Path(args[0]);
        Path output1 = new Path(args[1], "pass1");

        // Job 1
        long t11 = System.currentTimeMillis();
        Job tripConstructorJob = runTripConstructor(input, output1);
        if (!tripConstructorJob.waitForCompletion(true)) {
            System.exit(1);
        }
        long t12 = System.currentTimeMillis();
        System.out.println("Job 1 finished in: " + (t12 - t11) / 1000 + " seconds.");

        // Job 2
        long t21 = System.currentTimeMillis();
        Job revenueCalculatorJob = runRevenueCalculator(output1, new Path(args[1], "pass2"));
        if (!revenueCalculatorJob.waitForCompletion(true)) {
            System.exit(2);
        }
        long t22 = System.currentTimeMillis();
        System.out.println("Job 2 finished in: " + (t22 - t21) / 1000 + " seconds.");

    }
}
