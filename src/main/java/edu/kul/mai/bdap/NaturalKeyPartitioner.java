package edu.kul.mai.bdap;

import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKey, SegmentWritable> {

  @Override
  public int getPartition(CompositeKey key, SegmentWritable value, int numPartitions) {

    // Automatic n-partitioning using hash on the state name
    return Math.abs(key.getId() & Integer.MAX_VALUE) % numPartitions;
  }

}