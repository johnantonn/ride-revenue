package edu.kul.dtai.bdap;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class NaturalKeyPartitioner extends Partitioner<CompositeKey, Text> {

  @Override
  public int getPartition(CompositeKey key, Text value, int numPartitions) {

    // Automatic n-partitioning using hash on the state name
    return Math.abs(key.getId() & Integer.MAX_VALUE) % numPartitions;
  }

}