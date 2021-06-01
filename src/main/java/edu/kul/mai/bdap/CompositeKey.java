package edu.kul.mai.bdap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Composite key formed by a Natural Key (id) and Secondary Key (timestamp).
 * 
 * @author Ioannis Antoniadis
 *
 */
public class CompositeKey implements WritableComparable<CompositeKey> {

  private int id; // natural key
  private long timestamp; // secondary key

  public CompositeKey() {
  }

  public CompositeKey(int id, long timestamp) {
    super();
    this.set(id, timestamp);
  }

  public void set(int id, long timestamp) {
    this.id = id;
    this.timestamp = timestamp;
  }

  public int getId() {
    return this.id;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = in.readInt();
    this.timestamp = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.id);
    out.writeLong(this.timestamp);
  }

  @Override
  /**
   * This comparator controls the sort order of the keys.
   */
  public int compareTo(CompositeKey other) {
    if (this.id != other.id) {
      return this.id < other.id ? -1 : 1;
    } else if (this.timestamp != other.timestamp) {
      return timestamp < other.timestamp ? -1 : 1;
    } else {
      return 0;
    }
  }

}
