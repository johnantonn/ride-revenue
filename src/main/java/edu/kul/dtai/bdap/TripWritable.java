package edu.kul.dtai.bdap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

public class TripWritable implements WritableComparable<TripWritable> {

  private int id = -1;
  private long startTimestamp;
  private long endTimestamp;
  private int numStops = 0;
  private List<Point2D> stops = new ArrayList<Point2D>();

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = in.readInt();
    this.startTimestamp = in.readLong();
    this.endTimestamp = in.readLong();
    this.numStops = in.readInt();
    for (int i = 0; i < numStops; i++) {
      Point2D stop = new Point2D();
      stop.setLatitude(in.readDouble());
      stop.setLongitude(in.readDouble());
      this.stops.add(stop);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.id);
    out.writeLong(this.startTimestamp);
    out.writeLong(this.endTimestamp);
    out.writeInt(this.numStops);
    for (int i = 0; i < this.numStops; i++) {
      out.writeDouble(this.stops.get(i).getLatitude());
      out.writeDouble(this.stops.get(i).getLongitude());
    }
  }

  public int getId() {
    return this.id;
  }

  public long getStartTimestamp() {
    return this.startTimestamp;
  }

  public long getEndTimestamp() {
    return this.endTimestamp;
  }

  public int getNumStops() {
    return this.numStops;
  }

  public List<Point2D> getStops() {
    return this.stops;
  }

  public void setId(int id) {
    this.id = id;
  }

  public void setStartTimestamp(long t) {
    this.startTimestamp = t;
  }

  public void setEndTimestamp(long t) {
    this.endTimestamp = t;
  }

  public void addStop(Point2D stop) {
    this.stops.add(stop);
    this.numStops++;
  }

  public String stopsToCsvString() {
    String result = "";
    for (Point2D p : this.stops) {
      result += "," + p.getLatitude() + "," + p.getLongitude();
    }
    return result;
  }

  @Override
  public int compareTo(TripWritable o) {
    if (this.id == o.id) {
      return 0;
    } else {
      if (this.id < o.id) {
        return -1;
      } else {
        return 1;
      }
    }
  }

  @Override
  public String toString() {
    return String.format("%d,%d,%d,%d%s", this.id, this.startTimestamp, this.endTimestamp, this.numStops,
        this.stopsToCsvString());
  }

}