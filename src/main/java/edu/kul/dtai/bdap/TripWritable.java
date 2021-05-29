package edu.kul.dtai.bdap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class TripWritable implements Writable {

  private int id;
  private long startTimestamp;
  private long endTimestamp;
  private int numStops;
  private List<Point2D> stops;

  public TripWritable() {
    this.id = -1;
    this.numStops = 0;
    this.stops = new ArrayList<Point2D>();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = in.readInt();
    this.startTimestamp = in.readLong();
    this.endTimestamp = in.readLong();
    this.numStops = in.readInt();
    for (int i = 0; i < numStops; i++) {
      Point2D stop = new Point2D(in.readDouble(), in.readDouble());
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
    this.stops.add(new Point2D(stop.getLatitude(), stop.getLongitude()));
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
  public String toString() {
    return String.format("%d,%d,%d,%d%s", this.id, this.startTimestamp, this.endTimestamp, this.numStops,
        this.stopsToCsvString());
  }

  public void parseLine(String line) throws IOException {

    // Split on commas, unless they were escaped with a backslash
    // Negative limit param "-1" to keep empty values in resulting array
    String[] parts = line.split("\t")[1].split(",");

    this.id = StringHelper.parseInt(parts[0]);
    this.startTimestamp = StringHelper.parseLong(parts[1]);
    this.endTimestamp = StringHelper.parseLong(parts[2]);
    this.numStops = StringHelper.parseInt(parts[3]);
    for (int i = 0; i < this.numStops; i++) {
      this.stops
          .add(new Point2D(StringHelper.parseDouble(parts[4 + 2 * i]), StringHelper.parseDouble(parts[4 + 2 * i + 1])));
    }
  }

}