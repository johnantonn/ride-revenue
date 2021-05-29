package edu.kul.dtai.bdap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SegmentWritable implements Writable {

  private int id;
  private long startTimestamp;
  private long endTimestamp;
  private Point2D startPoint = new Point2D();
  private Point2D endPoint = new Point2D();;
  private boolean startStatus;
  private boolean endStatus;

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = in.readInt();
    this.startTimestamp = in.readLong();
    this.startPoint.setLatitude(in.readDouble());
    this.startPoint.setLongitude(in.readDouble());
    this.startStatus = in.readBoolean();
    this.endTimestamp = in.readLong();
    this.endPoint.setLatitude(in.readDouble());
    this.endPoint.setLongitude(in.readDouble());
    this.endStatus = in.readBoolean();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.id);
    out.writeLong(this.startTimestamp);
    out.writeDouble(this.startPoint.getLatitude());
    out.writeDouble(this.startPoint.getLongitude());
    out.writeBoolean(this.startStatus);
    out.writeLong(this.endTimestamp);
    out.writeDouble(this.endPoint.getLatitude());
    out.writeDouble(this.endPoint.getLongitude());
    out.writeBoolean(this.endStatus);
  }

  @Override
  public String toString() {
    return String.format("%d,%d,%f,%f,%b,%d,%f,%f,%b", this.id, this.startTimestamp, this.startPoint.getLatitude(),
        this.startPoint.getLongitude(), this.startStatus, this.endTimestamp, this.endPoint.getLatitude(),
        this.endPoint.getLongitude(), this.endStatus);
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

  public boolean getStartStatus() {
    return this.startStatus;
  }

  public boolean getEndStatus() {
    return this.endStatus;
  }

  public Point2D getStartPoint() {
    return this.startPoint;
  }

  public Point2D getEndPoint() {
    return this.endPoint;
  }

  public void parseLine(String line) throws IOException {

    // Remove all double-quotes from line
    line = StringHelper.removeSingleQuotes(line);

    // Split on commas, unless they were escaped with a backslash
    // Negative limit param "-1" to keep empty values in resulting array
    String[] parts = line.split("(?<!\\\\),", -1);

    this.id = StringHelper.parseInt(parts[0]);
    this.startTimestamp = StringHelper.parseDate(parts[1]);
    this.startPoint.setLatitude(StringHelper.parseDouble(parts[2]));
    this.startPoint.setLongitude(StringHelper.parseDouble(parts[3]));
    this.startStatus = StringHelper.parseStatus(parts[4]);
    this.endTimestamp = StringHelper.parseDate(parts[5]);
    this.endPoint.setLatitude(StringHelper.parseDouble(parts[6]));
    this.endPoint.setLongitude(StringHelper.parseDouble(parts[7]));
    this.endStatus = StringHelper.parseStatus(parts[8]);
  }

}