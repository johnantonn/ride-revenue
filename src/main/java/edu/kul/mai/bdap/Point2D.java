package edu.kul.mai.bdap;

public class Point2D {
  private double latitude, longitude;

  public Point2D() {

  }

  public Point2D(double lat, double lon) {
    setLatitude(lat);
    setLongitude(lon);
  }

  public double getLatitude() {
    return this.latitude;
  }

  public double getLongitude() {
    return this.longitude;
  }

  public void setLatitude(double lat) {
    this.latitude = lat;
  }

  public void setLongitude(double lon) {
    this.longitude = lon;
  }

  @Override
  public String toString() {
    String result = "[" + this.getLatitude() + "," + this.getLongitude() + "]";
    return result;
  }

}
