package edu.kul.mai.bdap;

/**
 * Represents a 2-dimensional point on the earth surface using its latitude and
 * longitude coordinates
 */
public class Point2D {
  private double latitude, longitude;

  public Point2D() {
  }

  public Point2D(double lat, double lon) {
    setLatitude(lat);
    setLongitude(lon);
  }

  @Override
  public boolean equals(Object o) {
    // If the object is compared with itself then return true

    /*
     * Check if o is an instance of Point2D or not "null instanceof [type]" also
     * returns false
     */
    if (!(o instanceof Point2D)) {
      return false;
    }

    // typecast o to Point2D so that we can compare data members
    Point2D c = (Point2D) o;

    return Double.compare(latitude, c.getLatitude()) == 0 && Double.compare(this.longitude, c.getLongitude()) == 0;
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
