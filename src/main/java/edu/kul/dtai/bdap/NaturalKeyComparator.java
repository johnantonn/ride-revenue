package edu.kul.dtai.bdap;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyComparator extends WritableComparator {

  public NaturalKeyComparator() {
    super(CompositeKey.class, true);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable wc1, WritableComparable wc2) {

    CompositeKey nk1 = (CompositeKey) wc1;
    CompositeKey nk2 = (CompositeKey) wc2;
    if (nk1.getId() == nk2.getId()) {
      return 0;
    } else {
      if (nk1.getId() < nk2.getId()) {
        return -1;
      } else {
        return 1;
      }
    }
  }

}