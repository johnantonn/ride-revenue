package edu.kul.mai.bdap;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {

	public CompositeKeyComparator() {
		super(CompositeKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {

		CompositeKey ck1 = (CompositeKey) wc1;
		CompositeKey ck2 = (CompositeKey) wc2;

		if (ck1.getId() == ck2.getId()) {
			// Ids are equal here
			if (ck1.getTimestamp() == ck2.getTimestamp()) {
				return 0;
			} else if (ck1.getTimestamp() < ck2.getTimestamp()) {
				return -1;
			} else {
				return 1;
			}
		} else {
			if (ck1.getId() < ck2.getId()) {
				return -1;
			} else {
				return 1;
			}
		}

	}
}