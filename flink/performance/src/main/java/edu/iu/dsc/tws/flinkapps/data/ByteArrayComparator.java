package edu.iu.dsc.tws.flinkapps.data;

import java.util.Comparator;

public class ByteArrayComparator implements Comparator<byte[]> {

  public static final ByteArrayComparator INSTANCE = new ByteArrayComparator();

  public ByteArrayComparator() {
  }

  public static ByteArrayComparator getInstance() {
    return INSTANCE;
  }

  @Override
  public int compare(byte[] left, byte[] right) {
    for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
      int a = left[i] & 0xff;
      int b = right[j] & 0xff;
      if (a != b) {
        return a - b;
      }
    }
    return left.length - right.length;
  }
}