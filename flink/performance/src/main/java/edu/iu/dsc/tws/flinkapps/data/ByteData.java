package edu.iu.dsc.tws.flinkapps.data;

import java.io.Serializable;
import java.util.Random;

public class ByteData implements Serializable {
  private byte[] data;

  public ByteData(int size) {
    data = new byte[size];
    new Random().nextBytes(data);
  }

  public ByteData() {
  }

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }
}
