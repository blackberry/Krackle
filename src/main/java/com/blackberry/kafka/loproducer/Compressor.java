package com.blackberry.kafka.loproducer;

import java.io.IOException;

public interface Compressor {
  public byte getAttribute();

  public int compress(byte[] src, int srcPos, int length, byte[] dest,
      int destPos) throws IOException;
}
