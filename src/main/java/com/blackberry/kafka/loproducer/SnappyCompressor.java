package com.blackberry.kafka.loproducer;

import java.io.IOException;

import org.xerial.snappy.Snappy;

public class SnappyCompressor implements Compressor {

  @Override
  public byte getAttribute() {
    return Constants.ATTR_SNAPPY;
  }

  @Override
  public int compress(byte[] src, int srcPos, int length, byte[] dest,
      int destPos) throws IOException {
    return Snappy.compress(src, srcPos, length, dest, destPos);
  }

}
