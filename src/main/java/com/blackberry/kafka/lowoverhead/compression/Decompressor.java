package com.blackberry.kafka.lowoverhead.compression;

import java.io.IOException;

public interface Decompressor {
  byte getAttribute();

  int decompress(byte[] src, int srcPos, int length, byte[] dest, int destPos,
      int maxLength) throws IOException;
}
