package com.blackberry.kafka.lowoverhead.compression;

import java.io.IOException;

import org.xerial.snappy.Snappy;

import com.blackberry.kafka.lowoverhead.Constants;

public class SnappyCompressor implements Compressor {
  private final static byte[] header = new byte[] { //
  -126, 'S', 'N', 'A', 'P', 'P', 'Y', 0, // Magic number
      0, 0, 0, 1, // version
      0, 0, 0, 1 // min compatable version
  };
  private final static int headerLength = header.length;

  private int compressedLength;

  @Override
  public byte getAttribute() {
    return Constants.SNAPPY;
  }

  @Override
  public int compress(byte[] src, int srcPos, int length, byte[] dest,
      int destPos) throws IOException {
    System.arraycopy(header, 0, dest, destPos, headerLength);

    compressedLength = Snappy.compress(src, srcPos, length, dest, destPos
        + headerLength + 4);
    writeInt(compressedLength, dest, destPos + headerLength);

    return headerLength + 4 + compressedLength;
  }

  private void writeInt(int i, byte[] dest, int pos) {
    dest[pos] = (byte) (i >> 24);
    dest[pos + 1] = (byte) (i >> 16);
    dest[pos + 2] = (byte) (i >> 8);
    dest[pos + 3] = (byte) i;
  }

}
