/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blackberry.bdp.krackle.compression;

import java.io.IOException;

import org.xerial.snappy.Snappy;

import com.blackberry.bdp.krackle.Constants;

/**
 * Compressor implementation that used the Snappy algorithm.
 */
public class SnappyCompressor implements Compressor {
  private final static byte[] header = new byte[] { //
  -126, 'S', 'N', 'A', 'P', 'P', 'Y', 0, // Magic number
      0, 0, 0, 1, // version
      0, 0, 0, 1 // min compatable version
  };
  private final static int headerLength = header.length;

  private int compressedLength;
  private int maxCompressedSize;

  @Override
  public byte getAttribute() {
    return Constants.SNAPPY;
  }

  @Override
  public int compress(byte[] src, int srcPos, int length, byte[] dest,
      int destPos) throws IOException {
    System.arraycopy(header, 0, dest, destPos, headerLength);

    // Compressed size cannot be greater than what we have available
    maxCompressedSize = dest.length - destPos - headerLength - 4;
    if (Snappy.maxCompressedLength(length) > maxCompressedSize) {
      return -1;
    }

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
