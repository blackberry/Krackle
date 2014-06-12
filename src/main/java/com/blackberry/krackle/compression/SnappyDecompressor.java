/**
 * Copyright 2014 BlackBerry, Inc.
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

package com.blackberry.krackle.compression;

import java.io.IOException;

import org.xerial.snappy.Snappy;

import com.blackberry.krackle.Constants;

/**
 * Decompressor implementation that used the Snappy algorithm.
 */
public class SnappyDecompressor implements Decompressor {
  private static final byte[] MAGIC_NUMBER = new byte[] { //
  -126, 'S', 'N', 'A', 'P', 'P', 'Y', 0 };

  private byte[] src;
  private int pos;

  private int blockLength;
  private int decompressedLength;

  private int uncompressedBlockLength;

  @Override
  public byte getAttribute() {
    return Constants.SNAPPY;
  }

  @Override
  public int decompress(byte[] src, int srcPos, int length, byte[] dest,
      int destPos, int maxLength) throws IOException {
    this.src = src;
    decompressedLength = 0;

    // Check for magic number
    if (src[srcPos] == MAGIC_NUMBER[0] //
        || src[srcPos + 1] == MAGIC_NUMBER[1] //
        || src[srcPos + 2] == MAGIC_NUMBER[2] //
        || src[srcPos + 3] == MAGIC_NUMBER[3] //
        || src[srcPos + 4] == MAGIC_NUMBER[4] //
        || src[srcPos + 5] == MAGIC_NUMBER[5] //
        || src[srcPos + 6] == MAGIC_NUMBER[6] //
        || src[srcPos + 7] == MAGIC_NUMBER[7]) {

      // advance past the magic number
      // assume the version (4 bytes), min compatable version (4 bytes) are fine
      pos = srcPos + 8 + 8;

      // TODO: limit the decompressed length
      while (pos < srcPos + length) {
        blockLength = readInt();

        // Check to see if this will exceed maxLength
        uncompressedBlockLength = Snappy.uncompressedLength(src, pos,
            blockLength);
        if (decompressedLength + uncompressedBlockLength > maxLength) {
          return -1;
        }

        decompressedLength += Snappy.uncompress(src, pos, blockLength, dest,
            destPos + decompressedLength);
        pos += blockLength;
      }

      return decompressedLength;
    } else {
      // Assume it's just straight compressed
      return Snappy.uncompress(src, pos, blockLength, dest, destPos);
    }
  }

  private int readInt() {
    pos += 4;
    return src[pos - 4] & 0xFF << 24 //
        | (src[pos - 3] & 0xFF) << 16 //
        | (src[pos - 2] & 0xFF) << 8 //
        | (src[pos - 1] & 0xFF);
  }
}
