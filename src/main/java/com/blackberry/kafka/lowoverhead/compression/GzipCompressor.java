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

package com.blackberry.kafka.lowoverhead.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

import com.blackberry.kafka.lowoverhead.Constants;

/**
 * Compressor implementation that used the GZIP algorithm.
 */
public class GzipCompressor implements Compressor {
  private static final byte[] HEADER_BYTES = new byte[] //
  { (byte) 0x1f, (byte) 0x8b, // Magic number
      8, // Deflate
      0, // All flags zero
      0, 0, 0, 0, // Set MTIME to zero, for ease of use
      0, // No extra flags
      3 // UNIX OS
  };
  private final Deflater deflater;
  private final CRC32 crc;
  private final ByteBuffer bb;
  private int compressedSize;
  private int maxOutputSize;
  private int deflaterOutputSize;
  private byte[] testBytes = new byte[1];

  /**
   * New instance with default compression level.
   */
  public GzipCompressor() {
    this(Deflater.DEFAULT_COMPRESSION);
  }

  /**
   * New instance with the given compression level
   *
   * @param compressionLevel
   *          requested compression level. Valid values are <code>-1</code>
   *          (default compression), <code>0</code> (no compression),
   *          <code>1-9</code>.
   */
  public GzipCompressor(int compressionLevel) {
    deflater = new Deflater(compressionLevel, true);
    crc = new CRC32();
    bb = ByteBuffer.allocate(8);
    bb.order(ByteOrder.LITTLE_ENDIAN);
  }

  @Override
  public byte getAttribute() {
    return Constants.GZIP;
  }

  @Override
  public int compress(byte[] src, int srcPos, int length, byte[] dest,
      int destPos) throws IOException {
    System.arraycopy(HEADER_BYTES, 0, dest, destPos, 10);
    compressedSize = 10;

    deflater.reset();
    deflater.setInput(src, srcPos, length);
    deflater.finish();

    // The output can't exceed the bytes we have to work with, less 10 bytes for
    // headers and 8 bytes for footers.
    maxOutputSize = dest.length - destPos - 10 - 8;
    deflaterOutputSize = deflater.deflate(dest, destPos + compressedSize,
        maxOutputSize);
    if (deflaterOutputSize == maxOutputSize) {
      // We just filled the output buffer! Either we have more to decompress, or
      // we don't. If we do, then that's an error. If we don't then that's fine.
      // So let's check.
      if (deflater.deflate(testBytes, 0, 1) == 1) {
        // We couldn't fit everything in the output buffer.
        return -1;
      }
    }
    compressedSize += deflaterOutputSize;

    crc.reset();
    crc.update(src, srcPos, length);
    bb.clear();
    bb.putInt((int) crc.getValue());
    bb.putInt(length);
    bb.rewind();
    bb.get(dest, destPos + compressedSize, 8);

    compressedSize += 8;

    return compressedSize;
  }
}
