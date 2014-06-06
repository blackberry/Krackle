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

  public GzipCompressor() {
    this(Deflater.DEFAULT_COMPRESSION);
  }

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
    compressedSize += deflater.deflate(dest, destPos + compressedSize,
        dest.length - destPos - compressedSize);

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
