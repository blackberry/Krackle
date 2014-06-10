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

package com.blackberry.kafka.lowoverhead.consumer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.kafka.lowoverhead.Constants;
import com.blackberry.kafka.lowoverhead.compression.Decompressor;
import com.blackberry.kafka.lowoverhead.compression.GzipDecompressor;
import com.blackberry.kafka.lowoverhead.compression.SnappyDecompressor;

/**
 * Class to read a messages from a message set.
 */
public class MessageSetReader {
  private static final Logger LOG = LoggerFactory
      .getLogger(MessageSetReader.class);

  private boolean ready = false;

  private byte[] bytes = new byte[0];
  private ByteBuffer buffer = ByteBuffer.wrap(bytes);

  // This starts at 1KiB, and doubles as necessary. I doubt it will need to do
  // so often, unless message sizes keep growing in an unbounded way.
  private byte[] decompressionBytes = new byte[1024];

  private CRC32 crc32 = new CRC32();

  private SnappyDecompressor snappyDecompressor = null;
  private GzipDecompressor gzipDecompressor = null;
  private MessageSetReader messageSetReader = null;

  /**
   * Initialize with a message set.
   * 
   * This copies the data from the source array, so the source can be altered
   * afterwards with no impact to this class.
   * 
   * @param src
   *          byte array holding the message set.
   * @param position
   *          position in the source where the message set starts.
   * @param length
   *          length of the message set.
   */
  public void init(byte[] src, int position, int length) {
    if (bytes.length < length) {
      bytes = new byte[length];
      buffer = ByteBuffer.wrap(bytes);
    }

    System.arraycopy(src, position, bytes, 0, length);
    buffer.clear();
    buffer.limit(length);

    if (length > 0) {
      ready = true;
    } else {
      ready = false;
    }
  }

  private long offset;
  private int messageSize;
  private int crc;
  private byte magicByte;
  private byte attributes;
  private byte compression;
  private int keyLength;
  private int valueLength;
  private int bytesCopied;
  private int decompressedSize;

  /**
   * Read in a message from message set into the given byte array.
   * 
   * If the size of the message exceeds maxLength, it will be truncated to fit.
   * 
   * @param dest
   *          the byte array to write into.
   * @param pos
   *          the position in the byte array to write to.
   * @param maxLength
   *          the max size of the message to write.
   * @return the number of bytes writen, or <code>-1</code> if no data was
   *         returned.
   * @throws IOException
   */
  public int getMessage(byte[] dest, int pos, int maxLength) throws IOException {
    if (messageSetReader != null && messageSetReader.isReady()) {
      bytesCopied = messageSetReader.getMessage(dest, pos, maxLength);
      offset = messageSetReader.getOffset();
      if (!messageSetReader.isReady() && !buffer.hasRemaining()) {
        ready = false;
      } else {
        ready = true;
      }
    } else {
      // There are occasional truncated messages. If we don't have enough,
      // then return -1 and go not-ready
      // This will cover the offset, message size and crc
      if (buffer.remaining() < 8 + 4) {
        ready = false;
        return -1;
      }

      // offset
      offset = buffer.getLong();

      // messageSize
      messageSize = buffer.getInt();
      // This should be the last size check we need to do.
      if (buffer.remaining() < messageSize) {
        ready = false;
        return -1;
      }

      // Crc => int32
      crc = buffer.getInt();
      // check that the crc is correct
      crc32.reset();
      crc32.update(bytes, buffer.position(), messageSize - 4);
      if (crc != (int) crc32.getValue()) {
        LOG.error("CRC value mismatch.");
        ready = false;
        return -1;
      }

      // MagicByte => int8
      magicByte = buffer.get();
      if (magicByte != Constants.MAGIC_BYTE) {
        LOG.error("Incorrect magic byte.");
        ready = false;
        return -1;
      }

      // Attributes => int8
      attributes = buffer.get();
      compression = (byte) (attributes & Constants.COMPRESSION_MASK);

      // Key => bytes
      keyLength = buffer.getInt();
      if (keyLength == -1) {
        // null key
      } else {
        // ignore the key
        buffer.position(buffer.position() + keyLength);
      }

      // Value => bytes
      valueLength = buffer.getInt();
      if (valueLength == -1) {
        // null value. return -1, but we may still be ready.
        if (!buffer.hasRemaining()) {
          ready = false;
        }
        return -1;
      }

      if (compression == Constants.NO_COMPRESSION) {
        bytesCopied = Math.min(maxLength, valueLength);
        if (bytesCopied < valueLength) {
          LOG.warn("Truncating message from {} to {} bytes.", valueLength,
              maxLength);
        }
        System.arraycopy(bytes, buffer.position(), dest, pos, bytesCopied);
      } else if (compression == Constants.SNAPPY) {
        decompressedSize = decompress(getSnappyDecompressor());
        ensureMessageSetReader();
        messageSetReader.init(decompressionBytes, 0, decompressedSize);
        if (messageSetReader.isReady()) {
          bytesCopied = messageSetReader.getMessage(dest, pos, maxLength);
          offset = messageSetReader.getOffset();
        }
      } else if (compression == Constants.GZIP) {
        decompressedSize = decompress(getGzipDecompressor());
        ensureMessageSetReader();
        messageSetReader.init(decompressionBytes, 0, decompressedSize);
        if (messageSetReader.isReady()) {
          bytesCopied = messageSetReader.getMessage(dest, pos, maxLength);
          offset = messageSetReader.getOffset();
        }
      }

      buffer.position(buffer.position() + valueLength);
      if ((messageSetReader == null || !messageSetReader.isReady())
          && !buffer.hasRemaining()) {
        ready = false;
      } else {
        ready = true;
      }
    }

    return bytesCopied;
  }

  private int decompress(Decompressor decompressor) throws IOException {
    while (true) {
      decompressedSize = decompressor.decompress(bytes, buffer.position(),
          valueLength, decompressionBytes, 0, decompressionBytes.length);
      if (decompressedSize == -1) {
        // Our output buffer was not big enough. So increase our
        // buffers and retry. This should be very rare.
        LOG.info("Expanding decompression buffer from {} to {}",
            decompressionBytes.length, 2 * decompressionBytes.length);
        decompressionBytes = new byte[2 * decompressionBytes.length];
      } else {
        // we didn't fill the buffer. So our buffer was big enough.
        break;
      }
    }

    return decompressedSize;
  }

  private void ensureMessageSetReader() {
    if (messageSetReader == null) {
      messageSetReader = new MessageSetReader();
    }
  }

  private SnappyDecompressor getSnappyDecompressor() {
    if (snappyDecompressor == null) {
      snappyDecompressor = new SnappyDecompressor();
    }
    return snappyDecompressor;
  }

  private GzipDecompressor getGzipDecompressor() {
    if (gzipDecompressor == null) {
      gzipDecompressor = new GzipDecompressor();
    }
    return gzipDecompressor;
  }

  /**
   * Checks if this message set ready is ready.
   * 
   * Ready means that it is initialized and has not reached the end of its data.
   * This does not guarantee that the next request will succeed, since the
   * remaining data could be a truncated message.
   * 
   * @return <code>true</code> if this MessageSetReader is ready, otherwise
   *         <code>false</code>.
   */
  public boolean isReady() {
    return ready;
  }

  /**
   * Gets the offset of the last message read.
   * 
   * @return the offset of the last message read.
   */
  public long getOffset() {
    return offset;
  }

  /**
   * Gets the offset of the next message that would be returned.
   * 
   * @return the offset of the next message that would be returned.
   */
  public long getNextOffset() {
    return offset + 1;
  }

}
