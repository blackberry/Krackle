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

  public int getMessage(byte[] dest, int pos, int maxLength) throws IOException {
    if (messageSetReader != null && messageSetReader.isReady()) {
      bytesCopied = messageSetReader.getMessage(dest, pos, maxLength);
      if (!messageSetReader.isReady() && !buffer.hasRemaining()) {
        ready = false;
      } else {
        ready = true;
      }
    } else {
      // There are occasional truncated messages. If we don't have enough, then
      // return -1 and go not-ready
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
        }
      } else if (compression == Constants.GZIP) {
        decompressedSize = decompress(getGzipDecompressor());
        ensureMessageSetReader();
        messageSetReader.init(decompressionBytes, 0, decompressedSize);
        if (messageSetReader.isReady()) {
          bytesCopied = messageSetReader.getMessage(dest, pos, maxLength);
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
      if (decompressedSize == decompressionBytes.length) {
        // we got back the maximum number of bytes we would accept. Most likely,
        // this means there is more data that we can take. So increase our
        // buffers and retry. This should be very rare.
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

  public boolean isReady() {
    return ready;
  }

  public long getOffset() {
    return offset;
  }

  public long getNextOffset() {
    return offset + 1;
  }

}
