package com.blackberry.kafka.loproducer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LowOverheadProducer {
  private static final Logger LOG = LoggerFactory
      .getLogger(LowOverheadProducer.class);

  private Configuration conf;

  private String clientIdString;
  private byte[] clientIdBytes;
  private short clientIdLength;

  private String keyString;
  private byte[] keyBytes;
  private int keyLength;

  private String topicString;
  private byte[] topicBytes;
  private short topicLength;

  // Various configs
  private short requiredAcks;
  private int brokerTimeout;
  private int retries;
  private int compressionLevel;
  private int retryBackoffMs;
  private long topicMetadataRefreshIntervalMs;

  // Store the uncompressed message set that we will be compressing later.
  private int messageBufferSize;
  private byte[] messageSetBytes;
  private ByteBuffer messageSetBuffer;

  // Store the compressed message + headers.
  // Generally, this needs to be bigger than the messageSetBufferSize
  // for uncompressed messages (size + key length + topic length + 34?)
  // For compressed messages, this shouldn't need to be bigger.
  private int sendBufferSize;
  private byte[] toSendBytes;
  private ByteBuffer toSendBuffer;

  // Buffer for reading responses from the server.
  // Assuming we only send for one topic per message, then this only needs to
  // be topic.length + 24 bytes
  private int responseBufferSize;
  private byte[] responseBytes;
  private ByteBuffer responseBuffer;

  // How to compress!
  Compressor compressor;

  // Stuff for internal use
  private int correlationId = 0;
  private int messageSetSizePos;
  private int messageSizePos;
  private int messageCompressedSize;
  private int responseSize;
  private int responseCorrelationId;
  private short responseErrorCode;
  private int retry;
  private int batchSize = 0;

  private CRC32 crc = new CRC32();

  private MetaData metadata;
  private long lastMetadataRefresh;
  private int partition;
  private Broker broker;
  private String brokerAddress = null;
  private Socket socket;
  private OutputStream out;
  private InputStream in;

  private Metrics metrics;

  public LowOverheadProducer(Configuration conf, String clientId, String topic,
      String key) throws Exception {
    LOG.info("New {} ({},{})", new Object[] { this.getClass().getName(), topic,
        clientId });
    this.conf = conf;

    this.topicString = topic;
    this.topicBytes = topic.getBytes(Constants.UTF8);
    this.topicLength = (short) topicBytes.length;

    this.clientIdString = clientId;
    this.clientIdBytes = clientId.getBytes(Constants.UTF8);
    this.clientIdLength = (short) clientId.length();

    this.keyString = key;
    this.keyBytes = key.getBytes(Constants.UTF8);
    this.keyLength = keyBytes.length;

    this.metrics = Metrics.getInstance();
    metrics.addTopic(topic);

    configure();

    // Try to do this. If it fails, then we can try again when it's time to
    // send.
    try {
      // In case this fails, we don't want the value to be null;
      lastMetadataRefresh = System.currentTimeMillis();
      updateMetaDataAndConnection(true);
    } catch (Throwable t) {
      LOG.warn("Initial load of metadata failed.", t);
      metadata = null;
    }
  }

  private void configure() throws Exception {
    requiredAcks = conf.getRequrestRequiredAcks();
    retryBackoffMs = conf.getRetryBackoffMs();
    brokerTimeout = conf.getRequestTimeoutMs();
    retries = conf.getMessageSendMaxRetries();
    messageBufferSize = conf.getMessageBufferSize();
    sendBufferSize = conf.getSendBufferSize();
    responseBufferSize = conf.getResponseBufferSize();
    topicMetadataRefreshIntervalMs = conf.getTopicMetadataRefreshIntervalMs();

    String compressionCodec = conf.getCompressionCodec();
    compressionLevel = conf.getCompressionLevel();

    messageSetBytes = new byte[messageBufferSize];
    messageSetBuffer = ByteBuffer.wrap(messageSetBytes);

    toSendBytes = new byte[sendBufferSize];
    toSendBuffer = ByteBuffer.wrap(toSendBytes);

    responseBytes = new byte[responseBufferSize];
    responseBuffer = ByteBuffer.wrap(responseBytes);

    if (compressionCodec.equals("none")) {
      compressor = null;
    } else if (compressionCodec.equals("snappy")) {
      compressor = new SnappyCompressor();
    } else if (compressionCodec.equals("gzip")) {
      compressor = new GzipCompressor(compressionLevel);
    } else {
      throw new Exception("Unknown compression type: " + compressionCodec);
    }

  }

  private void updateMetaDataAndConnection(boolean force) {
    LOG.info("Updating metadata");
    metadata = MetaData.getMetaData(conf, topicString, clientIdString);
    LOG.info("Metadata: {}", metadata);

    Topic topic = metadata.getTopic(topicString);

    partition = Math.abs(keyString.hashCode()) % topic.getNumPartitions();

    broker = metadata.getBroker(topic.getPartition(partition).getLeader());

    // Only reset our connection if the broker has changed, or it's forced
    String newBrokerAddress = broker.getHost() + ":" + broker.getPort();
    if (force || brokerAddress == null
        || brokerAddress.equals(newBrokerAddress) == false) {

      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          LOG.error("Error closing connection to broker.", e);
        }
      }

      try {
        socket = new Socket(broker.getHost(), broker.getPort());
        in = socket.getInputStream();
        out = socket.getOutputStream();
      } catch (UnknownHostException e) {
        LOG.error("Error connecting to broker.", e);
      } catch (IOException e) {
        LOG.error("Error connecting to broker.", e);
      }

      brokerAddress = newBrokerAddress;
    }

    lastMetadataRefresh = System.currentTimeMillis();
  }

  private int crcPos;

  // Add a message to the send queue. Takes bytes from buffer from start, up to
  // length bytes.
  public void send(byte[] buffer, int offset, int length) {
    metrics.receivedMark(topicString);

    if (messageSetBuffer.remaining() < length + keyLength + 26) {
      sendMessage();
      batchSize = 0;
    }
    // LOG.info("Received: {}", new String(buffer, offset, length, UTF8));

    messageSetBuffer.putLong(0L); // Offset
    // Size of uncompressed message
    messageSetBuffer.putInt(length + keyLength + 14);

    crcPos = messageSetBuffer.position();
    messageSetBuffer.position(crcPos + 4);

    messageSetBuffer.put(Constants.MAGIC_BYTE); // magic number
    messageSetBuffer.put(Constants.ATTR_NO_COMPRESSION); // no compression
    messageSetBuffer.putInt(keyLength); // Key length
    messageSetBuffer.put(keyBytes); // Key
    messageSetBuffer.putInt(length); // Value length
    messageSetBuffer.put(buffer, offset, length); // Value

    crc.reset();
    crc.update(messageSetBytes, crcPos + 4, length + keyLength + 10);

    messageSetBuffer.putInt(crcPos, (int) crc.getValue());

    batchSize++;
  }

  // Send accumulated messages
  private void sendMessage() {
    // New message, new id
    correlationId++;

    /* headers */
    // Skip 4 bytes for the size
    toSendBuffer.position(4);
    // API key for produce
    toSendBuffer.putShort(Constants.APIKEY_PRODUCE);
    // Version
    toSendBuffer.putShort(Constants.API_VERSION);
    // Correlation Id
    toSendBuffer.putInt(correlationId);
    // Client Id
    toSendBuffer.putShort(clientIdLength);
    toSendBuffer.put(clientIdBytes);
    // Required Acks
    toSendBuffer.putShort(requiredAcks);
    // Timeout in ms
    toSendBuffer.putInt(brokerTimeout);
    // Number of topics
    toSendBuffer.putInt(1);
    // Topic name
    toSendBuffer.putShort(topicLength);
    toSendBuffer.put(topicBytes);
    // Number of partitions
    toSendBuffer.putInt(1);
    // Partition
    toSendBuffer.putInt(partition);

    if (compressor == null) {
      // If we 're not compressing, then we can just dump the rest of the
      // message here.

      // Mesage set size
      toSendBuffer.putInt(messageSetBuffer.position());

      // Message set
      toSendBuffer.put(messageSetBytes, 0, messageSetBuffer.position());

    } else {
      // If we are compressing, then do that.

      // Message set size ? We'll have to do this later.
      messageSetSizePos = toSendBuffer.position();
      toSendBuffer.position(toSendBuffer.position() + 4);

      // offset can be anything for produce requests. We'll use 0
      toSendBuffer.putLong(0L);

      // Skip 4 bytes for size, and 4 for crc
      messageSizePos = toSendBuffer.position();
      toSendBuffer.position(toSendBuffer.position() + 8);

      toSendBuffer.put(Constants.MAGIC_BYTE); // magic number
      toSendBuffer.put(compressor.getAttribute()); // Compression goes here.

      // Add the key
      toSendBuffer.putInt(keyLength);
      toSendBuffer.put(keyBytes);

      // Compress the value here, into the toSendBuffer
      try {
        messageCompressedSize = compressor.compress(messageSetBytes, 0,
            messageSetBuffer.position(), toSendBytes,
            toSendBuffer.position() + 4);
        // LOG.info("Compressed message size is {}", messageCompressedSize);
      } catch (IOException e) {
        LOG.error("Exception while compressing data.  (data lost).", e);
        return;
      }
      // messageCompressedSize = messageSetBuffer.position();
      // System.arraycopy(messageSetBytes, 0, toSendBytes,
      // toSendBuffer.position() + 4, messageSetBuffer.position());

      // Write the size
      toSendBuffer.putInt(messageCompressedSize);

      // Update the send buffer position
      toSendBuffer.position(toSendBuffer.position() + messageCompressedSize);

      /** Go back and fill in the missing pieces **/
      // Message Set Size
      toSendBuffer.putInt(messageSetSizePos, toSendBuffer.position()
          - (messageSetSizePos + 4));

      // Message Size
      toSendBuffer.putInt(messageSizePos, toSendBuffer.position()
          - (messageSizePos + 4));

      // Message CRC
      crc.reset();
      crc.update(toSendBytes, messageSizePos + 8, toSendBuffer.position()
          - (messageSizePos + 8));
      toSendBuffer.putInt(messageSizePos + 4, (int) crc.getValue());
    }

    // Fill in the complete message size
    toSendBuffer.putInt(0, toSendBuffer.position() - 4);

    // Send it!
    retry = 0;
    while (retry <= retries) {
      try {
        if (metadata == null || socket == null) {
          updateMetaDataAndConnection(true);
        }

        // Send request
        out.write(toSendBytes, 0, toSendBuffer.position());

        if (requiredAcks != 0) {
          // Check response
          responseBuffer.clear();
          in.read(responseBytes, 0, 4);
          responseSize = responseBuffer.getInt();
          responseBuffer.clear();
          in.read(responseBytes, 0, responseSize);

          // Response bytes are
          // - 4 byte correlation id
          // - 4 bytes for number of topics. This is always 1 in this case.
          // - 2 bytes for length of topic name.
          // - topicLength bytes for topic
          // - 4 bytes for number of partitions. Always 1.
          // - 4 bytes for partition number (which we already know)
          // - 2 bytes for error code (That's interesting to us)
          // - 8 byte offset of the first message (we don't care).
          // The only things we care about here are the correlation id (must
          // match) and the error code (so we can throw an exception if it's not
          // 0)
          responseCorrelationId = responseBuffer.getInt();
          if (responseCorrelationId != correlationId) {
            throw new Exception("Correlation ID mismatch.  Expected "
                + correlationId + ", got " + responseCorrelationId);
          }
          responseErrorCode = responseBuffer.getShort(18 + topicLength);
          if (responseErrorCode != KafkaError.NoError.getCode()) {
            throw new Exception("Got error from broker " + responseErrorCode
                + "(" + getErrorString(responseErrorCode) + ")");
          }
        }

        break;
      } catch (Throwable t) {
        metadata = null;

        retry++;
        if (retry <= retries) {
          LOG.warn("Request failed. Retrying {} more times.", retries - retry
              + 1, t);
          try {
            Thread.sleep(retryBackoffMs);
          } catch (InterruptedException e) {
            // Do nothing
          }
        } else {
          LOG.error("Request failed. No more retries (data lost).", t);
          metrics.droppedMark(topicString, batchSize);
        }

      }
    }

    clearBuffers();
    metrics.sentMark(topicString, batchSize);

    // Periodic metadata refreshes.
    if (topicMetadataRefreshIntervalMs >= 0
        && System.currentTimeMillis() - lastMetadataRefresh >= topicMetadataRefreshIntervalMs) {
      try {
        updateMetaDataAndConnection(false);
      } catch (Throwable t) {
        LOG.error("Error refreshing metadata.", t);
      }
    }
  }

  private String getErrorString(short errorCode) {
    for (KafkaError e : KafkaError.values()) {
      if (e.getCode() == errorCode) {
        return e.getMessage();
      }
    }
    return null;
  }

  private void clearBuffers() {
    messageSetBuffer.clear();
    toSendBuffer.clear();
  }

  public void close() {
    if (messageSetBuffer.position() > 0) {
      sendMessage();
    }
  }

}
