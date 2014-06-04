package com.blackberry.kafka.lowoverhead.producer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.kafka.lowoverhead.Constants;
import com.blackberry.kafka.lowoverhead.KafkaError;
import com.blackberry.kafka.lowoverhead.MetricRegistrySingleton;
import com.blackberry.kafka.lowoverhead.compression.Compressor;
import com.blackberry.kafka.lowoverhead.compression.GzipCompressor;
import com.blackberry.kafka.lowoverhead.compression.SnappyCompressor;
import com.blackberry.kafka.lowoverhead.meta.Broker;
import com.blackberry.kafka.lowoverhead.meta.MetaData;
import com.blackberry.kafka.lowoverhead.meta.Topic;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class LowOverheadProducer {
  private static final Logger LOG = LoggerFactory
      .getLogger(LowOverheadProducer.class);

  private ProducerConfiguration conf;

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
  private long queueBufferingMaxMs;

  // Store the uncompressed message set that we will be compressing later.
  private int numBuffers;
  private int loadingBuffer = 0;
  private int sendingBuffer = 0;
  private MessageSetBuffer[] messageSetBuffers;
  private ReentrantLock[] messageSetBufferLocks;

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

  // We could be using crc in 2 separate blocks, which could cause corruption
  // with one instance.
  private CRC32 crcSendMessage = new CRC32();
  private CRC32 crcSend = new CRC32();

  private MetaData metadata;
  private long lastMetadataRefresh;
  private int partition;
  private Broker broker;
  private String brokerAddress = null;
  private Socket socket;
  private OutputStream out;
  private InputStream in;

  private Sender sender;
  private Thread senderThread;
  private Loader loader;
  private Thread loaderThread;
  private SynchronousQueue<BytesAndPosition> loaderQueue = new SynchronousQueue<BytesAndPosition>();
  private SynchronousQueue<Object> loaderResponseQueue = new SynchronousQueue<Object>();

  private boolean closed = false;

  private ScheduledExecutorService scheduledExecutor = Executors
      .newSingleThreadScheduledExecutor();

  private MetricRegistry metrics;
  private Meter receivedTotal = null;
  private Meter sentTotal = null;
  private Meter droppedTotal = null;
  private Meter received = null;
  private Meter sent = null;
  private Meter dropped = null;

  public LowOverheadProducer(ProducerConfiguration conf, String clientId,
      String topic, String key, MetricRegistry metrics) throws Exception {
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

    if (metrics == null) {
      this.metrics = MetricRegistrySingleton.getInstance().getMetricsRegistry();
      MetricRegistrySingleton.getInstance().enableJmx();
      MetricRegistrySingleton.getInstance().enableConsole();
    } else {
      this.metrics = metrics;
    }

    receivedTotal = this.metrics.meter(MetricRegistry.name(
        LowOverheadProducer.class, "total messages received"));
    sentTotal = this.metrics.meter(MetricRegistry.name(
        LowOverheadProducer.class, "total messages sent"));
    droppedTotal = this.metrics.meter(MetricRegistry.name(
        LowOverheadProducer.class, "total messages dropped"));

    received = this.metrics.meter(MetricRegistry.name(
        LowOverheadProducer.class, "total messages received [" + topic + "]"));
    sent = this.metrics.meter(MetricRegistry.name(LowOverheadProducer.class,
        "total messages sent[" + topic + "]"));
    dropped = this.metrics.meter(MetricRegistry.name(LowOverheadProducer.class,
        "total messages dropped[" + topic + "]"));

    configure();

    // Start a periodic sender to ensure that things get sent from time to
    // time.
    scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        try {
          send(null, 0, 0);
        } catch (Exception e) {
          // That's fine.
        }
      }
    }, queueBufferingMaxMs, queueBufferingMaxMs, TimeUnit.MILLISECONDS);

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

    // Start the loader thread
    loader = new Loader();
    loaderThread = new Thread(loader);
    loaderThread.setDaemon(false);
    loaderThread.setName("Loader");
    loaderThread.start();
    // Wait for the loader thread to get the lock before we start the sender
    // thread.
    loaderResponseQueue.take();

    // Start the send thread
    sender = new Sender();
    senderThread = new Thread(sender);
    senderThread.setDaemon(false);
    senderThread.setName("Sender");
    senderThread.start();
  }

  private void configure() throws Exception {
    requiredAcks = conf.getRequestRequiredAcks();
    retryBackoffMs = conf.getRetryBackoffMs();
    brokerTimeout = conf.getRequestTimeoutMs();
    retries = conf.getMessageSendMaxRetries();
    sendBufferSize = conf.getSendBufferSize();
    responseBufferSize = conf.getResponseBufferSize();
    topicMetadataRefreshIntervalMs = conf.getTopicMetadataRefreshIntervalMs();
    queueBufferingMaxMs = conf.getQueueBufferingMaxMs();

    String compressionCodec = conf.getCompressionCodec();
    compressionLevel = conf.getCompressionLevel();

    int messageBufferSize = conf.getMessageBufferSize();
    numBuffers = conf.getNumBuffers();
    messageSetBuffers = new MessageSetBuffer[numBuffers];
    messageSetBufferLocks = new ReentrantLock[numBuffers];
    for (int i = 0; i < numBuffers; i++) {
      messageSetBuffers[i] = new MessageSetBuffer(this, messageBufferSize);
      messageSetBufferLocks[i] = new ReentrantLock();
    }

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
    metadata = MetaData.getMetaData(conf.getMetadataBrokerList(), topicString,
        clientIdString);
    LOG.info("Metadata: {}", metadata);

    Topic topic = metadata.getTopic(topicString);

    partition = Math.abs(keyString.hashCode()) % topic.getNumPartitions();
    LOG.info("Sending to partition {} of {}", partition,
        topic.getNumPartitions());

    broker = metadata.getBroker(topic.getPartition(partition).getLeader());

    // Only reset our connection if the broker has changed, or it's forced
    String newBrokerAddress = broker.getHost() + ":" + broker.getPort();
    if (force || brokerAddress == null
        || brokerAddress.equals(newBrokerAddress) == false) {
      brokerAddress = newBrokerAddress;
      LOG.info("Changing brokers to {}", broker);

      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          LOG.error("Error closing connection to broker.", e);
        }
      }

      try {
        socket = new Socket(broker.getHost(), broker.getPort());
        socket.setSendBufferSize(conf.getSendBufferSize());
        LOG.info("Connected to {}", socket);
        in = socket.getInputStream();
        out = socket.getOutputStream();
      } catch (UnknownHostException e) {
        LOG.error("Error connecting to broker.", e);
      } catch (IOException e) {
        LOG.error("Error connecting to broker.", e);
      }
    }

    lastMetadataRefresh = System.currentTimeMillis();
  }

  private int crcPos;

  private MessageSetBuffer activeMessageSetBuffer;
  private ByteBuffer activeByteBuffer;
  private BytesAndPosition bap = new BytesAndPosition();

  // Add a message to the send queue. Takes bytes from buffer from start, up
  // to length bytes.
  public synchronized void send(byte[] buffer, int offset, int length)
      throws Exception {
    if (closed) {
      LOG.warn("Trying to send data on a closed producer.");
      return;
    }

    bap.setBytes(buffer);
    bap.setPos(offset);
    bap.setLength(length);

    loaderQueue.put(bap);
    loaderResponseQueue.take();
  }

  private void doSend(byte[] buffer, int offset, int length) {
    activeMessageSetBuffer = messageSetBuffers[loadingBuffer];
    activeByteBuffer = activeMessageSetBuffer.getBuffer();

    // null buffer means send what we have
    if (buffer == null) {
      if (activeMessageSetBuffer.getBatchSize() > 0) {
        if (incrementLoadingBuffer()) {
          // Do nothing on success
        } else {
          LOG.info("Buffer full.  Dropping message.");
        }
      }
      return;
    }

    received.mark();
    receivedTotal.mark();

    if (activeMessageSetBuffer.getBuffer().remaining() < length + keyLength
        + 26) {
      if (incrementLoadingBuffer()) {
        // Do nothing on success
      } else {
        LOG.info("Buffer full.  Dropping message.");
        return;
      }
    }
    activeMessageSetBuffer = messageSetBuffers[loadingBuffer];
    activeByteBuffer = activeMessageSetBuffer.getBuffer();

    // LOG.info("Received: {}", new String(buffer, offset, length, UTF8));

    activeByteBuffer.putLong(0L); // Offset
    // Size of uncompressed message
    activeByteBuffer.putInt(length + keyLength + 14);

    crcPos = activeByteBuffer.position();
    activeByteBuffer.position(crcPos + 4);

    // magic number
    activeByteBuffer.put(Constants.MAGIC_BYTE);

    // no compression
    activeByteBuffer.put(Constants.NO_COMPRESSION);

    activeByteBuffer.putInt(keyLength); // Key length
    activeByteBuffer.put(keyBytes); // Key
    activeByteBuffer.putInt(length); // Value length
    activeByteBuffer.put(buffer, offset, length); // Value

    crcSend.reset();
    crcSend.update(activeMessageSetBuffer.getBytes(), crcPos + 4, length
        + keyLength + 10);

    activeByteBuffer.putInt(crcPos, (int) crcSend.getValue());

    activeMessageSetBuffer.incrementBatchSize();
  }

  int nextLoadingBuffer;
  boolean incrementLoadingBufferResult;

  private boolean incrementLoadingBuffer() {
    nextLoadingBuffer = loadingBuffer + 1;
    nextLoadingBuffer %= numBuffers;

    LOG.debug("Incrementing loading buffer from {} to {}", loadingBuffer,
        nextLoadingBuffer);

    if (conf.getQueueEnqueueTimeoutMs() == -1) {
      messageSetBufferLocks[nextLoadingBuffer].lock();
      incrementLoadingBufferResult = true;
    } else {
      try {
        incrementLoadingBufferResult = messageSetBufferLocks[nextLoadingBuffer]
            .tryLock(conf.getQueueEnqueueTimeoutMs(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.error("Error getting lock on next buffer.", e);
        incrementLoadingBufferResult = false;
      }
    }

    LOG.trace("Got lock on buffer {} ({})", nextLoadingBuffer,
        incrementLoadingBufferResult);

    if (incrementLoadingBufferResult == true) {
      LOG.debug("Unlocking buffer {}", loadingBuffer);
      try {
        messageSetBufferLocks[loadingBuffer].unlock();
      } catch (IllegalMonitorStateException e) {
        LOG.error("Error releasing lock.", e);
      }
      LOG.debug("Unlocked buffer {}", loadingBuffer);
      loadingBuffer = nextLoadingBuffer;
    }

    return incrementLoadingBufferResult;
  }

  // Send accumulated messages
  protected void sendMessage(MessageSetBuffer messageSetBuffer) {
    // New message, new id
    correlationId++;
    // LOG.info("Sending message {}", correlationId);

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
      toSendBuffer.putInt(messageSetBuffer.getBuffer().position());

      // Message set
      toSendBuffer.put(messageSetBuffer.getBytes(), 0, messageSetBuffer
          .getBuffer().position());

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
      toSendBuffer.put(compressor.getAttribute()); // Compression goes
      // here.

      // Add the key
      toSendBuffer.putInt(keyLength);
      toSendBuffer.put(keyBytes);

      // Compress the value here, into the toSendBuffer
      try {
        messageCompressedSize = compressor.compress(
            messageSetBuffer.getBytes(), 0, messageSetBuffer.getBuffer()
                .position(), toSendBytes, toSendBuffer.position() + 4);
      } catch (IOException e) {
        LOG.error("Exception while compressing data.  (data lost).", e);
        return;
      }

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
      crcSendMessage.reset();
      crcSendMessage.update(toSendBytes, messageSizePos + 8,
          toSendBuffer.position() - (messageSizePos + 8));
      toSendBuffer.putInt(messageSizePos + 4, (int) crcSendMessage.getValue());
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
          // - 4 bytes for number of topics. This is always 1 in this
          // case.
          // - 2 bytes for length of topic name.
          // - topicLength bytes for topic
          // - 4 bytes for number of partitions. Always 1.
          // - 4 bytes for partition number (which we already know)
          // - 2 bytes for error code (That's interesting to us)
          // - 8 byte offset of the first message (we don't care).
          // The only things we care about here are the correlation id
          // (must
          // match) and the error code (so we can throw an exception
          // if it's not
          // 0)
          responseCorrelationId = responseBuffer.getInt();
          if (responseCorrelationId != correlationId) {
            throw new Exception("Correlation ID mismatch.  Expected "
                + correlationId + ", got " + responseCorrelationId);
          }
          responseErrorCode = responseBuffer.getShort(18 + topicLength);
          if (responseErrorCode != KafkaError.NoError.getCode()) {
            throw new Exception("Got error from broker. Error Code "
                + responseErrorCode + " (" + getErrorString(responseErrorCode)
                + ")");
          }

          // Clear the responses, if there is anything else to read
          while (in.available() > 0) {
            in.read(responseBytes, 0, responseBytes.length);
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
          dropped.mark(messageSetBuffer.getBatchSize());
          droppedTotal.mark(messageSetBuffer.getBatchSize());
        }

      }
    }

    toSendBuffer.clear();
    sent.mark(messageSetBuffer.getBatchSize());
    sentTotal.mark(messageSetBuffer.getBatchSize());

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

  public synchronized void close() {
    LOG.info("Closing producer.");
    incrementLoadingBuffer();

    closed = true;
    try {
      senderThread.join();
      loaderThread.join();
    } catch (InterruptedException e) {
      LOG.error("Error shutting down sender and loader threads.", e);
    }
  }

  private class BytesAndPosition {
    private byte[] bytes;
    private int pos;
    private int length;

    public byte[] getBytes() {
      return bytes;
    }

    public void setBytes(byte[] bytes) {
      this.bytes = bytes;
    }

    public int getPos() {
      return pos;
    }

    public void setPos(int pos) {
      this.pos = pos;
    }

    public int getLength() {
      return length;
    }

    public void setLength(int length) {
      this.length = length;
    }
  }

  private class Loader implements Runnable {
    private BytesAndPosition pos;
    private Object responseObject = new Object();

    @Override
    public void run() {
      messageSetBufferLocks[loadingBuffer].lock();
      try {
        loaderResponseQueue.put(responseObject);
      } catch (InterruptedException e) {
        LOG.error("Error responding to send request", e);
      }

      while (true) {
        if (closed) {
          break;
        }

        try {
          pos = loaderQueue.poll(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          LOG.error("Interrupted polling for new messages.", e);
          continue;
        }

        if (pos == null) {
          continue;
        }

        doSend(pos.getBytes(), pos.getPos(), pos.getLength());

        try {
          loaderResponseQueue.put(responseObject);
        } catch (InterruptedException e) {
          LOG.error("Error responding to send request", e);
        }

      }
    }

  }

  private class Sender implements Runnable {

    @Override
    public void run() {
      while (true) {
        LOG.debug("Sender waiting for lock on {}", sendingBuffer);
        if (closed) {
          if (messageSetBufferLocks[sendingBuffer].tryLock() == false) {
            break;
          }
        } else {
          messageSetBufferLocks[sendingBuffer].lock();
        }

        LOG.debug("Sending buffer {}", sendingBuffer);

        if (messageSetBuffers[sendingBuffer].getBatchSize() > 0) {
          sendMessage(messageSetBuffers[sendingBuffer]);
        }
        messageSetBuffers[sendingBuffer].clear();

        messageSetBufferLocks[sendingBuffer].unlock();
        LOG.debug("Done sending buffer {}", sendingBuffer);

        sendingBuffer++;
        sendingBuffer %= numBuffers;
      }
    }

  }
}
