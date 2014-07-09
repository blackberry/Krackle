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

package com.blackberry.krackle.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.krackle.Constants;
import com.blackberry.krackle.KafkaError;
import com.blackberry.krackle.MetricRegistrySingleton;
import com.blackberry.krackle.meta.Broker;
import com.blackberry.krackle.meta.MetaData;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

/**
 * An implementation of the Kafka 0.8 consumer.
 * 
 * This class acts as a consumer of data from a cluster of Kafka brokers. Each
 * instance only reads data from a single partition of a single topic. If you
 * need to read more than that, then instantiate more instances.
 * 
 * This class was designed to be very light weight. The standard Java client
 * creates a lot of objects, and therefore causes a lot of garbage collection
 * that leads to a major slowdown in performance. This client creates no new
 * objects during steady state running, and so avoids all garbage collection
 * overhead.
 */
public class Consumer {
  private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

  private static final Charset UTF8 = Charset.forName("UTF8");

  private ConsumerConfiguration conf;

  private String clientId;
  private byte[] clientIdBytes;
  private short clientIdLength;

  private String topic;
  private byte[] topicBytes;
  private short topicLength;

  private int partition;

  private MessageSetReader messageSetReader = new MessageSetReader();

  private long offset;
  private long lastOffset;
  private long highWaterMark;

  private byte[] offsetRequestBytes;
  private ByteBuffer offsetRequestBuffer;

  private byte[] offsetResponseBytes;
  private ByteBuffer offsetResponseBuffer;

  private byte[] requestBytes;
  private ByteBuffer requestBuffer;

  private int fetchMessageMaxBytes;
  private byte[] responseBytes;
  private ByteBuffer responseBuffer;

  private int fetchWaitMaxMs;
  private int fetchMinBytes;

  private MetricRegistry metrics;
  private Meter mMessageRequests = null;
  private Meter mMessageRequestsTotal = null;
  private Meter mMessagesReturned = null;
  private Meter mMessagesReturnedTotal = null;
  private Meter mBytesReturned = null;
  private Meter mBytesReturnedTotal = null;
  private Meter mMessageRequestsNoData = null;
  private Meter mMessageRequestsNoDataTotal = null;
  private Meter mBrokerReadAttempts = null;
  private Meter mBrokerReadAttemptsTotal = null;
  private Meter mBrokerReadSuccess = null;
  private Meter mBrokerReadSuccessTotal = null;
  private Meter mBrokerReadFailure = null;
  private Meter mBrokerReadFailureTotal = null;

  /**
   * Create a new consumer that reads from a given consumer. It attempts to
   * start at offset 0.
   * 
   * @param conf
   *          ConsumerConfiguration for this consumer.
   * @param clientId
   *          clientId to be send with requests to Kafka.
   * @param topic
   *          topic to read from.
   * @param partition
   *          id of the partition to read from.
   */
  public Consumer(ConsumerConfiguration conf, String clientId, String topic,
      int partition) {
    this(conf, clientId, topic, partition, 0L);
  }

  /**
   * Create a new consumer that reads from a given consumer that attempts to
   * start reading at the given offset.
   * 
   * @param conf
   *          ConsumerConfiguration for this consumer.
   * @param clientId
   *          clientId to be send with requests to Kafka.
   * @param topic
   *          topic to read from.
   * @param partition
   *          id of the partition to read from.
   * @param offset
   *          the offset to start reading from.
   */
  public Consumer(ConsumerConfiguration conf, String clientId, String topic,
      int partition, long offset) {
    this(conf, clientId, topic, partition, offset, null);
  }

  /**
   * Create a new consumer that reads from a given consumer that attempts to
   * start reading at the given offset.
   * 
   * Metrics are reported using the given instance of MetricRegistry instead the
   * internal singleton instance.
   * 
   * @param conf
   *          ConsumerConfiguration for this consumer.
   * @param clientId
   *          clientId to be send with requests to Kafka.
   * @param topic
   *          topic to read from.
   * @param partition
   *          id of the partition to read from.
   * @param offset
   *          the offset to start reading from.
   * @param metrics
   *          the instance of MetricRegistry to use for reporting metrics.
   */
  public Consumer(ConsumerConfiguration conf, String clientId, String topic,
      int partition, long offset, MetricRegistry metrics) {
    LOG.info("Creating consumer for {}-{} from offset {}", topic, partition,
        offset);

    this.conf = conf;

    if (metrics == null) {
      this.metrics = MetricRegistrySingleton.getInstance().getMetricsRegistry();
      MetricRegistrySingleton.getInstance().enableJmx();
      MetricRegistrySingleton.getInstance().enableConsole();
    } else {
      this.metrics = metrics;
    }

    initializeMetrics();

    this.clientId = clientId;
    clientIdBytes = clientId.getBytes(UTF8);
    clientIdLength = (short) clientIdBytes.length;

    this.topic = topic;
    topicBytes = topic.getBytes(UTF8);
    topicLength = (short) topicBytes.length;

    this.partition = partition;
    this.offset = offset;

    offsetRequestBytes = new byte[44 + clientIdLength + topicLength];
    offsetRequestBuffer = ByteBuffer.wrap(offsetRequestBytes);

    offsetResponseBytes = new byte[32 + topicLength];
    offsetResponseBuffer = ByteBuffer.wrap(offsetResponseBytes);

    requestBytes = new byte[52 + clientIdLength + topicLength];
    requestBuffer = ByteBuffer.wrap(requestBytes);

    fetchMessageMaxBytes = conf.getFetchMessageMaxBytes();
    responseBytes = new byte[fetchMessageMaxBytes + 32 + topicLength];
    responseBuffer = ByteBuffer.wrap(responseBytes);

    fetchWaitMaxMs = conf.getFetchWaitMaxMs();
    fetchMinBytes = conf.getFetchMinBytes();

    LOG.info("Connecting to broker.");
    connectToBroker();
  }

  private void initializeMetrics() {

    String name = "[" + topic + "-" + partition + "]";

    mMessageRequests = this.metrics.meter(MetricRegistry.name(Consumer.class,
        "message requests " + name));
    mMessageRequestsTotal = this.metrics.meter(MetricRegistry.name(
        Consumer.class, "message requests [total]"));
    mMessagesReturned = this.metrics.meter(MetricRegistry.name(Consumer.class,
        "message returned " + name));
    mMessagesReturnedTotal = this.metrics.meter(MetricRegistry.name(
        Consumer.class, "message returned [total]"));
    mBytesReturned = this.metrics.meter(MetricRegistry.name(Consumer.class,
        "bytes returned " + name));
    mBytesReturnedTotal = this.metrics.meter(MetricRegistry.name(
        Consumer.class, "bytes returned [total]"));
    mMessageRequestsNoData = this.metrics.meter(MetricRegistry.name(
        Consumer.class, "no message returned " + name));
    mMessageRequestsNoDataTotal = this.metrics.meter(MetricRegistry.name(
        Consumer.class, "no message returned [total]"));

    mBrokerReadAttempts = this.metrics.meter(MetricRegistry.name(
        Consumer.class, "broker consume attempts " + name));
    mBrokerReadAttemptsTotal = this.metrics.meter(MetricRegistry.name(
        Consumer.class, "broker consume attempts [total]"));
    mBrokerReadSuccess = this.metrics.meter(MetricRegistry.name(Consumer.class,
        "broker consume success " + name));
    mBrokerReadSuccessTotal = this.metrics.meter(MetricRegistry.name(
        Consumer.class, "broker consume success [total]"));
    mBrokerReadFailure = this.metrics.meter(MetricRegistry.name(Consumer.class,
        "broker consume failure " + name));
    mBrokerReadFailureTotal = this.metrics.meter(MetricRegistry.name(
        Consumer.class, "broker consume failure [total]"));
  }

  private int bytesReturned = 0;

  /**
   * Read in a message from Kafka into the given byte array.
   * 
   * If the size of the message exceeds maxLength, it will be truncated to fit.
   * 
   * @param buffer
   *          the byte array to write into.
   * @param pos
   *          the position in the byte array to write to.
   * @param maxLength
   *          the max size of the message to write.
   * @return the number of bytes writen, or <code>-1</code> if no data was
   *         returned.
   * @throws IOException
   */
  public int getMessage(byte[] buffer, int pos, int maxLength)
      throws IOException {

    mMessageRequests.mark();
    mMessageRequestsTotal.mark();

    if (messageSetReader == null || messageSetReader.isReady() == false) {
      readFromBroker();

      if (messageSetReader == null || messageSetReader.isReady() == false) {
        mMessageRequestsNoData.mark();
        mMessageRequestsNoDataTotal.mark();
        return -1;
      }
    }

    bytesReturned = messageSetReader.getMessage(buffer, pos, maxLength);

    if (bytesReturned == -1) {
      mMessageRequestsNoData.mark();
      mMessageRequestsNoDataTotal.mark();
      return -1;
    }

    lastOffset = messageSetReader.getOffset();
    offset = messageSetReader.getNextOffset();

    mMessagesReturned.mark();
    mMessagesReturnedTotal.mark();
    mBytesReturned.mark(bytesReturned);
    mBytesReturnedTotal.mark(bytesReturned);

    return bytesReturned;
  }

  private Socket brokerSocket = null;
  private InputStream brokerIn = null;
  private OutputStream brokerOut = null;

  private int correlationId = 0;

  private void readFromBroker() throws IOException {
    mBrokerReadAttempts.mark();
    mBrokerReadAttemptsTotal.mark();

    if (brokerSocket == null || brokerSocket.isClosed()) {
      LOG.info("Connecting to broker.");
      connectToBroker();
    }

    try {
      correlationId++;

      sendConsumeRequest(correlationId);
      receiveConsumeResponse(correlationId);

      mBrokerReadSuccess.mark();
      mBrokerReadSuccessTotal.mark();
    } catch (OffsetOutOfRangeException e) {
      mBrokerReadFailure.mark();
      mBrokerReadFailureTotal.mark();

      if (conf.getAutoOffsetReset().equals("smallest")) {
        LOG.warn("Offset out of range.  Resetting to the earliest offset available.");
        offset = getEarliestOffset();
      } else if (conf.getAutoOffsetReset().equals("largest")) {
        LOG.warn("Offset out of range.  Resetting to the latest offset available.");
        offset = getLatestOffset();
      } else {
        throw e;
      }
    } catch (Exception e) {
      LOG.error("Error getting data from broker.", e);

      if (brokerSocket != null) {
        try {
          brokerSocket.close();
        } catch (IOException e1) {
          LOG.error("Error closing socket.", e1);
        }
      }
      brokerSocket = null;

      mBrokerReadFailure.mark();
      mBrokerReadFailureTotal.mark();
    }
  }

  private long getEarliestOffset() {
    try {
      correlationId++;
      sendOffsetRequest(Constants.EARLIEST_OFFSET, correlationId);
      return getOffsetResponse(correlationId);
    } catch (IOException e) {
      LOG.error("Error getting earliest offset.");
    }
    return 0L;
  }

  private long getLatestOffset() {
    try {
      correlationId++;
      sendOffsetRequest(Constants.LATEST_OFFSET, correlationId);
      return getOffsetResponse(correlationId);
    } catch (IOException e) {
      LOG.error("Error getting latest offset.");
    }
    return Long.MAX_VALUE;
  }

  private void sendOffsetRequest(long time, int correlationId)
      throws IOException {
    LOG.debug("Sending request for offset. correlation id = {}, time = {}",
        correlationId, time);

    offsetRequestBuffer.clear();

    // skip 4 bytes for length
    offsetRequestBuffer.position(4);

    // API key
    offsetRequestBuffer.putShort(Constants.APIKEY_OFFSET_REQUEST);

    // API Version
    offsetRequestBuffer.putShort(Constants.API_VERSION);

    // Correlation Id
    offsetRequestBuffer.putInt(correlationId);

    // ClientId
    offsetRequestBuffer.putShort(clientIdLength);
    offsetRequestBuffer.put(clientIdBytes);

    // replica id is always -1
    offsetRequestBuffer.putInt(-1);

    // Only requesting for 1 topic
    offsetRequestBuffer.putInt(1);

    // Topic Name
    offsetRequestBuffer.putShort(topicLength);
    offsetRequestBuffer.put(topicBytes);

    // Only requesting for 1 partition
    offsetRequestBuffer.putInt(1);

    // Partition
    offsetRequestBuffer.putInt(partition);

    // Time for offset
    offsetRequestBuffer.putLong(time);

    // We only need one offset
    offsetRequestBuffer.putInt(1);

    // Add the length to the start
    offsetRequestBuffer.putInt(0, offsetRequestBuffer.position() - 4);

    brokerOut.write(offsetRequestBytes, 0, offsetRequestBuffer.position());
  }

  private long getOffsetResponse(int correlationId) throws IOException {
    LOG.debug("Waiting for response. correlation id = {}", correlationId);

    try {
      // read the length of the response
      bytesRead = 0;
      while (bytesRead < 4) {
        bytesRead += brokerIn.read(offsetResponseBytes, bytesRead,
            4 - bytesRead);
      }
      offsetResponseBuffer.clear();
      responseLength = offsetResponseBuffer.getInt();

      bytesRead = 0;
      while (bytesRead < responseLength) {
        bytesRead += brokerIn.read(offsetResponseBytes, bytesRead,
            responseLength - bytesRead);
        LOG.debug("Read {} bytes", bytesRead);
      }
      offsetResponseBuffer.clear();

      // Check correlation Id
      responseCorrelationId = offsetResponseBuffer.getInt();
      if (responseCorrelationId != correlationId) {
        LOG.error("correlation id mismatch.  Expected {}, got {}",
            correlationId, responseCorrelationId);
        throw new IOException("Correlation ID mismatch.  Expected "
            + correlationId + ". Got " + responseCorrelationId + ".");
      }

      // We can skip a bunch of stuff here.
      // There is 1 topic (4 bytes), then the topic name (2 + topicLength
      // bytes), then the number of partitions (which is 1) (4 bytes),
      // then the
      // partition id (4 bytes)
      offsetResponseBuffer.position(offsetResponseBuffer.position() + 4 + 2
          + topicLength + 4 + 4);

      // Next is the error code.
      errorCode = offsetResponseBuffer.getShort();

      if (errorCode == KafkaError.OffsetOutOfRange.getCode()) {
        throw new OffsetOutOfRangeException();
      } else if (errorCode != KafkaError.NoError.getCode()) {
        throw new IOException("Error from Kafka. (" + errorCode + ") "
            + KafkaError.getMessage(errorCode));
      }

      // Finally, the offset. There is an array of one (skip 4 bytes)
      offsetResponseBuffer.position(offsetResponseBuffer.position() + 4);
      LOG.debug("Succeeded in request.  correlation id = {}", correlationId);
      return offsetResponseBuffer.getLong();

    } finally {
      // Clean out any other data that is sitting on the socket to be
      // read. It's
      // useless to us, but may through off future transactions if we
      // leave it
      // there.
      bytesRead = 0;
      while (brokerIn.available() > 0) {
        bytesRead += brokerIn.read(offsetResponseBytes, bytesRead,
            offsetResponseBytes.length);
      }
    }
  }

  private void sendConsumeRequest(int correlationId) throws IOException {
    LOG.debug("Sending consume request. correlation id = {}", correlationId);

    requestBuffer.clear();

    // Skip 4 bytes for the request size
    requestBuffer.position(requestBuffer.position() + 4);

    // API key
    requestBuffer.putShort(Constants.APIKEY_FETCH_REQUEST);

    // API Version
    requestBuffer.putShort(Constants.API_VERSION);

    // Correlation Id
    requestBuffer.putInt(correlationId);

    // ClientId
    requestBuffer.putShort(clientIdLength);
    requestBuffer.put(clientIdBytes);

    // Replica ID is always -1
    requestBuffer.putInt(-1);

    // Max wait time
    requestBuffer.putInt(fetchWaitMaxMs);

    // Min bytes
    requestBuffer.putInt(fetchMinBytes);

    // Only requesting for 1 topic
    requestBuffer.putInt(1);

    // Topic Name
    requestBuffer.putShort(topicLength);
    requestBuffer.put(topicBytes);

    // Only requesting for 1 partition
    requestBuffer.putInt(1);

    // Partition
    requestBuffer.putInt(partition);

    // FetchOffset
    requestBuffer.putLong(offset);

    // MaxBytes
    requestBuffer.putInt(fetchMessageMaxBytes);

    /* Fill in missing data */
    // Full size
    requestBuffer.putInt(0, requestBuffer.position() - 4);

    // Send!
    brokerOut.write(requestBytes, 0, requestBuffer.position());
  }

  private int bytesRead;
  private int responseLength;
  private int responseCorrelationId;
  private short errorCode;
  private int messageSetSize;

  private void receiveConsumeResponse(int correlationId) throws IOException {
    LOG.debug("Waiting for response. correlation id = {}", correlationId);

    try {
      // read the length of the response
      bytesRead = 0;
      while (bytesRead < 4) {
        bytesRead += brokerIn.read(responseBytes, bytesRead, 4 - bytesRead);
      }
      responseBuffer.clear();
      responseLength = responseBuffer.getInt();

      // LOG.info("message length is {}", responseLength);

      bytesRead = 0;
      while (bytesRead < responseLength) {
        bytesRead += brokerIn.read(responseBytes, bytesRead, responseLength
            - bytesRead);
        // LOG.info("Read {} bytes", bytesRead);
      }
      responseBuffer.clear();

      // Check correlation Id
      // LOG.info("Checking correlation id");
      responseCorrelationId = responseBuffer.getInt();
      if (responseCorrelationId != correlationId) {
        LOG.error("correlation id mismatch.  Expected {}, got {}",
            correlationId, responseCorrelationId);
        throw new IOException("Correlation ID mismatch.  Expected "
            + correlationId + ". Got " + responseCorrelationId + ".");
      }

      // We can skip a bunch of stuff here.
      // There is 1 topic (4 bytes), then the topic name (2 + topicLength
      // bytes), then the number of partitions (which is 1) (4 bytes),
      // then the partition id (4 bytes)
      responseBuffer.position(responseBuffer.position() + 4 + 2 + topicLength
          + 4 + 4);

      // Next is the error code.
      // LOG.info("Checking error code");
      errorCode = responseBuffer.getShort();

      if (errorCode == KafkaError.OffsetOutOfRange.getCode()) {
        throw new OffsetOutOfRangeException();
      } else if (errorCode != KafkaError.NoError.getCode()) {
        throw new IOException("Error from Kafka. (" + errorCode + ") "
            + KafkaError.getMessage(errorCode));
      }

      // Highwatermark offset.
      highWaterMark = responseBuffer.getLong();

      // Message set size
      messageSetSize = responseBuffer.getInt();

      // LOG.info("Message set size = {}", messageSetSize);

      // MessageSet!
      messageSetReader.init(responseBytes, responseBuffer.position(),
          messageSetSize);

      LOG.debug("Succeeded in request.  correlation id = {}", correlationId);

    } finally {
      // Clean out any other data that is sitting on the socket to be
      // read. It's
      // useless to us, but may through off future transactions if we
      // leave it
      // there.
      bytesRead = 0;
      while (brokerIn.available() > 0) {
        bytesRead += brokerIn.read(responseBytes, bytesRead,
            responseBytes.length);
      }
    }
  }

  private void connectToBroker() {
    while (true) {
      try {
        MetaData meta = MetaData.getMetaData(conf.getMetadataBrokerList(),
            topic, clientId);
        Broker leader = meta.getBroker(meta.getTopic(topic)
            .getPartition(partition).getLeader());

        LOG.info("Connecting to broker {} @ {}:{}", leader.getNodeId(),
            leader.getHost(), leader.getPort());
        brokerSocket = new Socket(leader.getHost(), leader.getPort());
        brokerSocket.setReceiveBufferSize(conf.getSocketReceiveBufferBytes());
        brokerIn = brokerSocket.getInputStream();
        brokerOut = brokerSocket.getOutputStream();

        break;
      } catch (Exception e) {
        LOG.error("Error connecting to broker.", e);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e1) {
        }
      }
    }
  }

  /**
   * Gets the offset of the last message read.
   * 
   * @return the offset of the last message read.
   */
  public long getLastOffset() {
    return lastOffset;
  }

  /**
   * Gets the offset of the next message that would be returned.
   * 
   * @return the offset of the next message that would be returned.
   */
  public long getNextOffset() {
    return offset;
  }

  /**
   * Get the last high watermark we received from Kafka.
   * 
   * @return the last high watermark (highest offset)
   */
  public long getHighWaterMark() {
    return highWaterMark;
  }

}
