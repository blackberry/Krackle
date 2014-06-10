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

package com.blackberry.kafka.lowoverhead.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.zip.Deflater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for a producer.
 * 
 * Many of these properties are the same as those in the standard Java client,
 * as documented at http://kafka.apache.org/documentation.html#producerconfigs
 * 
 * Valid properties are
 * <table>
 * 
 * <tr>
 * <th>property</th>
 * <th>default</th>
 * <th>description</th>
 * </tr>
 * 
 * <tr>
 * <td>metadata.broker.list</td>
 * <td></td>
 * <td>(required) A comma separated list of seed brokers to connect to in order
 * to get metadata about the cluster.</td>
 * </tr>
 * 
 * <tr>
 * <td>queue.buffering.max.ms</td>
 * <td>5000</td>
 * <td>Maximum time to buffer data. For example a setting of 100 will try to
 * batch together 100ms of messages to send at once. This will improve
 * throughput but adds message delivery latency due to the buffering.</td>
 * </tr>
 * 
 * <tr>
 * <td>request.required.acks</td>
 * <td>1</td>
 * <td>This value controls when a produce request is considered completed.
 * Specifically, how many other brokers must have committed the data to their
 * log and acknowledged this to the leader? Typical values are
 * <ul>
 * <li>0, which means that the producer never waits for an acknowledgement from
 * the broker (the same behavior as 0.7). This option provides the lowest
 * latency but the weakest durability guarantees (some data will be lost when a
 * server fails).</li>
 * <li>1, which means that the producer gets an acknowledgement after the leader
 * replica has received the data. This option provides better durability as the
 * client waits until the server acknowledges the request as successful (only
 * messages that were written to the now-dead leader but not yet replicated will
 * be lost).</li>
 * <li>-1, which means that the producer gets an acknowledgement after all
 * in-sync replicas have received the data. This option provides the best
 * durability, we guarantee that no messages will be lost as long as at least
 * one in sync replica remains.</li>
 * </ul>
 * </td>
 * </tr>
 * 
 * <tr>
 * <td>request.timeout.ms</td>
 * <td>10000</td>
 * <td>The amount of time the broker will wait trying to meet the
 * request.required.acks requirement before sending back an error to the client.
 * </td>
 * </tr>
 * 
 * <tr>
 * <td>message.send.max.retries</td>
 * <td>3</td>
 * <td>This property will cause the producer to automatically retry a failed
 * send request. This property specifies the number of retries when such
 * failures occur. Note that setting a non-zero value here can lead to
 * duplicates in the case of network errors that cause a message to be sent but
 * the acknowledgement to be lost.</td>
 * </tr>
 * 
 * <tr>
 * <td>retry.backoff.ms</td>
 * <td>100</td>
 * <td>Before each retry, the producer refreshes the metadata of relevant topics
 * to see if a new leader has been elected. Since leader election takes a bit of
 * time, this property specifies the amount of time that the producer waits
 * before refreshing the metadata.</td>
 * </tr>
 * 
 * <tr>
 * <td>topic.metadata.refresh.interval.ms</td>
 * <td>600 * 1000</td>
 * <td>The producer generally refreshes the topic metadata from brokers when
 * there is a failure (partition missing, leader not available...). It will also
 * poll regularly (default: every 10min so 600000ms). If you set this to a
 * negative value, metadata will only get refreshed on failure. If you set this
 * to zero, the metadata will get refreshed after each message sent (not
 * recommended). Important note: the refresh happen only AFTER the message is
 * sent, so if the producer never sends a message the metadata is never
 * refreshed</td>
 * </tr>
 * 
 * <tr>
 * <td>message.buffer.size</td>
 * <td>1024*1024</td>
 * <td>The size of each buffer that is used to store raw messages before they
 * are sent. Since a full buffer is sent at once, don't make this too big.</td>
 * </tr>
 * 
 * <tr>
 * <td>num.buffers</td>
 * <td>2</td>
 * <td>The number of buffers to use. At any given time, there is up to one
 * buffer being filled with new data, up to one buffer having its data sent to
 * the broker, and any number of buffers waiting to be filled and/or sent.
 * 
 * Essentially, the limit of the amount of data that can be queued at at any
 * given time is message.buffer.size * num.buffers. Although, in reality, you
 * won't get buffers to 100% full each time.</td>
 * </tr>
 * 
 * <tr>
 * <td>send.buffer.size</td>
 * <td>message.buffer.size + 200</td>
 * <td>Size of the byte buffer used to store the final (with headers and
 * compression applied) data to be sent to the broker.</td>
 * </tr>
 * 
 * <tr>
 * <td>compression.codec</td>
 * <td>none</td>
 * <td>This parameter allows you to specify the compression codec for all data
 * generated by this producer. Valid values are "none", "gzip" and "snappy".</td>
 * </tr>
 * 
 * <tr>
 * <td>gzip.compression.level</td>
 * <td>java.util.zip.Deflater.DEFAULT_COMPRESSION</td>
 * <td>If compression.codec is set to gzip, then this allows configuration of
 * the compression level.
 * <ul>
 * <li>-1: default compression level</li>
 * <li>0: no compression</li>
 * <li>1-9: 1=fastest compression ... 9=best compression</li>
 * </ul>
 * </td>
 * </tr>
 * 
 * <tr>
 * <td>queue.enqueue.timeout.ms</td>
 * <td>-1</td>
 * <td>The amount of time to block before dropping messages when all buffers are
 * full. If set to 0 events will be enqueued immediately or dropped if the queue
 * is full (the producer send call will never block). If set to -1 the producer
 * will block indefinitely and never willingly drop a send.</td>
 * </tr>
 * 
 * </table>
 */
public class ProducerConfiguration {
  private static final Logger LOG = LoggerFactory
      .getLogger(ProducerConfiguration.class);

  protected static final int ONE_MB = 1024 * 1024;

  // Options matching the producer client
  private List<String> metadataBrokerList;
  private short requestRequiredAcks;
  private int requestTimeoutMs;
  private String compressionCodec;
  private int messageSendMaxRetries;
  private int retryBackoffMs;
  private long topicMetadataRefreshIntervalMs;
  private long queueBufferingMaxMs;
  private long queueEnqueueTimeoutMs;

  // Client specific options
  private int messageBufferSize;
  private int numBuffers;
  private int sendBufferSize;
  private int compressionLevel;

  public ProducerConfiguration(Properties props) throws Exception {
    LOG.info("Building configuration.");

    metadataBrokerList = new ArrayList<String>();
    String metadataBrokerListString = props.getProperty("metadata.broker.list");
    if (metadataBrokerListString == null || metadataBrokerListString.isEmpty()) {
      throw new Exception("metadata.broker.list cannot be empty.");
    }
    for (String s : metadataBrokerListString.split(",")) {
      // This is not a good regex. Could make it better.
      if (s.matches("^[\\.a-zA-Z0-9-]*:\\d+$")) {
        metadataBrokerList.add(s);
      } else {
        throw new Exception(
            "metata.broker.list must contain a list of hosts and ports (localhost:123,192.168.1.1:456).  Got "
                + metadataBrokerListString);
      }
    }
    LOG.info("metadata.broker.list = {}", metadataBrokerList);

    queueBufferingMaxMs = Long.parseLong(props.getProperty(
        "queue.buffering.max.ms", "5000"));
    if (queueBufferingMaxMs < 0) {
      throw new Exception("queue.buffering.max.ms cannot be negative.");
    }
    LOG.info("queue.buffering.max.ms = {}", queueBufferingMaxMs);

    requestRequiredAcks = Short.parseShort(props.getProperty(
        "request.required.acks", "1"));
    if (requestRequiredAcks != -1 && requestRequiredAcks != 0
        && requestRequiredAcks != 1) {
      throw new Exception("request.required.acks can only be -1, 0 or 1.  Got "
          + requestRequiredAcks);
    }
    LOG.info("request.required.acks = {}", requestRequiredAcks);

    requestTimeoutMs = Integer.parseInt(props.getProperty("request.timeout.ms",
        "10000"));
    if (requestTimeoutMs < 0) {
      throw new Exception("request.timeout.ms cannot be negative.  Got "
          + requestTimeoutMs);
    }
    LOG.info("request.timeout.ms = {}", requestTimeoutMs);

    messageSendMaxRetries = Integer.parseInt(props.getProperty(
        "message.send.max.retries", "3"));
    if (messageSendMaxRetries < 0) {
      throw new Exception("message.send.max.retries cannot be negative.  Got "
          + messageSendMaxRetries);
    }
    LOG.info("message.send.max.retries = {}", messageSendMaxRetries);

    retryBackoffMs = Integer.parseInt(props.getProperty("retry.backoff.ms",
        "100"));
    if (retryBackoffMs < 0) {
      throw new Exception("retry.backoff.ms cannot be negative.  Got "
          + retryBackoffMs);
    }
    LOG.info("retry.backoff.ms = " + retryBackoffMs);

    topicMetadataRefreshIntervalMs = Long.parseLong(props.getProperty(
        "topic.metadata.refresh.interval.ms", "" + (600 * 1000)));
    LOG.info("topic.metadata.refresh.interval.ms = {}",
        topicMetadataRefreshIntervalMs);

    messageBufferSize = Integer.parseInt(props.getProperty(
        "message.buffer.size", "" + ONE_MB));
    if (messageBufferSize < 1) {
      throw new Exception("message.buffer.size must be greater than 0.  Got "
          + messageBufferSize);
    }
    LOG.info("message.buffer.size = {}", messageBufferSize);

    numBuffers = Integer.parseInt(props.getProperty("num.buffers", "2"));
    if (numBuffers < 2) {
      throw new Exception("num.buffers must be at least 2.  Got " + numBuffers);
    }
    LOG.info("num.buffers = {}", numBuffers);

    sendBufferSize = Integer.parseInt(props.getProperty("send.buffer.size", ""
        + (messageBufferSize + 200)));
    if (sendBufferSize < 1) {
      throw new Exception(
          "message.send.max.retries must be greater than 0.  Got "
              + sendBufferSize);
    }
    LOG.info("send.buffer.size = {}", sendBufferSize);

    String rawCompressionCodec = props.getProperty("compression.codec", "none");
    compressionCodec = rawCompressionCodec.toLowerCase();
    if (compressionCodec.equals("none") == false
        && compressionCodec.equals("gzip") == false
        && compressionCodec.equals("snappy") == false) {
      throw new Exception(
          "compression.codec must be one of none, gzip or snappy.  Got "
              + rawCompressionCodec);
    }
    LOG.info("compression.codec = {}", compressionCodec);

    compressionLevel = Integer.parseInt(props.getProperty(
        "gzip.compression.level", "" + Deflater.DEFAULT_COMPRESSION));
    if (compressionLevel < -1 || compressionLevel > 9) {
      throw new Exception(
          "gzip.compression.level must be -1 (default), 0 (no compression) or in the range 1-9.  Got "
              + compressionLevel);
    }
    LOG.info("gzip.compression.level = {}", compressionLevel);

    queueEnqueueTimeoutMs = Long.parseLong(props.getProperty(
        "queue.enqueue.timeout.ms", "-1"));
    if (queueEnqueueTimeoutMs != -1 && queueEnqueueTimeoutMs < 0) {
      throw new Exception(
          "queue.enqueue.timeout.ms must either be -1 or a non-negative.");
    }
    LOG.info("queue.enqueue.timeout.ms = {}", queueEnqueueTimeoutMs);

  }

  public List<String> getMetadataBrokerList() {
    return metadataBrokerList;
  }

  public void setMetadataBrokerList(List<String> metadataBrokerList) {
    this.metadataBrokerList = metadataBrokerList;
  }

  public long getQueueBufferingMaxMs() {
    return queueBufferingMaxMs;
  }

  public void setQueueBufferingMaxMs(long queueBufferingMaxMs) {
    this.queueBufferingMaxMs = queueBufferingMaxMs;
  }

  public short getRequestRequiredAcks() {
    return requestRequiredAcks;
  }

  public void setRequestRequiredAcks(short requestRequiredAcks) {
    this.requestRequiredAcks = requestRequiredAcks;
  }

  public int getRequestTimeoutMs() {
    return requestTimeoutMs;
  }

  public void setRequestTimeoutMs(int requestTimeoutMs) {
    this.requestTimeoutMs = requestTimeoutMs;
  }

  public int getMessageSendMaxRetries() {
    return messageSendMaxRetries;
  }

  public void setMessageSendMaxRetries(int messageSendMaxRetries) {
    this.messageSendMaxRetries = messageSendMaxRetries;
  }

  public int getRetryBackoffMs() {
    return retryBackoffMs;
  }

  public void setRetryBackoffMs(int retryBackoffMs) {
    this.retryBackoffMs = retryBackoffMs;
  }

  public int getMessageBufferSize() {
    return messageBufferSize;
  }

  public void setMessageBufferSize(int messageBufferSize) {
    this.messageBufferSize = messageBufferSize;
  }

  public int getNumBuffers() {
    return numBuffers;
  }

  public void setNumBuffers(int numBuffers) {
    this.numBuffers = numBuffers;
  }

  public int getSendBufferSize() {
    return sendBufferSize;
  }

  public void setSendBufferSize(int sendBufferSize) {
    this.sendBufferSize = sendBufferSize;
  }

  public String getCompressionCodec() {
    return compressionCodec;
  }

  public void setCompressionCodec(String compressionCodec) {
    this.compressionCodec = compressionCodec;
  }

  public int getCompressionLevel() {
    return compressionLevel;
  }

  public void setCompressionLevel(int compressionLevel) {
    this.compressionLevel = compressionLevel;
  }

  public long getTopicMetadataRefreshIntervalMs() {
    return topicMetadataRefreshIntervalMs;
  }

  public void setTopicMetadataRefreshIntervalMs(
      long topicMetadataRefreshIntervalMs) {
    this.topicMetadataRefreshIntervalMs = topicMetadataRefreshIntervalMs;
  }

  public long getQueueEnqueueTimeoutMs() {
    return queueEnqueueTimeoutMs;
  }

  public void setQueueEnqueueTimeoutMs(long queueEnqueueTimeoutMs) {
    this.queueEnqueueTimeoutMs = queueEnqueueTimeoutMs;
  }

}
