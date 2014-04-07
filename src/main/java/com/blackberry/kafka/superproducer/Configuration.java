package com.blackberry.kafka.superproducer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.zip.Deflater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configuration {
  private static final Logger LOG = LoggerFactory
      .getLogger(Configuration.class);

  protected static final int ONE_MB = 1024 * 1024;

  private List<String> metadataBrokerList;
  private short requrestRequiredAcks;
  private int requestTimeoutMs;
  private int messageSendMaxRetries;
  private int retryBackoffMs;
  private int messageBufferSize;
  private int sendBufferSize;
  private int responseBufferSize;
  private String compressionCodec;
  private int compressionLevel;
  private long topicMetadataRefreshIntervalMs;

  private boolean metricsToConsole;
  private int metricsToConsoleIntervalMs;

  public Configuration(Properties props) throws Exception {
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

    requrestRequiredAcks = Short.parseShort(props.getProperty(
        "request.required.acks", "1"));
    if (requrestRequiredAcks != -1 && requrestRequiredAcks != 0
        && requrestRequiredAcks != 1) {
      throw new Exception("request.required.acks can only be -1, 0 or 1.  Got "
          + requrestRequiredAcks);
    }
    LOG.info("request.required.acks = {}", requrestRequiredAcks);

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

    sendBufferSize = Integer.parseInt(props.getProperty("send.buffer.size", ""
        + (messageBufferSize + 200)));
    if (sendBufferSize < 1) {
      throw new Exception(
          "message.send.max.retries must be greater than 0.  Got "
              + sendBufferSize);
    }
    LOG.info("send.buffer.size = {}", sendBufferSize);

    responseBufferSize = Integer.parseInt(props.getProperty(
        "response.buffer.size", "100"));
    if (responseBufferSize < 1) {
      throw new Exception("response.buffer.size must be greater than 0.  Got "
          + responseBufferSize);
    }
    LOG.info("response.buffer.size = {}", responseBufferSize);

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

    metricsToConsole = Boolean.parseBoolean(props.getProperty(
        "metrics.to.console", "false"));
    LOG.info("metrics.to.console = {}", metricsToConsole);

    metricsToConsoleIntervalMs = Integer.parseInt(props.getProperty(
        "metrics.to.console.interval.ms", "60000"));
    if (metricsToConsoleIntervalMs < 1) {
      throw new Exception(
          "metrics.to.console.intercal.ms must be greater than 0.  Got "
              + metricsToConsoleIntervalMs);
    }
    LOG.info("metrics.to.console.interval.ms = {}", metricsToConsoleIntervalMs);
  }

  public List<String> getMetadataBrokerList() {
    return metadataBrokerList;
  }

  public void setMetadataBrokerList(List<String> metadataBrokerList) {
    this.metadataBrokerList = metadataBrokerList;
  }

  public short getRequrestRequiredAcks() {
    return requrestRequiredAcks;
  }

  public void setRequrestRequiredAcks(short requrestRequiredAcks) {
    this.requrestRequiredAcks = requrestRequiredAcks;
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

  public int getSendBufferSize() {
    return sendBufferSize;
  }

  public void setSendBufferSize(int sendBufferSize) {
    this.sendBufferSize = sendBufferSize;
  }

  public int getResponseBufferSize() {
    return responseBufferSize;
  }

  public void setResponseBufferSize(int responseBufferSize) {
    this.responseBufferSize = responseBufferSize;
  }

  public long getTopicMetadataRefreshIntervalMs() {
    return topicMetadataRefreshIntervalMs;
  }

  public void setTopicMetadataRefreshIntervalMs(
      long topicMetadataRefreshIntervalMs) {
    this.topicMetadataRefreshIntervalMs = topicMetadataRefreshIntervalMs;
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

  public boolean isMetricsToConsole() {
    return metricsToConsole;
  }

  public void setMetricsToConsole(boolean metricsToConsole) {
    this.metricsToConsole = metricsToConsole;
  }

  public int getMetricsToConsoleIntervalMs() {
    return metricsToConsoleIntervalMs;
  }

  public void setMetricsToConsoleIntervalMs(int metricsToConsoleIntervalMs) {
    this.metricsToConsoleIntervalMs = metricsToConsoleIntervalMs;
  }

}
