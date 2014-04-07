package com.blackberry.kafka.loproducer;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaData {
  private static final Logger LOG = LoggerFactory.getLogger(MetaData.class);

  private Map<Integer, Broker> brokers = new HashMap<Integer, Broker>();
  private Map<String, Topic> topics = new HashMap<String, Topic>();
  private int correlationId;

  public static MetaData getMetaData(Configuration conf, String topicString,
      String clientIdString) {
    MetaData metadata = new MetaData();
    metadata.setCorrelationId((int) System.currentTimeMillis());

    // Get the broker seeds from the config.
    List<HostAndPort> seedBrokers = new ArrayList<HostAndPort>();
    for (String hnp : conf.getMetadataBrokerList()) {
      String[] hostPort = hnp.split(":", 2);
      seedBrokers.add(new HostAndPort(hostPort[0], Integer
          .parseInt(hostPort[1])));
    }

    LOG.debug("Getting metadata for topic {}", topicString);

    // Try each seed broker in a random order
    Collections.shuffle(seedBrokers);

    Socket sock = null;
    for (HostAndPort hnp : seedBrokers) {
      try {
        sock = new Socket(hnp.host, hnp.port);
      } catch (UnknownHostException e) {
        LOG.warn("Unknown host: {}", hnp.host);
        continue;
      } catch (IOException e) {
        LOG.warn("Error connecting to {}:{}", hnp.host, hnp.port);
        continue;
      }
      break;
    }

    if (sock == null) {
      LOG.error("Unable to connect to any seed broker to updata metadata.");
      return null;
    }

    try {
      sock.getOutputStream().write(
          buildMetadataRequest(topicString.getBytes(Constants.UTF8),
              clientIdString.getBytes(Constants.UTF8),
              metadata.getCorrelationId()));

      byte[] sizeBuffer = new byte[4];
      InputStream in = sock.getInputStream();
      int bytesRead = 0;
      while (bytesRead < 4) {
        int read = in.read(sizeBuffer, bytesRead, 4 - bytesRead);
        if (read == -1) {
          throw new IOException(
              "Stream ended before data length could be read.");
        }
        bytesRead += read;
      }
      int length = ByteBuffer.wrap(sizeBuffer).getInt();

      byte[] responseArray = new byte[length];
      bytesRead = 0;
      while (bytesRead < length) {
        int read = in.read(responseArray, bytesRead, length - bytesRead);
        if (read == -1) {
          throw new IOException("Stream ended before end of response.");
        }
        bytesRead += read;
      }
      ByteBuffer responseBuffer = ByteBuffer.wrap(responseArray);
      int cid = responseBuffer.getInt();
      if (cid != metadata.getCorrelationId()) {
        LOG.error("Got back wrong correlation id.");
        return null;
      }

      // Load the brokers
      int numBrokers = responseBuffer.getInt();
      for (int i = 0; i < numBrokers; i++) {
        int nodeId = responseBuffer.getInt();
        String host = readString(responseBuffer);
        int port = responseBuffer.getInt();

        metadata.getBrokers().put(nodeId, new Broker(nodeId, host, port));

        LOG.debug("Broker {} @ {}:{}", nodeId, host, port);
      }

      // Load the topics
      int numTopics = responseBuffer.getInt();
      for (int i = 0; i < numTopics; i++) {

        short errorCode = responseBuffer.getShort();
        String name = readString(responseBuffer);
        Topic t = new Topic(name);
        LOG.debug("Topic {} (Error {})", name, errorCode);

        int numParts = responseBuffer.getInt();
        for (int j = 0; j < numParts; j++) {

          short partError = responseBuffer.getShort();
          int partId = responseBuffer.getInt();
          int leader = responseBuffer.getInt();
          LOG.debug("    Partition ID={}, Leader={} (Error={})", leader,
              partId, partError);

          Partition part = new Partition(partId);
          part.setLeader(leader);

          int numReplicas = responseBuffer.getInt();
          for (int k = 0; k < numReplicas; k++) {
            int rep = responseBuffer.getInt();
            LOG.debug("        Replica on {}", rep);
            part.getReplicas().add(rep);
          }

          int numIsr = responseBuffer.getInt();
          for (int k = 0; k < numIsr; k++) {
            int isr = responseBuffer.getInt();
            LOG.debug("        Isr on {}", isr);
            part.getInSyncReplicas().add(isr);
          }

          t.getPartitions().add(part);
        }

        metadata.getTopics().put(name, t);
      }

    } catch (IOException e) {
      LOG.error("Failed to get metadata");
      return null;
    }

    return metadata;
  }

  private static String readString(ByteBuffer bb) {
    short length = bb.getShort();
    byte[] a = new byte[length];
    bb.get(a);
    return new String(a, Constants.UTF8);
  }

  private static byte[] buildMetadataRequest(byte[] topic, byte[] clientId,
      int correlationId) {
    byte[] request = new byte[20 + clientId.length + topic.length];
    ByteBuffer bb = ByteBuffer.wrap(request);
    bb.putInt(16 + clientId.length + topic.length);
    bb.putShort(Constants.APIKEY_METADATA);
    bb.putShort(Constants.API_VERSION);
    bb.putInt(correlationId);
    bb.putShort((short) clientId.length);
    bb.put(clientId);

    // topics.
    bb.putInt(1);
    bb.putShort((short) topic.length);
    bb.put(topic);

    return request;
  }

  // We're storing the given hostname and not an InetAddress since we want to
  // re-resolve the address each time. This way changes to DNS can make us
  // change properly.
  private static class HostAndPort {
    String host;
    int port;

    HostAndPort(String host, int port) {
      this.host = host;
      this.port = port;
    }

    @Override
    public String toString() {
      return "HostAndPort [host=" + host + ", port=" + port + "]";
    }
  }

  public Map<Integer, Broker> getBrokers() {
    return brokers;
  }

  public Broker getBroker(Integer id) {
    return brokers.get(id);
  }

  public Map<String, Topic> getTopics() {
    return topics;
  }

  public Topic getTopic(String name) {
    return topics.get(name);
  }

  public int getCorrelationId() {
    return correlationId;
  }

  public void setCorrelationId(int correlationId) {
    this.correlationId = correlationId;
  }

  @Override
  public String toString() {
    return "MetaData [brokers=" + brokers + ", topics=" + topics + "]";
  }

}
