package com.blackberry.kafka.superproducer;

public class Broker {
  private int nodeId;
  private String host;
  private int port;

  public Broker() {
  }

  public Broker(int nodeId, String host, int port) {
    this.nodeId = nodeId;
    this.host = host;
    this.port = port;
  }

  public int getNodeId() {
    return nodeId;
  }

  public void setNodeId(int nodeId) {
    this.nodeId = nodeId;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  @Override
  public String toString() {
    return "Broker [nodeId=" + nodeId + ", host=" + host + ", port=" + port
        + "]";
  }

}
