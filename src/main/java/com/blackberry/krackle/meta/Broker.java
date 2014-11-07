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

package com.blackberry.krackle.meta;

/**
 * Class to hold the id, host and port of a Kafka broker.
 */
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

  public String getNiceDescription()
  {
	  return "node " + nodeId + " @ " + host + ":" + port;
  }
  
  @Override
  public String toString() {
    return "Broker [nodeId=" + nodeId + ", host=" + host + ", port=" + port
        + "]";
  }

}
