/**
 * Copyright 2014 BlackBerry, Limited.
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

package com.blackberry.krackle.producer;

import java.nio.ByteBuffer;

/**
 * Buffer to hold data that is to be sent to Kafka.
 */
public class MessageSetBuffer {
  private int size;
  private byte[] bytes;
  private ByteBuffer buffer;
  private int batchSize = 0;
  private Producer producer;

  public MessageSetBuffer(Producer producer, int size) {
    this.producer = producer;
    this.size = size;
    bytes = new byte[this.size];
    buffer = ByteBuffer.wrap(bytes);
  }

  public void clear() {
    batchSize = 0;
    buffer.clear();
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public void setBytes(byte[] bytes) {
    this.bytes = bytes;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public void setBuffer(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public Producer getProducer() {
    return producer;
  }

  public void setProducer(Producer producer) {
    this.producer = producer;
  }

  public void incrementBatchSize() {
    batchSize++;
  }

}
